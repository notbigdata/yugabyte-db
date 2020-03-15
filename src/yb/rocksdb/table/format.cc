//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// The following only applies to changes made to this file as part of YugaByte development.
//
// Portions Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "yb/rocksdb/table/format.h"

#include <inttypes.h>

#include <string>

#include "yb/gutil/macros.h"
#include "yb/rocksdb/env.h"
#include "yb/rocksdb/options.h"
#include "yb/rocksdb/table/block.h"
#include "yb/rocksdb/util/coding.h"
#include "yb/rocksdb/util/compression.h"
#include "yb/rocksdb/util/crc32c.h"
#include "yb/rocksdb/util/file_reader_writer.h"
#include "yb/rocksdb/util/perf_context_imp.h"
#include "yb/rocksdb/util/xxhash.h"

#include "yb/util/encryption_util.h"
#include "yb/util/encrypted_file.h"
#include "yb/util/format.h"
#include "yb/util/mem_tracker.h"
#include "yb/util/string_util.h"

using yb::Format;
using yb::Result;

namespace rocksdb {

extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;
extern const uint64_t kCuckooTableMagicNumber;

const uint32_t DefaultStackBufferSize = 5000;

void BlockHandle::AppendEncodedTo(std::string* dst) const {
  // Sanity check that all fields have been set
  DCHECK_NE(offset_, kUint64FieldNotSet);
  DCHECK_NE(size_, kUint64FieldNotSet);
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) &&
      GetVarint64(input, &size_)) {
    if (offset_ == 196446 && size_ == 11057) {
      LOG(INFO) << "DEBUG mbautin: bad block handle created from here: " << yb::GetStackTrace();
    }
    return Status::OK();
  } else {
    return STATUS(Corruption, "bad block handle");
  }
}

// Return a string that contains the copy of handle.
std::string BlockHandle::ToString(bool hex) const {
  std::string handle_str;
  AppendEncodedTo(&handle_str);
  if (hex) {
    std::string result;
    char buf[10];
    for (size_t i = 0; i < handle_str.size(); i++) {
      snprintf(buf, sizeof(buf), "%02X",
               static_cast<unsigned char>(handle_str[i]));
      result += buf;
    }
    return result;
  } else {
    return handle_str;
  }
}

std::string BlockHandle::ToDebugString() const {
  return Format("BlockHandle { offset: $0 size: $1 }", offset_, size_);
}

const BlockHandle BlockHandle::kNullBlockHandle(0, 0);

namespace {
inline bool IsLegacyFooterFormat(uint64_t magic_number) {
  return magic_number == kLegacyBlockBasedTableMagicNumber ||
         magic_number == kLegacyPlainTableMagicNumber;
}
inline uint64_t UpconvertLegacyFooterFormat(uint64_t magic_number) {
  if (magic_number == kLegacyBlockBasedTableMagicNumber) {
    return kBlockBasedTableMagicNumber;
  }
  if (magic_number == kLegacyPlainTableMagicNumber) {
    return kPlainTableMagicNumber;
  }
  assert(false);
  return 0;
}

inline bool IsValidMagicNumber(uint64_t magic_number) {
  return magic_number == kLegacyBlockBasedTableMagicNumber ||
         magic_number == kBlockBasedTableMagicNumber ||
         magic_number == kPlainTableMagicNumber ||
         magic_number == kLegacyPlainTableMagicNumber ||
         magic_number == kCuckooTableMagicNumber;
}

}  // namespace

// legacy footer format:
//    metaindex handle (varint64 offset, varint64 size)
//    index handle     (varint64 offset, varint64 size)
//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength
//    table_magic_number (8 bytes)
// new footer format:
//    checksum (char, 1 byte)
//    metaindex handle (varint64 offset, varint64 size)
//    index handle     (varint64 offset, varint64 size)
//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength + 1
//    footer version (4 bytes)
//    table_magic_number (8 bytes)
void Footer::AppendEncodedTo(std::string* dst) const {
  assert(HasInitializedTableMagicNumber());
  if (IsLegacyFooterFormat(table_magic_number())) {
    // has to be default checksum with legacy footer
    assert(checksum_ == kCRC32c);
    const size_t original_size = dst->size();
    metaindex_handle_.AppendEncodedTo(dst);
    data_index_handle_.AppendEncodedTo(dst);
    dst->resize(original_size + 2 * BlockHandle::kMaxEncodedLength);  // Padding
    PutFixed32(dst, static_cast<uint32_t>(table_magic_number() & 0xffffffffu));
    PutFixed32(dst, static_cast<uint32_t>(table_magic_number() >> 32));
    assert(dst->size() == original_size + kVersion0EncodedLength);
  } else {
    const size_t original_size = dst->size();
    dst->push_back(static_cast<char>(checksum_));
    metaindex_handle_.AppendEncodedTo(dst);
    data_index_handle_.AppendEncodedTo(dst);
    dst->resize(original_size + kNewVersionsEncodedLength - 12);  // Padding
    PutFixed32(dst, version());
    PutFixed32(dst, static_cast<uint32_t>(table_magic_number() & 0xffffffffu));
    PutFixed32(dst, static_cast<uint32_t>(table_magic_number() >> 32));
    assert(dst->size() == original_size + kNewVersionsEncodedLength);
  }
}

Footer::Footer(uint64_t _table_magic_number, uint32_t _version)
    : version_(_version),
      checksum_(kCRC32c),
      table_magic_number_(_table_magic_number) {
  // This should be guaranteed by constructor callers
  assert(!IsLegacyFooterFormat(_table_magic_number) || version_ == 0);
}

Status Footer::DecodeFrom(Slice* input) {
  DSCHECK(!HasInitializedTableMagicNumber(), IllegalState, "Decoding into the same footer twice");
  DSCHECK(input != nullptr, IllegalState, "input can't be null");
  DSCHECK_GE(input->size(), kMinEncodedLength, Corruption, "Footer size is too small");

  const char *magic_ptr = input->cend() - kMagicNumberLengthByte;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                    (static_cast<uint64_t>(magic_lo)));

  // We check for legacy formats here and silently upconvert them
  bool legacy = IsLegacyFooterFormat(magic);
  if (legacy) {
    magic = UpconvertLegacyFooterFormat(magic);
  }
  set_table_magic_number(magic);

  if (legacy) {
    // The size is already asserted to be at least kMinEncodedLength
    // at the beginning of the function
    input->remove_prefix(input->size() - kVersion0EncodedLength);
    version_ = 0 /* legacy */;
    checksum_ = kCRC32c;
  } else {
    version_ = DecodeFixed32(magic_ptr - 4);
    // Footer version 1 and higher will always occupy exactly this many bytes.
    // It consists of the checksum type, two block handles, padding,
    // a version number, and a magic number
    if (input->size() < kNewVersionsEncodedLength) {
      return STATUS(Corruption, "input is too short to be an sstable");
    } else {
      input->remove_prefix(input->size() - kNewVersionsEncodedLength);
    }
    uint32_t chksum;
    if (!GetVarint32(input, &chksum)) {
      return STATUS(Corruption, "bad checksum type");
    }
    checksum_ = static_cast<ChecksumType>(chksum);
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = data_index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + kMagicNumberLengthByte;
    *input = Slice(end, input->cend() - end);
  }
  return result;
}

std::string Footer::ToString() const {
  std::string result, handle_;
  result.reserve(1024);

  bool legacy = IsLegacyFooterFormat(table_magic_number_);
  if (legacy) {
    result.append("metaindex handle: " + metaindex_handle_.ToString() + "\n  ");
    result.append("data index handle: " + data_index_handle_.ToString() + "\n  ");
    result.append("table_magic_number: " +
                  rocksdb::ToString(table_magic_number_) + "\n  ");
  } else {
    result.append("checksum: " + rocksdb::ToString(checksum_) + "\n  ");
    result.append("metaindex handle: " + metaindex_handle_.ToString() + "\n  ");
    result.append("data index handle: " + data_index_handle_.ToString() + "\n  ");
    result.append("footer version: " + rocksdb::ToString(version_) + "\n  ");
    result.append("table_magic_number: " +
                  rocksdb::ToString(table_magic_number_) + "\n  ");
  }
  return result;
}

Status CheckSSTableFileSize(RandomAccessFileReader* file, uint64_t file_size) {
  if (file_size < Footer::kMinEncodedLength) {
    return STATUS_FORMAT(Corruption,
                         "File is too short to be an SSTable: $0",
                         file->file()->filename());
  }
  return Status::OK();
}

Status ReadFooterFromFile(
    RandomAccessFileReader* file, uint64_t file_size, Footer* footer,
    uint64_t enforce_table_magic_number) {
  RETURN_NOT_OK(CheckSSTableFileSize(file, file_size));

  char footer_space[Footer::kMaxEncodedLength];
  Slice footer_input;
  size_t read_offset =
      (file_size > Footer::kMaxEncodedLength)
          ? static_cast<size_t>(file_size - Footer::kMaxEncodedLength)
          : 0;
  const size_t read_size = std::min<size_t>(Footer::kMaxEncodedLength, file_size);
  struct FooterValidator : public yb::ReadValidator {
    FooterValidator(RandomAccessFileReader* file_,
                    Footer* footer_,
                    uint64_t enforce_table_magic_number_)
        : file(file_),
          footer(footer_),
          enforce_table_magic_number(enforce_table_magic_number_) {}

    CHECKED_STATUS Validate(const Slice& read_result) const override {
      // Check that we actually read the whole footer from the file. It may be that size isn't
      // correct.
      RETURN_NOT_OK(CheckSSTableFileSize(file, read_result.size()));
      Slice mutable_read_result(read_result);
      *footer = Footer();
      RETURN_NOT_OK(footer->DecodeFrom(&mutable_read_result));
      if (enforce_table_magic_number == 0 && !IsValidMagicNumber(footer->table_magic_number())) {
        return STATUS_FORMAT(Corruption, "Bad table magic number: $0", enforce_table_magic_number);        
      }

      if (enforce_table_magic_number != 0 &&
          enforce_table_magic_number != footer->table_magic_number()) {
        LOG(INFO) << Format(
                "DEBUG mbautin: bad table magic number $0, expected $1",
                footer->table_magic_number(),
                enforce_table_magic_number)
            << " when reading footer from file "
            << file->file()->filename()
            << ", footer length=" << read_result.size();

        return STATUS_FORMAT(Corruption, "Bad table magic number: $0, expected: $1",
                              footer->table_magic_number(),
                              enforce_table_magic_number);
      }
      LOG(INFO) << Format(
              "DEBUG mbautin: GOOD table magic number $0, expected $1",
              footer->table_magic_number(),
              enforce_table_magic_number)
          << " when reading footer from file "
          << file->file()->filename()
          << ", footer length=" << read_result.size();
      LOG(INFO) << "DEBUG mbautin: footer data: " << footer->ToString();
      return Status::OK();
    }
    RandomAccessFileReader* file;
    Footer* const footer;
    const uint64_t enforce_table_magic_number;
  } validator(file, footer, enforce_table_magic_number);

  return file->ReadAndValidate(read_offset, read_size, &footer_input, footer_space, validator);
}

// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace {

string GetHexDump(Slice data) {
  std::ostringstream out;
  vector<string> hex_lines;
  vector<string> char_lines;
  const auto kBytesPerLine = 64;

  auto n = data.size();
  auto block_size = n - kBlockTrailerSize;
  out << "Data block of size " << block_size << " (plus " << kBlockTrailerSize << " bytes trailer): "
      << std::endl;
  for (size_t i = 0; i < n; ++i) {
    if (i % kBytesPerLine == 0) {
      hex_lines.emplace_back();
      char_lines.emplace_back();
    }
    uint8_t c = data.data()[i];
    hex_lines.back() += StringPrintf("%02x ", c);
    char_lines.back().push_back((32 <= c && c <= 126) ? static_cast<char>(c) : '.');
  }
  for (size_t i = 0; i < hex_lines.size(); ++i) {
    if (char_lines[i].empty()) {
      break;
    }
    const auto& hex_line = hex_lines[i];
    const auto& char_line = char_lines[i];
    out << char_line << std::string(kBytesPerLine - char_line.size(), ' ') << ' '
        << hex_line << std::endl;
  }
  return out.str();
}

struct ChecksumData {
  uint32_t expected;
  uint32_t actual;
};

Result<ChecksumData> ComputeChecksum(
    RandomAccessFileReader* file,
    const Footer& footer,
    const BlockHandle& handle,
    const Slice& src_data,
    uint32_t raw_expected_checksum) {
  switch (footer.checksum()) {
    case kCRC32c:
      return ChecksumData {
          .expected = crc32c::Unmask(raw_expected_checksum),
          .actual = crc32c::Value(src_data.data(), src_data.size())
      };
    case kxxHash:
      if (src_data.size() > std::numeric_limits<int>::max()) {
        return STATUS_FORMAT(
            Corruption, "Block too large for xxHash ($0 bytes, but must be $1 or smaller)",
            src_data.size(), std::numeric_limits<int>::max());
      }
      return ChecksumData {
          .expected = raw_expected_checksum,
          .actual = XXH32(src_data.data(), static_cast<int>(src_data.size()), 0)
      };
    case kNoChecksum:
      return ChecksumData {
          .expected = raw_expected_checksum,
          .actual = raw_expected_checksum
      };
  }
  return STATUS_FORMAT(
      Corruption, "Unknown checksum type in file: $0, block handle: $1",
      file->file()->filename(), handle.ToDebugString());
}
    
Status VerifyBlockChecksum(
    RandomAccessFileReader* file,
    const Footer& footer,
    const BlockHandle& handle,
    const char* data,
    const size_t block_size) {
  PERF_TIMER_GUARD(block_checksum_time);
  const uint32_t raw_expected_checksum = DecodeFixed32(data + block_size + 1);
  auto checksum = VERIFY_RESULT(
      ComputeChecksum(file, footer, handle, Slice(data, block_size + 1), raw_expected_checksum));
  if (checksum.actual != checksum.expected) {
    std::cerr << "DEBUG mbautin: hex dump of a block with a checksum mismatch (" 
              << "file " << file->file()->filename()
              << ", " << handle.ToDebugString() << "):\n"
              << GetHexDump(Slice(data, block_size + 5));

    if (false) {
      LOG(INFO) << "DEBUG mbautin: trying to restore block offsets in file "
                << file->file()->filename();
      uint64_t cur_offset = 0;
      auto file_size_result = file->file()->Size();
      CHECK_OK(file_size_result);
      auto file_size = file_size_result.get();

      std::vector<char> buf_vec;
      buf_vec.resize(50 * 1024 * 1024);
      char* buf = buf_vec.data();
      
      if (false && 
          file->file()->filename() == "/nfusr/dev-server/mbautin/encryption_corruption/new_bad_file/tablet-f7be082e2db04aca9704c2f84d1d2721/000269.sst.sblock.0") {
        Slice tmp_read_result;
        CHECK_OK(file->Read(0, file_size, &tmp_read_result, buf));
        const char* out_path = "/tmp/000269.sst.sblock.0.decrypted";
        FILE* fp = fopen(out_path, "wb");
        CHECK_NOTNULL(fp);
        CHECK_EQ(fwrite(buf, 1, file_size, fp), file_size);
        fclose(fp);
        LOG(INFO) << "Wrote the decrypted file at " << out_path;
        LOG(FATAL) << "Stop here";
      }

      if (false && file->file()->filename() == "/nfusr/dev-server/mbautin/encryption_corruption/new_bad_file/tablet-f7be082e2db04aca9704c2f84d1d2721/000269.sst.sblock.0") {
        // Slice tmp_read_result;
        // CHECK_OK(file->Read(0, file_size, &tmp_read_result, buf));
        // CHECK_EQ(tmp_read_result.size(), file_size);
        // CHECK_EQ(tmp_read_result.data(), buf);
        // for (size_t i = 0; i < file_size; ++i) {
        //   if (buf[i] == kSnappyCompression) {
        //     size_t bytes_left = file_size - i;
        //     if (bytes_left >= 5) {
        //       const uint32_t raw_expected_checksum2 = DecodeFixed32(buf + i + 1); 
        //       for (size_t start_pos = 0; start_pos < i; ++start_pos) {
        //         auto checksum2 = VERIFY_RESULT(
        //             ComputeChecksum(file, footer, handle, Slice(buf + start_pos, i + 1 - start_pos),
        //             raw_expected_checksum2));
        //       }
        //     }
        //   }
        // }

        LOG(FATAL) << "Stopping analysis here";
      }

      if (file->file()->filename() == "/nfusr/dev-server/mbautin/encryption_corruption/new_bad_file/tablet-f7be082e2db04aca9704c2f84d1d2721/000269.sst.sblock.0") {

        while (cur_offset < file_size) {
          bool found_valid = false;
          uint64_t valid_block_size = 0;
          for (uint64_t cur_block_size = 5;
              cur_block_size <= file_size - cur_offset;
              ++cur_block_size) {
            if (cur_block_size % 100000 == 0) {
              LOG(INFO) << "DEBUG mbautin: Trying block size " << cur_block_size << " at offset " << cur_offset;
            }
            Slice result;
            Status read_block_result = file->Read(cur_offset, cur_block_size, &result, buf);
            CHECK_OK(read_block_result);
            const uint32_t raw_expected_checksum2 = DecodeFixed32(buf + cur_block_size - 4);
            auto checksum2 = VERIFY_RESULT(
                ComputeChecksum(file, footer, handle, Slice(buf, cur_block_size - 4),
                raw_expected_checksum2));
            if (checksum2.expected == checksum2.actual) {
              LOG(INFO) << "DEBUG mbautin: Found valid block size for offset " << cur_offset
                        << ": block_size (including checksum and compression type): " << cur_block_size;
              found_valid = true;
              valid_block_size = cur_block_size;
              break;
            }
          }
          if (found_valid) {
            cur_offset += valid_block_size;
            LOG(INFO) << "DEBUG mbautin: Advancing to offset: " << cur_offset;
          } else {
            LOG(INFO) << "DEBUG mbautin: could not find valid block size for offset " << cur_offset;
            cur_offset++;
            LOG(INFO) << "DEBUG mbautin: incremented offset to " << cur_offset;
          }
        }
      }
    }
    
    return STATUS_FORMAT(
        Corruption, "Block checksum mismatch in file: $0, block handle: $1",
        file->file()->filename(), handle.ToDebugString());
  }
  LOG(INFO) << "DEBUG mbautin: checksum matched for block: " << handle.ToDebugString()
            << " in file " << file->file()->filename();
  return Status::OK();
}

// Read a block and check its CRC. When this function returns, *contents will contain the result of
// reading.
Status ReadBlock(
    RandomAccessFileReader* file, const Footer& footer, const ReadOptions& options,
    const BlockHandle& handle, Slice* contents, /* result of reading */ char* buf) {
  *contents = Slice(buf, buf);
  const size_t expected_read_size = static_cast<size_t>(handle.size()) + kBlockTrailerSize;
  Status s;
  {
    PERF_TIMER_GUARD(block_read_time);
    struct BlockChecksumValidator : public yb::ReadValidator {
      BlockChecksumValidator(
          RandomAccessFileReader* file_, const Footer& footer_, const ReadOptions& options_,
          const BlockHandle& handle_, size_t expected_read_size_)
          : file(file_),
            footer(footer_),
            options(options_),
            handle(handle_),
            expected_read_size(expected_read_size_) {}

      CHECKED_STATUS Validate(const Slice& read_result) const override {
        if (read_result.size() != expected_read_size) {
          return STATUS_FORMAT(
              Corruption, "Truncated block read in file: $0, block handle: $1, expected size: $2",
              file->file()->filename(), handle.ToDebugString(), expected_read_size);
        }

        if (options.verify_checksums) {
          return VerifyBlockChecksum(file, footer, handle, read_result.cdata(), handle.size());
        }
        return Status::OK();
      };

      RandomAccessFileReader* file;
      const Footer& footer;
      const ReadOptions& options;
      const BlockHandle& handle;
      const size_t expected_read_size;
    } validator(file, footer, options, handle, expected_read_size);

    s = file->ReadAndValidate(handle.offset(), expected_read_size, contents, buf, validator);
  }

  PERF_COUNTER_ADD(block_read_count, 1);
  PERF_COUNTER_ADD(block_read_byte, expected_read_size);

  return s;
}

}  // namespace

TrackedAllocation::TrackedAllocation()
    : size_(0) {
}

TrackedAllocation::TrackedAllocation(
    std::unique_ptr<char[]>&& data, size_t size, yb::MemTrackerPtr mem_tracker)
    : holder_(std::move(data)), size_(size), mem_tracker_(std::move(mem_tracker)) {
  if (holder_ && mem_tracker_) {
    mem_tracker_->Consume(size_);
  }
}

TrackedAllocation::~TrackedAllocation() {
  if (holder_ && mem_tracker_) {
    mem_tracker_->Release(size_);
  }
}

TrackedAllocation& TrackedAllocation::operator=(TrackedAllocation&& other) {
  if (holder_ && mem_tracker_) {
    mem_tracker_->Release(size_);
  }

  holder_ = std::move(other.holder_);
  size_ = other.size_;
  mem_tracker_ = std::move(other.mem_tracker_);

  return *this;
}

BlockContents::BlockContents(
    std::unique_ptr<char[]>&& _data, size_t _size, bool _cachable,
    CompressionType _compression_type, yb::MemTrackerPtr mem_tracker)
    : data(_data.get(), _size),
      cachable(_cachable),
      compression_type(_compression_type),
      allocation(std::move(_data), _size, std::move(mem_tracker)) {
}

Status ReadBlockContents(RandomAccessFileReader* file, const Footer& footer,
                         const ReadOptions& options, const BlockHandle& handle,
                         BlockContents* contents, Env* env,
                         const yb::MemTrackerPtr& mem_tracker, bool decompression_requested) {
  Status status;
  Slice slice;
  size_t n = static_cast<size_t>(handle.size());
  std::unique_ptr<char[]> heap_buf;
  char stack_buf[DefaultStackBufferSize];
  char* used_buf = nullptr;
  rocksdb::CompressionType compression_type;

  if (decompression_requested &&
      n + kBlockTrailerSize < DefaultStackBufferSize) {
    // If we've got a small enough hunk of data, read it in to the
    // trivially allocated stack buffer instead of needing a full malloc()
    used_buf = &stack_buf[0];
  } else {
    heap_buf = std::unique_ptr<char[]>(new char[n + kBlockTrailerSize]);
    used_buf = heap_buf.get();
  }

  status = ReadBlock(file, footer, options, handle, &slice, used_buf);

  if (!status.ok()) {
    LOG(ERROR) << __func__ << ": " << status << "\n" << yb::GetStackTrace();
    return status;
  }

  PERF_TIMER_GUARD(block_decompress_time);

  compression_type = static_cast<rocksdb::CompressionType>(slice.data()[n]);

  if (decompression_requested && compression_type != kNoCompression) {
    return UncompressBlockContents(slice.cdata(), n, contents, footer.version(), mem_tracker);
  }

  if (slice.cdata() != used_buf) {
    *contents = BlockContents(Slice(slice.data(), n), false, compression_type);
    return status;
  }

  if (used_buf == &stack_buf[0]) {
    heap_buf = std::unique_ptr<char[]>(new char[n]);
    memcpy(heap_buf.get(), stack_buf, n);
  }

  *contents = BlockContents(std::move(heap_buf), n, true, compression_type, mem_tracker);
  return status;
}

//
// The 'data' points to the raw block contents that was read in from file.
// This method allocates a new heap buffer and the raw block
// contents are uncompresed into this buffer. This
// buffer is returned via 'result' and it is upto the caller to
// free this buffer.
// format_version is the block format as defined in include/rocksdb/table.h
Status UncompressBlockContents(const char* data, size_t n,
                               BlockContents* contents,
                               uint32_t format_version,
                               const std::shared_ptr<yb::MemTracker>& mem_tracker) {
  std::unique_ptr<char[]> ubuf;
  int decompress_size = 0;
  assert(data[n] != kNoCompression);
  switch (data[n]) {
    case kSnappyCompression: {
      size_t ulength = 0;
      static char snappy_corrupt_msg[] =
        "Snappy not supported or corrupted Snappy compressed block contents";
      if (!Snappy_GetUncompressedLength(data, n, &ulength)) {
        return STATUS(Corruption, snappy_corrupt_msg);
      }
      ubuf = std::unique_ptr<char[]>(new char[ulength]);
      if (!Snappy_Uncompress(data, n, ubuf.get())) {
        return STATUS(Corruption, snappy_corrupt_msg);
      }
      *contents = BlockContents(std::move(ubuf), ulength, true, kNoCompression, mem_tracker);
      break;
    }
    case kZlibCompression:
      ubuf = std::unique_ptr<char[]>(Zlib_Uncompress(
          data, n, &decompress_size,
          GetCompressFormatForVersion(kZlibCompression, format_version)));
      if (!ubuf) {
        static char zlib_corrupt_msg[] =
          "Zlib not supported or corrupted Zlib compressed block contents";
        return STATUS(Corruption, zlib_corrupt_msg);
      }
      *contents =
          BlockContents(std::move(ubuf), decompress_size, true, kNoCompression, mem_tracker);
      break;
    case kBZip2Compression:
      ubuf = std::unique_ptr<char[]>(BZip2_Uncompress(
          data, n, &decompress_size,
          GetCompressFormatForVersion(kBZip2Compression, format_version)));
      if (!ubuf) {
        static char bzip2_corrupt_msg[] =
          "Bzip2 not supported or corrupted Bzip2 compressed block contents";
        return STATUS(Corruption, bzip2_corrupt_msg);
      }
      *contents =
          BlockContents(std::move(ubuf), decompress_size, true, kNoCompression, mem_tracker);
      break;
    case kLZ4Compression:
      ubuf = std::unique_ptr<char[]>(LZ4_Uncompress(
          data, n, &decompress_size,
          GetCompressFormatForVersion(kLZ4Compression, format_version)));
      if (!ubuf) {
        static char lz4_corrupt_msg[] =
          "LZ4 not supported or corrupted LZ4 compressed block contents";
        return STATUS(Corruption, lz4_corrupt_msg);
      }
      *contents =
          BlockContents(std::move(ubuf), decompress_size, true, kNoCompression, mem_tracker);
      break;
    case kLZ4HCCompression:
      ubuf = std::unique_ptr<char[]>(LZ4_Uncompress(
          data, n, &decompress_size,
          GetCompressFormatForVersion(kLZ4HCCompression, format_version)));
      if (!ubuf) {
        static char lz4hc_corrupt_msg[] =
          "LZ4HC not supported or corrupted LZ4HC compressed block contents";
        return STATUS(Corruption, lz4hc_corrupt_msg);
      }
      *contents =
          BlockContents(std::move(ubuf), decompress_size, true, kNoCompression, mem_tracker);
      break;
    case kZSTDNotFinalCompression:
      ubuf =
          std::unique_ptr<char[]>(ZSTD_Uncompress(data, n, &decompress_size));
      if (!ubuf) {
        static char zstd_corrupt_msg[] =
            "ZSTD not supported or corrupted ZSTD compressed block contents";
        return STATUS(Corruption, zstd_corrupt_msg);
      }
      *contents =
          BlockContents(std::move(ubuf), decompress_size, true, kNoCompression, mem_tracker);
      break;
    default:
      return STATUS(Corruption, "bad block type");
  }
  return Status::OK();
}

}  // namespace rocksdb
