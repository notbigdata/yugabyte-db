// Copyright (c) YugaByte, Inc.
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

#include <sys/types.h>

#include <string>

#include "yb/gutil/stringprintf.h"
#include "yb/rocksutil/rocksdb_encrypted_file_factory.h"

#include "yb/tserver/header_manager_impl.h"
#include "yb/util/status.h"
#include "yb/util/test_util.h"
#include "yb/util/header_manager.h"
#include "yb/util/header_manager_mock_impl.h"
#include "yb/util/encryption_test_util.h"

#include "yb/util/random_util.h"
#include "yb/util/path_util.h"
#include "yb/gutil/stringprintf.h"

#include "yb/rocksdb/table/block_based_table_factory.h"
#include "yb/rocksdb/table/table_builder.h"
#include "yb/rocksdb/util/file_reader_writer.h"
#include "yb/rocksdb/table/internal_iterator.h"

#include "yb/util/universe_key_manager.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

using namespace std::literals;

namespace yb {
namespace enterprise {

constexpr uint32_t kDataSize = 1000;

class TestRocksDBEncryptedEnv : public YBTest {};

TEST_F(TestRocksDBEncryptedEnv, FileOps) {
  auto header_manager = GetMockHeaderManager();
  HeaderManager* hm_ptr = header_manager.get();
  auto env = yb::enterprise::NewRocksDBEncryptedEnv(std::move(header_manager));
  auto fname = "test-file";
  auto bytes = RandomBytes(kDataSize);
  Slice data(bytes.data(), bytes.size());

  for (bool encrypted : {false, true}) {
    down_cast<HeaderManagerMockImpl*>(hm_ptr)->SetFileEncryption(encrypted);

    std::unique_ptr<rocksdb::WritableFile> writable_file;
    ASSERT_OK(env->NewWritableFile(fname, &writable_file, rocksdb::EnvOptions()));
    TestWrites<rocksdb::WritableFile>(writable_file.get(), data);

    std::unique_ptr<rocksdb::RandomAccessFile> ra_file;
    ASSERT_OK(env->NewRandomAccessFile(fname, &ra_file, rocksdb::EnvOptions()));
    TestRandomAccessReads<rocksdb::RandomAccessFile, uint8_t>(ra_file.get(), data);

    std::unique_ptr<rocksdb::SequentialFile> s_file;
    ASSERT_OK(env->NewSequentialFile(fname, &s_file, rocksdb::EnvOptions()));
    TestSequentialReads<rocksdb::SequentialFile, uint8_t>(s_file.get(), data);

    ASSERT_OK(env->DeleteFile(fname));
  }
}

TEST_F(TestRocksDBEncryptedEnv, ParallelEncryptedFileOps) {
  
  const auto kWriteThreads = 10;
  TestThreadHolder thread_holder;

  string test_dir;
  ASSERT_OK(Env::Default()->GetTestDirectory(&test_dir));

  for (int j = 0; j != kWriteThreads; ++j) {
    thread_holder.AddThreadFunctor([&test_dir,j, &stop = thread_holder.stop_flag()] {
      auto header_manager = GetMockHeaderManager();
      HeaderManager* hm_ptr = header_manager.get();
      auto env = yb::enterprise::NewRocksDBEncryptedEnv(std::move(header_manager));
      const auto kBigDataSize = 1048576;
      auto bytes = RandomBytes(kBigDataSize);
      Slice data(bytes.data(), bytes.size());

      while (!stop.load()) {
        auto file_name = JoinPathSegments(test_dir, Format("test-file-$0", j));

        if (env->FileExists(file_name).ok()) {
          env->DeleteFile(file_name);
        }

        down_cast<HeaderManagerMockImpl*>(hm_ptr)->SetFileEncryption(true);

        std::unique_ptr<rocksdb::WritableFile> writable_file;
        ASSERT_OK(env->NewWritableFile(file_name, &writable_file, rocksdb::EnvOptions()));
        TestWrites<rocksdb::WritableFile>(writable_file.get(), data);

        std::unique_ptr<rocksdb::RandomAccessFile> ra_file;
        ASSERT_OK(env->NewRandomAccessFile(file_name, &ra_file, rocksdb::EnvOptions()));
        TestRandomAccessReads<rocksdb::RandomAccessFile, uint8_t>(ra_file.get(), data);

        std::unique_ptr<rocksdb::SequentialFile> s_file;
        ASSERT_OK(env->NewSequentialFile(file_name, &s_file, rocksdb::EnvOptions()));
        TestSequentialReads<rocksdb::SequentialFile, uint8_t>(s_file.get(), data);
      }
    });
  }

  thread_holder.WaitAndStop(600s);
}

std::unique_ptr<yb::enterprise::UniverseKeyManager> GenerateUniverseKeyManager() {
  auto universe_key_manager = std::make_unique<yb::enterprise::UniverseKeyManager>();
  UniverseKeyRegistryPB registry;
  auto encryption_params = yb::enterprise::EncryptionParams::NewEncryptionParams();
  EncryptionParamsPB params_pb;
  encryption_params->ToEncryptionParamsPB(&params_pb);
  auto version_id = RandomHumanReadableString(16);
  (*registry.mutable_universe_keys())[version_id] = params_pb;
  registry.set_encryption_enabled(true);
  registry.set_latest_version_id(version_id);
  universe_key_manager->SetUniverseKeyRegistry(registry);
  return universe_key_manager;
}

std::string GetKey(int i) {
  return StringPrintf("key%09dSSSSSSSS", i);
}

std::string GetValue(int i) {
  return Format("value%d", i);
}

TEST_F(TestRocksDBEncryptedEnv, ParallelBlockBasedTables) {
  
  const auto kWriteThreads = 16;
  TestThreadHolder thread_holder;

  string test_dir;
  ASSERT_OK(Env::Default()->GetTestDirectory(&test_dir));

  rocksdb::Options opts;
  const rocksdb::ImmutableCFOptions imoptions(opts);
  auto ikc = std::make_shared<rocksdb::InternalKeyComparator>(opts.comparator);
  std::vector<std::unique_ptr<rocksdb::IntTblPropCollectorFactory> >
    block_based_table_factories;
  rocksdb::CompressionOptions compression_opts;
  rocksdb::TableBuilderOptions table_builder_options(
      imoptions,
      ikc,
      block_based_table_factories,
      rocksdb::CompressionType::kSnappyCompression,
      compression_opts,
      /* skip_filters */ false);

  rocksdb::TableReaderOptions table_reader_options(
      imoptions,
      rocksdb::EnvOptions(), 
      ikc,
      /*skip_filters=*/ false);

  std::shared_ptr<yb::enterprise::UniverseKeyManager> universe_key_manager = 
      GenerateUniverseKeyManager();
      
  auto header_manager = tserver::enterprise::DefaultHeaderManager(universe_key_manager.get());
  auto env = yb::enterprise::NewRocksDBEncryptedEnv(std::move(header_manager));
  
  for (int j = 0; j != kWriteThreads; ++j) {
    thread_holder.AddThreadFunctor(
        [// &test_dir,
         j,
         &stop = thread_holder.stop_flag(), 
         &table_builder_options,
         &table_reader_options,
         &env] {
      
      // auto header_manager = GetMockHeaderManager();
      // HeaderManager* hm_ptr = header_manager.get();
      // down_cast<HeaderManagerMockImpl*>(hm_ptr)->SetFileEncryption(true);
      auto file_name = JoinPathSegments("/Volumes/RamDisk", Format("test-file-$0", j));
      while (!stop.load()) {
        if (env->FileExists(file_name).ok()) {
          env->DeleteFile(file_name);
        }

        std::unique_ptr<rocksdb::WritableFile> base_file;
        ASSERT_OK(env->NewWritableFile(file_name, &base_file, rocksdb::EnvOptions()));
        rocksdb::WritableFileWriter base_writer(std::move(base_file), rocksdb::EnvOptions(), 
            /* suspender */ nullptr);

        string data_file_name = file_name + ".sblock.0";
        std::unique_ptr<rocksdb::WritableFile> data_file;
        ASSERT_OK(env->NewWritableFile(data_file_name, &data_file, rocksdb::EnvOptions()));
        rocksdb::WritableFileWriter data_writer(std::move(data_file), rocksdb::EnvOptions(), 
            /* suspender */ nullptr);

        rocksdb::BlockBasedTableFactory bbtf;
        auto table_builder = std::unique_ptr<rocksdb::TableBuilder>(bbtf.NewTableBuilder(
            table_builder_options, 0, &base_writer, &data_writer
        ));
        const int kNumKeys = 10000000;
        const int start_key = RandomUniformInt(0, 10000);
        for (int i = start_key; i < start_key + kNumKeys; ++i) {
          string key = GetKey(i);
          table_builder->Add(key, GetValue(i));
        }
        ASSERT_OK(table_builder->Finish());
        LOG(INFO) << "Wrote a file of total size " << table_builder->TotalFileSize()
            << ", base file size: " << table_builder->BaseFileSize();
        base_writer.Flush();
        data_writer.Flush();
        base_writer.Close();
        data_writer.Close();

        std::unique_ptr<yb::SequentialFile> seq_file;
        ASSERT_OK(env->NewSequentialFile(file_name, &seq_file, rocksdb::EnvOptions()));
        uint8_t buf[1024];
        Slice slice_read;
        size_t seq_file_size = 0;
        do {
          ASSERT_OK(seq_file->Read(sizeof(buf), &slice_read, buf));
          seq_file_size += slice_read.size();
        } while (slice_read.size() > 0);
        LOG(INFO) << "Total bytes read using SequentialFile: " << seq_file_size;

        std::unique_ptr<rocksdb::RandomAccessFile> random_access_file;
        ASSERT_OK(env->NewRandomAccessFile(file_name, &random_access_file, rocksdb::EnvOptions()));
        auto base_file_size = ASSERT_RESULT(random_access_file->Size());
        LOG(INFO) << "Base file size as reported by RandomAccessFile: " << base_file_size;

        std::unique_ptr<rocksdb::RandomAccessFile> random_access_data_file;
        ASSERT_OK(env->NewRandomAccessFile(
            data_file_name, &random_access_data_file, rocksdb::EnvOptions()));
        auto data_file_reader = std::make_unique<rocksdb::RandomAccessFileReader>(
            std::move(random_access_data_file), env.get());

        size_t raw_size = ASSERT_RESULT(Env::Default()->GetFileSize(file_name));
        LOG(INFO) << "Raw file size as reported by default GetFileSize: " << raw_size;

        auto random_access_file_reader = std::make_unique<rocksdb::RandomAccessFileReader>(
            std::move(random_access_file));

        std::unique_ptr<rocksdb::TableReader> table_reader;

        ASSERT_OK(bbtf.NewTableReader(
            table_reader_options, std::move(random_access_file_reader),
            seq_file_size,
            &table_reader,
            rocksdb::DataIndexLoadMode::PRELOAD_ON_OPEN,
            rocksdb::PrefetchFilter::YES));

        table_reader->SetDataFileReader(std::move(data_file_reader));

        auto it = std::unique_ptr<rocksdb::InternalIterator>(
            table_reader->NewIterator(rocksdb::ReadOptions()));
        it->SeekToFirst();
        int i = start_key;
        while (it->Valid()) {
          ASSERT_EQ(it->key(), GetKey(i));
          ASSERT_EQ(it->value(), GetValue(i));
          i++;
          it->Next();
        }
        std::this_thread::sleep_for(1ms * RandomUniformInt(1, 1000));
      }
    });
  }

  thread_holder.WaitAndStop(500s);
}

} // namespace enterprise
} // namespace yb
