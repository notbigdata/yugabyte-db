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

#include "yb/rocksdb/db/simple_db_iter.h"

#include <stdexcept>
#include <string>
#include <limits>

#include "yb/rocksdb/db/dbformat.h"
#include "yb/rocksdb/db/filename.h"
#include "yb/rocksdb/port/port.h"
#include "yb/rocksdb/env.h"
#include "yb/rocksdb/iterator.h"
#include "yb/rocksdb/merge_operator.h"
#include "yb/rocksdb/options.h"
#include "yb/rocksdb/table/internal_iterator.h"
#include "yb/rocksdb/util/arena.h"
#include "yb/rocksdb/util/logging.h"
#include "yb/rocksdb/util/mutexlock.h"
#include "yb/rocksdb/util/perf_context_imp.h"

#include "yb/util/string_util.h"

namespace rocksdb {

inline bool SimpleDBIter::ParseKey(ParsedInternalKey* ikey) {
  if (!ParseInternalKey(iter_->key(), ikey)) {
    status_ = STATUS(Corruption, "corrupted internal key in DBIter");
    RLOG(InfoLogLevel::ERROR_LEVEL,
        logger_, "corrupted internal key in DBIter: %s",
        iter_->key().ToString(true).c_str());
    return false;
  } else {
    return true;
  }
}

void SimpleDBIter::Next() {
  assert(valid_);

  if (direction_ == kReverse) {
    FindNextUserKey();
    direction_ = kForward;
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    }
  } else if (iter_->Valid()) {
    // The iter position is the current key, which is already returned. We can safely issue a
    // Next() without checking the current key.
    iter_->Next();
  }

  // Now we point to the next internal position.
  if (!iter_->Valid()) {
    valid_ = false;
    return;
  }
  FindNextUserEntry();
}

// POST: saved_key_ should have the next user key if valid_,
//       if the current entry is a result of merge
//           current_entry_is_merged_ => true
//           saved_value_             => the merged value
//
// NOTE: In between, saved_key_ can point to a user key that has
//       a delete marker
inline void SimpleDBIter::FindNextUserEntry() {
  FindNextUserEntryInternal();
}

// Actual implementation of SimpleDBIter::FindNextUserEntry()
void SimpleDBIter::FindNextUserEntryInternal() {
  assert(iter_->Valid());
  assert(direction_ == kForward);
  uint64_t num_skipped = 0;
  do {
    ParsedInternalKey ikey;
    if (ParseKey(&ikey)) {
      switch (ikey.type) {
        case kTypeValue:
          valid_ = true;
          saved_key_.SetKey(ikey.user_key,
                            !iter_->IsKeyPinned() /* copy */);
          return;
        default:
          LOG(FATAL) << "Unsupported internal key type: " << ikey.type;
          break;
      }
    }
    iter_->Next();
  } while (iter_->Valid());
  valid_ = false;
}

void SimpleDBIter::Prev() {
  assert(valid_);
  if (direction_ == kForward) {
    ReverseToBackward();
  }
  PrevInternal();
}

void SimpleDBIter::ReverseToBackward() {
#ifndef NDEBUG
  if (iter_->Valid()) {
    ParsedInternalKey ikey;
    assert(ParseKey(&ikey));
    assert(user_comparator_->Compare(ikey.user_key, saved_key_.GetKey()) <= 0);
  }
#endif

  FindPrevUserKey();
  direction_ = kReverse;
}

void SimpleDBIter::PrevInternal() {
  if (!iter_->Valid()) {
    valid_ = false;
    return;
  }

  ParsedInternalKey ikey;

  while (iter_->Valid()) {
    saved_key_.SetKey(ExtractUserKey(iter_->key()),
                      !iter_->IsKeyPinned() /* copy */);
    if (FindValueForCurrentKey()) {
      valid_ = true;
      if (!iter_->Valid()) {
        return;
      }
      FindParseableKey(&ikey, kReverse);
      if (user_comparator_->Equal(ikey.user_key, saved_key_.GetKey())) {
        FindPrevUserKey();
      }
      return;
    }
    if (!iter_->Valid()) {
      break;
    }
    FindParseableKey(&ikey, kReverse);
    if (user_comparator_->Equal(ikey.user_key, saved_key_.GetKey())) {
      FindPrevUserKey();
    }
  }
  // We haven't found any key - iterator is not valid
  assert(!iter_->Valid());
  valid_ = false;
}

// This function checks, if the entry with biggest sequence_number <= sequence_
// is non kTypeDeletion or kTypeSingleDeletion. If it's not, we save value in
// saved_value_
bool SimpleDBIter::FindValueForCurrentKey() {
  assert(iter_->Valid());
  ValueType last_key_entry_type = kTypeDeletion;

  ParsedInternalKey ikey;
  FindParseableKey(&ikey, kReverse);

  while (iter_->Valid() &&
         user_comparator_->Equal(ikey.user_key, saved_key_.GetKey())) {
    last_key_entry_type = ikey.type;
    switch (last_key_entry_type) {
      case kTypeValue:
        saved_value_ = iter_->value().ToString();
        break;
      default:
        LOG(INFO) << "Unsupported internal key type: " << last_key_entry_type;
    }

    assert(user_comparator_->Equal(ikey.user_key, saved_key_.GetKey()));
    iter_->Prev();
    FindParseableKey(&ikey, kReverse);
  }

  switch (last_key_entry_type) {
    case kTypeValue:
      // do nothing - we've already has value in saved_value_
      break;
    default:
      LOG(FATAL) << "Unsupported internal key type: " << last_key_entry_type;
  }
  valid_ = true;
  return true;
}

// Used in Next to change directions
// Go to next user key
// Don't use Seek(),
// because next user key will be very close
void SimpleDBIter::FindNextUserKey() {
  if (!iter_->Valid()) {
    return;
  }
  ParsedInternalKey ikey;
  FindParseableKey(&ikey, kForward);
  while (iter_->Valid() &&
         !user_comparator_->Equal(ikey.user_key, saved_key_.GetKey())) {
    iter_->Next();
    FindParseableKey(&ikey, kForward);
  }
}

// Go to previous user_key
void SimpleDBIter::FindPrevUserKey() {
  if (!iter_->Valid()) {
    return;
  }
  size_t num_skipped = 0;
  ParsedInternalKey ikey;
  FindParseableKey(&ikey, kReverse);
  int cmp;
  while (iter_->Valid() && ((cmp = user_comparator_->Compare(
                                 ikey.user_key, saved_key_.GetKey())) == 0 ||
                            (cmp > 0 && ikey.sequence > sequence_))) {
    iter_->Prev();
    FindParseableKey(&ikey, kReverse);
  }
}

// Skip all unparseable keys
void SimpleDBIter::FindParseableKey(ParsedInternalKey* ikey, Direction direction) {
  while (iter_->Valid() && !ParseKey(ikey)) {
    if (direction == kReverse) {
      iter_->Prev();
    } else {
      iter_->Next();
    }
  }
}

void SimpleDBIter::Seek(const Slice& target) {
  saved_key_.Clear();
  // now saved_key is used to store internal key.
  saved_key_.SetInternalKey(target, sequence_);

  iter_->Seek(saved_key_.GetKey());

  if (iter_->Valid()) {
    direction_ = kForward;
    ClearSavedValue();
    FindNextUserEntry();
  } else {
    valid_ = false;
  }
}

void SimpleDBIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();

  iter_->SeekToFirst();

  if (iter_->Valid()) {
    FindNextUserEntry();
  } else {
    valid_ = false;
  }
}

void SimpleDBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();

  iter_->SeekToLast();
  PrevInternal();
}

}  // namespace rocksdb
