//
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

#ifndef YB_UTIL_SHARED_LOCK_H
#define YB_UTIL_SHARED_LOCK_H

#include <shared_mutex>

#include "yb/gutil/thread_annotations.h"

namespace yb {

// A wrapper around Boost shared lock that supports thread annotations.
template<typename Mutex>
class SCOPED_CAPABILITY SharedLock {
 public:
  explicit SharedLock(Mutex &mutex) ACQUIRE_SHARED(mutex) : m_lock(mutex) {}

  // One would think that in the destructor we would use RELEASE_SHARED(mutex), but for some reason
  // that does not work. See http://bit.ly/shared_lock
  ~SharedLock() RELEASE() = default;

  // The first argument to TRY_ACQUIRE_SHARED is technically the value that the function returns
  // in case of successful lock acquisition, but obviously the constructor does not return any
  // value.
  SharedLock(Mutex& m, std::try_to_lock_t t) TRY_ACQUIRE_SHARED(true, m)
      : m_lock(m, t) {}

  void swap(SharedLock<Mutex>& other) {
    std::swap(m_lock, other.m_lock);
  }

  bool owns_lock() const {
    return m_lock.owns_lock();
  }

  void unlock() RELEASE() {
    m_lock.release();
  }

  // Making this a factory method instead of default constructor, to avoid forgetting to specify
  // the mutex to lock.
  static SharedLock<Mutex> CreateUnlocked() { return SharedLock<Mutex>(); }

 private:
  SharedLock() = default;
  std::shared_lock<Mutex> m_lock;
};

} // namespace yb

#endif  // YB_UTIL_SHARED_LOCK_H
