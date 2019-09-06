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

#include "yb/util/random_util.h"
#include "yb/util/scope_exit.h"
#include "yb/util/size_literals.h"

#include "yb/yql/pgwrapper/libpq_utils.h"
#include "yb/yql/pgwrapper/libpq_test_base.h"

#include "yb/common/common.pb.h"

using namespace std::literals;

DECLARE_int64(external_mini_cluster_max_log_bytes);
DECLARE_int64(retryable_rpc_single_call_timeout_ms);

METRIC_DECLARE_entity(tablet);
METRIC_DECLARE_counter(transaction_not_found);

namespace yb {
namespace pgwrapper {

class PgReadRestartTest : public LibPqTestBase {
 protected:
  void TestOnConflict(bool kill_master, const MonoDelta& duration);
};

TEST_F(PgReadRestartTest, YB_DISABLE_TEST_IN_TSAN(CountWithConcurrentInserts)) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(Execute(conn.get(), "CREATE TABLE t (key INT PRIMARY KEY)"));
  TestThreadHolder thread_holder;

  std::atomic<int> inserts(0);
  std::atomic<int> reads(0);

  const auto kWriterThreads = RegularBuildVsSanitizers(4, 2);
  const auto kTotalNumKeys = 1000;
  for (int i = 1; i <= kWriterThreads; ++i) {
    thread_holder.AddThreadFunctor(
        [this, i, &stop_flag = thread_holder.stop_flag(), &inserts]() {
      auto write_conn = ASSERT_RESULT(Connect());
      int write_key = RandomUniformInt(1, kTotalNumKeys);
      while (!stop_flag.load(std::memory_order_acquire)) {
        VLOG(1) << "Thread " << i << " writing key " << write_key;
        SCOPED_TRACE(Format("Writing: $0", write_key));

        ASSERT_OK(Execute(write_conn.get(), "START TRANSACTION ISOLATION LEVEL REPEATABLE READ"));
        auto status = Execute(write_conn.get(), Format("INSERT INTO t (key) VALUES ($0)", write_key));
        if (status.ok()) {
          status = Execute(write_conn.get(), "COMMIT");
        }
        if (status.ok()) {
          inserts.fetch_add(1, std::memory_order_acq_rel);
          ++write_key;
        } else {
          LOG(WARNING) << "Write " << write_key << " failed: " << status;
        }
        if (!status.ok()) {
          ASSERT_TRUE(
              TransactionalFailure(status) ||
              // TODO: use SQLSTATE to test for this condition.
              status.ToString().find("duplicate key value violates unique constraint") !=
                  std::string::npos);
          ASSERT_OK(Execute(write_conn.get(), "ROLLBACK"));
        }
      }
    });
  }

  // One reader thread.
  thread_holder.AddThreadFunctor(
      [this, &stop_flag = thread_holder.stop_flag(), 
       &inserts, &reads]() {
    auto read_conn = ASSERT_RESULT(Connect());
    while (!stop_flag.load(std::memory_order_acquire)) {
      const int64_t min_expected_count = inserts.load(std::memory_order_acquire);
      ASSERT_OK(Execute(read_conn.get(), "START TRANSACTION ISOLATION LEVEL REPEATABLE READ"));
      auto res = ASSERT_RESULT(Fetch(read_conn.get(), "SELECT COUNT(*) FROM t"));
      auto num_rows = PQntuples(res.get());
      ASSERT_EQ(1, num_rows);

      auto num_columns = PQnfields(res.get());
      ASSERT_EQ(1, num_columns);

      const int64_t count = ASSERT_RESULT(GetInt64(res.get(), 0, 0));
      ASSERT_GE(count, min_expected_count);
      
      ASSERT_OK(Execute(read_conn.get(), "COMMIT"));
      reads.fetch_add(1, std::memory_order_acq_rel);
    }
  });

  const auto kRequiredWrites = 100;
  const auto kRequiredReads = 10;
  const auto kTimeout = 30s;
  auto wait_status = WaitFor([&reads, &inserts, &stop = thread_holder.stop_flag()] {
    return stop.load() || (inserts.load() >= kRequiredWrites && reads.load() >= kRequiredReads);
  }, kTimeout, Format("At least $0 reads and $1 writes", kRequiredReads, kRequiredWrites));

  LOG(INFO) << "Number of successful inserts: " << inserts;
  LOG(INFO) << "Number of successful reads: " << reads;
  thread_holder.Stop();
}

TEST_F(PgReadRestartTest, YB_DISABLE_TEST_IN_TSAN(ReadRestart)) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(Execute(conn.get(), "CREATE TABLE t (key INT PRIMARY KEY)"));

  std::atomic<bool> stop(false);
  std::atomic<int> last_written(0);

  std::thread write_thread([this, &stop, &last_written] {
    auto write_conn = ASSERT_RESULT(Connect());
    int write_key = 1;
    while (!stop.load(std::memory_order_acquire)) {
      SCOPED_TRACE(Format("Writing: $0", write_key));

      ASSERT_OK(Execute(write_conn.get(), "BEGIN"));
      auto status = Execute(write_conn.get(), Format("INSERT INTO t (key) VALUES ($0)", write_key));
      if (status.ok()) {
        status = Execute(write_conn.get(), "COMMIT");
      }
      if (status.ok()) {
        last_written.store(write_key, std::memory_order_release);
        ++write_key;
      } else {
        LOG(INFO) << "Write " << write_key << " failed: " << status;
      }
    }
  });

  auto se = ScopeExit([&stop, &write_thread] {
    stop.store(true, std::memory_order_release);
    write_thread.join();
  });

  auto deadline = CoarseMonoClock::now() + 30s;

  while (CoarseMonoClock::now() < deadline) {
    int read_key = last_written.load(std::memory_order_acquire);
    if (read_key == 0) {
      std::this_thread::sleep_for(100ms);
      continue;
    }

    SCOPED_TRACE(Format("Reading: $0", read_key));

    ASSERT_OK(Execute(conn.get(), "BEGIN"));

    auto res = ASSERT_RESULT(Fetch(conn.get(), Format("SELECT * FROM t WHERE key = $0", read_key)));
    auto columns = PQnfields(res.get());
    ASSERT_EQ(1, columns);

    auto lines = PQntuples(res.get());
    ASSERT_EQ(1, lines);

    auto key = ASSERT_RESULT(GetInt32(res.get(), 0, 0));
    ASSERT_EQ(key, read_key);

    ASSERT_OK(Execute(conn.get(), "ROLLBACK"));
  }

  ASSERT_GE(last_written.load(std::memory_order_acquire), 100);
}

}  // namespace pgwrapper
}  // namespace yb