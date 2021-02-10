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

#include <atomic>
#include <stack>
#include "yb/yql/pgwrapper/pg_mini_test_base.h"

#include "yb/common/pgsql_error.h"

#include "yb/util/test_macros.h"
#include "yb/util/random_util.h"

#include "yb/gutil/stringprintf.h"

DECLARE_bool(TEST_fail_in_apply_if_no_metadata);

using namespace std::literals;

namespace yb {
namespace pgwrapper {

class PgTxnTest : public PgMiniTestBase {

};

TEST_F(PgTxnTest, YB_DISABLE_TEST_IN_SANITIZERS(EmptyUpdate)) {
  FLAGS_TEST_fail_in_apply_if_no_metadata = true;

  auto conn = ASSERT_RESULT(Connect());

  ASSERT_OK(conn.Execute("CREATE TABLE test (key TEXT, value TEXT, PRIMARY KEY((key) HASH))"));
  ASSERT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
  ASSERT_OK(conn.Execute("UPDATE test SET value = 'a' WHERE key = 'b'"));
  ASSERT_OK(conn.CommitTransaction());
}

class PgTxnRF1Test : public PgTxnTest {
 public:
  int NumTabletServers() override {
    return 1;
  }
};

TEST_F_EX(PgTxnTest, YB_DISABLE_TEST_IN_TSAN(SelectRF1ReadOnlyDeferred), PgTxnRF1Test) {
  auto conn = ASSERT_RESULT(Connect());

  ASSERT_OK(conn.Execute("CREATE TABLE test (key INT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test VALUES (1)"));
  ASSERT_OK(conn.Execute("BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE"));
  auto res = ASSERT_RESULT(conn.FetchValue<int32_t>("SELECT * FROM test"));
  ASSERT_EQ(res, 1);
  ASSERT_OK(conn.Execute("COMMIT"));
}

template<typename Key>
class CycleFinder {
 public:
  using DirectedGraph = std::map<Key, std::set<Key>>;

  CycleFinder(const DirectedGraph& graph) : graph_(graph) {}

  bool LookForCycle() {
    for (const auto& entry : graph_) {
      Traverse(entry.first);
      if (cycle_found_)
        return true;
    }
    return cycle_found_;
  }

  const std::vector<Key>& cycle() {
    return cycle_;
  }

 private:
  void Traverse(Key v) {
    if (cycle_found_ || visited_.count(v))
      return;
    in_current_path_.insert(v);
    current_path_.push_back(v);
    visited_.insert(v);
    auto it = graph_.find(v);
    if (it != graph_.end()) {
      for (int32_t next_vertex : it->second) {
        if (in_current_path_.count(next_vertex)) {
          if (!cycle_found_) {
            cycle_found_ = true;
            auto it = std::find(current_path_.begin(), current_path_.end(), next_vertex);
            CHECK(it != current_path_.end());
            cycle_.clear();
            std::copy(it, current_path_.end(), std::back_inserter(cycle_));
          }
          return;
        }
        Traverse(next_vertex);
        if (cycle_found_)
          return;
      }
    }
    in_current_path_.erase(v);
    current_path_.pop_back();
  }

  const DirectedGraph graph_;
  std::set<Key> visited_, in_current_path_;
  std::vector<Key> current_path_;
  bool cycle_found_ = false;
  std::vector<Key> cycle_;
};

TEST_F(PgTxnTest, YB_DISABLE_TEST_IN_TSAN(SimplifiedLongFork)) {
  static const char* kDbName = "long_fork_db";
  static const char* kTableName = "long_fork_tbl";
  {
    auto create_db_conn = ASSERT_RESULT(Connect());
    ASSERT_OK(create_db_conn.ExecuteFormat("CREATE DATABASE $0", kDbName));
    ASSERT_OK(create_db_conn.ExecuteFormat(
        "ALTER DATABASE $0 SET DEFAULT_TRANSACTION_ISOLATION TO SERIALIZABLE", kDbName));
    auto conn = ASSERT_RESULT(ConnectToDB(kDbName));

    ASSERT_OK(conn.ExecuteFormat(
        "CREATE TABLE $0("
        "key int PRIMARY KEY, "
        "key2 int NOT NULL, "
        "val int NOT NULL"
        ") SPLIT INTO 2 TABLETS",
        kTableName));

    ASSERT_OK(conn.ExecuteFormat(
        "CREATE INDEX long_fork_idx ON $0 (key2) INCLUDE (val) SPLIT INTO 2 TABLETS",
        kTableName));
  }

  TestThreadHolder thread_holder;
  constexpr int kNumReadThreads = 8;
  constexpr int kNumWriteThreads = 8;

  // Split the keys we will write and read into this number of logical group.
  constexpr int kNumGroups = 3;

  // We will try to read this many recent keys written to each group.
  constexpr int kHindsight = 10;

  std::atomic<int32_t> next_key{0};

  auto& stop = thread_holder.stop_flag();

  // A partial order between key insertions. If we read a and b, don't find a
  // but find b, it means a is written before b. If we find cycles in this
  // graph, it indicates a serializability violation.
  std::mutex partial_order_mutex;
  using DirectedGraph = std::map<int32_t, std::set<int32_t>>;
  DirectedGraph partial_order;

  std::atomic<int> total_attempted_reads{0};
  std::atomic<int> total_successful_reads{0};

  std::atomic<int> total_attempted_writes{0};
  std::atomic<int> total_successful_writes{0};

  std::mutex recent_keys_per_group_mutex;
  std::vector<std::deque<int32_t>> recent_keys_per_group(kNumGroups);

  for (int i = 0; i < kNumReadThreads; ++i) {
    thread_holder.AddThreadFunctor([
        i,
        this,
        &stop,
        &partial_order,
        &partial_order_mutex,
        &total_attempted_reads,
        &total_successful_reads,
        &recent_keys_per_group,
        &recent_keys_per_group_mutex]() {
      LOG(INFO) << "Starting reader thread " << i;
      auto read_conn = ASSERT_RESULT(ConnectToDB(kDbName));
      std::vector<int32_t> keys_to_read;
      keys_to_read.reserve(kHindsight * kNumGroups);
      while (!stop.load(std::memory_order_acquire)) {
        total_attempted_reads.fetch_add(1, std::memory_order_acq_rel);
        std::string comma_separated_keys;
        keys_to_read.clear();
        {
          std::lock_guard<std::mutex> recent_keys_per_group_lock(recent_keys_per_group_mutex);
          for (auto& group : recent_keys_per_group) {
            std::copy(group.begin(), group.end(), std::back_inserter(keys_to_read));
          }
        }
        if (keys_to_read.empty()) {
          std::this_thread::sleep_for(50ms);
          continue;
        }

        for (int32_t k : keys_to_read) {
          if (!comma_separated_keys.empty()) {
            comma_separated_keys += ", ";
          }
          comma_separated_keys += Format("$0", k);
        }

        auto result = read_conn.FetchColumns(
            Format("SELECT key2, val FROM $0 WHERE key IN ($1)",
                   kTableName, comma_separated_keys),
            2);
        if (!result.ok()) {
          auto status = result.status();
          auto sql_error = PgsqlError(status);
          ASSERT_EQ(sql_error, YBPgErrorCode::YB_PG_T_R_SERIALIZATION_FAILURE) << status;
          continue;
        }
        total_successful_reads.fetch_add(1, std::memory_order_acq_rel);

        auto fetched_rows = PQntuples(result->get());

        std::set<int32_t> keys_read;
        for (int i = 0; i < fetched_rows; ++i) {
          int32_t key2 = ASSERT_RESULT(GetInt32(result->get(), i, 0));
          int32_t val = ASSERT_RESULT(GetInt32(result->get(), i, 1));
          ASSERT_EQ(key2, val);
          keys_read.insert(val);
        }

        std::vector<std::pair<int32_t, int32_t>> new_orderings;

        for (int32_t k1 : keys_to_read) {
          if (!keys_read.count(k1)) {
            for (int32_t k2 : keys_read) {
              // In this read operation, we did not see k1 but saw k2.
              // This means k2 was written before k1.
              new_orderings.emplace_back(k2, k1);
            }
          }
        }

        if (!new_orderings.empty()) {
          std::lock_guard<std::mutex> partial_order_lock(partial_order_mutex);
          for (auto& key_pair : new_orderings) {
            partial_order[key_pair.first].insert(key_pair.second);
          }
        }

      }
      LOG(INFO) << "Exiting reader thread " << i;
    });
  }

  for (int i = 0; i < kNumWriteThreads; ++i) {
    thread_holder.AddThreadFunctor([
        i,
        this,
        &stop,
        &next_key,
        &total_attempted_writes,
        &total_successful_writes,
        &recent_keys_per_group,
        &recent_keys_per_group_mutex]() {
      LOG(INFO) << "Starting writer thread " << i;
      auto write_conn = ASSERT_RESULT(ConnectToDB(kDbName));
      while (!stop.load(std::memory_order_acquire)) {
        ASSERT_OK(write_conn.Execute(
            "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE"));
        const int k = next_key.fetch_add(1, std::memory_order_acq_rel);
        const int group_index = RandomUniformInt(0, kNumGroups - 1);
        {
          std::lock_guard<std::mutex> recent_keys_per_group_lock(recent_keys_per_group_mutex);
          auto& group = recent_keys_per_group[group_index];
          group.push_back(k);
          if (group.size() > kHindsight) {
            group.pop_front();
          }
        }

        auto status = write_conn.ExecuteFormat(
            "INSERT INTO $0(key, key2, val) VALUES ($1, $1, $1)", kTableName, k);
        if (status.ok()) {
          status = write_conn.Execute("COMMIT");
        }

        total_attempted_writes.fetch_add(1, std::memory_order_acq_rel);
        if (status.ok()) {
          total_successful_writes.fetch_add(1, std::memory_order_acq_rel);
        } else {
          PgsqlError sql_error(status);
          ASSERT_EQ(sql_error, YBPgErrorCode::YB_PG_T_R_SERIALIZATION_FAILURE) << status;
          ASSERT_OK(write_conn.Execute("ROLLBACK"));
        }
      }
      LOG(INFO) << "Exiting reader thread " << i;
    });
  }

  auto report_partial_stats = [](const string& op_type, int num_attempted, int num_successful) {
    LOG(INFO) << "Total attempted " << op_type << ": " << num_attempted;
    LOG(INFO) << "Total successful " << op_type << ": " << num_successful;
    if (num_attempted > 0) {
      LOG(INFO) << "Success rate for " << op_type << ": " << StringPrintf(
          "%.2f%%", num_successful * 100.0 / num_attempted);
    }
    LOG(INFO) << string(80, '-');
  };

  auto report_all_stats = [
      &report_partial_stats,
      &total_attempted_reads,
      &total_successful_reads,
      &total_attempted_writes,
      &total_successful_writes](const std::string& extra_description = ""s) {
    report_partial_stats(
        "reads" + extra_description,
        total_attempted_reads.load(std::memory_order_acquire),
        total_successful_reads.load(std::memory_order_acquire));

    report_partial_stats(
        "writes" + extra_description,
        total_attempted_writes.load(std::memory_order_acquire),
        total_successful_writes.load(std::memory_order_acquire));
  };

  thread_holder.AddThreadFunctor([
      &stop,
      &partial_order,
      &partial_order_mutex,
      &report_all_stats]() {
    LOG(INFO) << "Cycle finding thread is starting";
    while (!stop.load(std::memory_order_acquire)) {
      DirectedGraph graph;
      {
        std::lock_guard<std::mutex> partial_order_lock(partial_order_mutex);
        graph = partial_order;
      }
      CycleFinder<int32_t> cycle_finder(graph);
      if (cycle_finder.LookForCycle()) {
        auto msg = Format("Cycle found: $0", cycle_finder.cycle());
        LOG(ERROR) << "Cycle found: " << msg;
        stop.store(true, std::memory_order_release);
        FAIL() << msg;
        break;
      }

      report_all_stats();
      for (int i = 0; i < 100 && !stop.load(std::memory_order_acquire); ++i) {
        std::this_thread::sleep_for(100ms);
      }
    }
    LOG(INFO) << "Cycle finding thread is exiting";
  });

  thread_holder.WaitAndStop(300s);

  report_all_stats(" (final)");
}

} // namespace pgwrapper
} // namespace yb
