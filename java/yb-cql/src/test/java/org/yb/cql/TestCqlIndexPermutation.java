package org.yb.cql;

import com.datastax.driver.core.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.yb.YBTestRunner;
import org.yb.minicluster.BaseMiniClusterTest;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.yb.AssertionWrappers.*;

@RunWith(value=YBTestRunner.class)
public class TestCqlIndexPermutation extends BaseCQLTest {

  @BeforeClass
  public static void SetUpBeforeClass() throws Exception {
    BaseMiniClusterTest.tserverArgs.add("--allow_index_table_read_write");
//    BaseMiniClusterTest.tserverArgs.add(
//      "--index_backfill_upperbound_for_user_enforced_txn_duration_ms=1000");
//    BaseMiniClusterTest.tserverArgs.add(
//      "--index_backfill_wait_for_old_txns_ms=100");
    BaseCQLTest.setUpBeforeClass();
  }

  @Override
  public int getTestMethodTimeoutSec() {
    return 600;
  }

  @Test(timeout = 3600 * 1000)
  public void testCqlIndexConsistency() throws Exception {
    final int numKeys = 50;
    try (Cluster cluster = getDefaultClusterBuilder().build();
         final Session session = cluster.connect()) {
      session.execute("CREATE KEYSPACE ks");
      String createTable =
        "CREATE TABLE ks.t (" +
          "k INT, " +
          "v INT, " +
          "PRIMARY KEY (k)" +
          ") WITH transactions = { 'enabled' : true };";
      String createIndex =
        "CREATE UNIQUE INDEX index_on_v ON ks.t (v);";
      session.execute(createTable);
      session.execute(createIndex);
      PreparedStatement insertRowStatement =
          session.prepare("INSERT INTO ks.t (k, v) VALUES (?, ?)");

      for (int i = 1; i <= numKeys; ++i) {
        session.execute(insertRowStatement.bind(i, i));
      }

      final int numWriterThreads = 4;
      final int numReaderThread = 1;
      ExecutorCompletionService ecs = new ExecutorCompletionService(
        Executors.newFixedThreadPool(
          numWriterThreads + numReaderThread ));
      final List<Future<Void>> futures = new ArrayList<>();
      final AtomicBoolean stop = new AtomicBoolean(false);

      final PreparedStatement selectExistingValues = session.prepare(
          "SELECT k, v FROM ks.t WHERE k IN (?, ?)");
      final PreparedStatement compareAndSetTwoValues = session.prepare(
          "BEGIN TRANSACTION " +
          "UPDATE ks.t SET v = ? WHERE k = ? IF v = ? ELSE ERROR; " +
          "UPDATE ks.t SET v = ? WHERE k = ? IF v = ? ELSE ERROR; " +
          "END TRANSACTION;");

      final AtomicBoolean failed = new AtomicBoolean(false);
      final AtomicInteger numSwapAttempts = new AtomicInteger(0);
      final AtomicInteger numSwapSuccesses = new AtomicInteger(0);

      // Insertion / overwrite threads.
      for (int wThreadIndex = 1; wThreadIndex <= numWriterThreads; ++wThreadIndex) {
        final String threadName = "Workload writer thread " + wThreadIndex;
        futures.add(ecs.submit(() -> {
          Thread.currentThread().setName(threadName);
          LOG.info("Thread starting: {}", threadName);
          while (!stop.get()) {
            try {
              numSwapAttempts.incrementAndGet();
              int i = ThreadLocalRandom.current().nextInt(1, numKeys + 1);
              int j = ThreadLocalRandom.current().nextInt(1, numKeys);
              if (j == i) j++;
              List<Row> existingValues = session.execute(selectExistingValues.bind(i, j)).all();
              assertEquals(2, existingValues.size());
              int iValue = 0;
              int jValue = 0;
              for (Row row : existingValues) {
                int k = row.getInt(0);
                int v = row.getInt(1);
                if (k == i) {
                  iValue = v;
                } else if (k == j) {
                  jValue = v;
                } else {
                  throw new AssertionError("Unexpected key: " + k);
                }
              }
              assertNotEquals(0, iValue);
              assertNotEquals(0, jValue);

              session.execute(
                  compareAndSetTwoValues.bind(jValue, i, iValue, iValue, j, jValue));
              numSwapSuccesses.incrementAndGet();
            } catch (com.datastax.driver.core.exceptions.InvalidQueryException ex) {
              String msg = ex.getMessage();
              if (msg.contains("Duplicate value disallowed") ||
                msg.contains("Duplicate request") ||
                msg.contains("Condition on table ")) {
                continue;
              }
              LOG.error("Exception in: {}", threadName, ex);
              stop.set(true);
              failed.set(true);
              break;
            } catch (Exception ex) {
              LOG.error("Exception in: {}", threadName, ex);
              stop.set(true);
              failed.set(true);
              break;
            }
          }
          return null;
        }));
      }
//
//      // Deletion threads.
//      final PreparedStatement preparedDeleteStatement = session.prepare(
//        "DELETE FROM global_keys.global_akpk_one_to_one " +
//          "WHERE key_id = ? AND key_type = ?");
//
//      for (int dThreadIndex = 1; dThreadIndex <= numDeletionThreads; ++dThreadIndex) {
//        final String threadName = "Workload deletion thread " + dThreadIndex;
//        futures.add(ecs.submit(() -> {
//          Thread.currentThread().setName(threadName);
//          while (!stop.get()) {
//            try {
//              StringBuilder sb = new StringBuilder();
//              String k = genRandomKeyId();
//              String keyType = genRandomKeyType();
//
//              BoundStatement boundDeleteStatement = preparedDeleteStatement.bind(k, keyType);
//              numDeletionAttempts.incrementAndGet();
//
//              int lockIndex = keyAndTypeToLockIndex(k, keyType);
//              Lock lock = locks.get(lockIndex);
//              lock.lock();
//
//              try {
//                session.execute(boundDeleteStatement);
//                numDeletionSuccesses.incrementAndGet();
//              } finally {
//                lock.unlock();
//              }
//            } catch (Exception ex) {
//              LOG.error("Exception in: {}", threadName, ex);
//              stop.set(true);
//              failed.set(true);
//              break;
//            }
//          }
//          return null;
//        }));
//      }

      AtomicInteger numSuccessfulVerifications = new AtomicInteger(0);

      for (int rThreadIndex = 1; rThreadIndex <= numReaderThread; ++rThreadIndex) {
        final String threadName = "Workload reader thread " + rThreadIndex;
        futures.add(ecs.submit(() -> {
          Thread.currentThread().setName(threadName);
          LOG.info("Thread starting: {}", threadName);
          int[] permutation = new int[numKeys + 1];
          int[] reversePermutation = new int[numKeys + 1];

          boolean selectFromIndex = false;
          while (!stop.get()) {
            try {
              List<Row> allRows = session.execute(
                selectFromIndex ? "SELECT * FROM ks.index_on_v" : "SELECT k, v FROM ks.t"
              ).all();

              for (int i = 1; i <= numKeys; ++i) {
                permutation[i] = 0;
                reversePermutation[i] = 0;
              }
              for (Row row : allRows) {
                int k = row.getInt(0);
                int v = row.getInt(1);
                assertEquals(0, permutation[k]);
                permutation[k] = v;
                assertEquals(0, reversePermutation[v]);
                reversePermutation[v] = k;
              }
              numSuccessfulVerifications.incrementAndGet();
            } catch (Exception ex) {
              LOG.error("Exception in: {} (selectFromIndex: {})", threadName, selectFromIndex, ex);
              stop.set(true);
              failed.set(true);
              break;
            }
            selectFromIndex = !selectFromIndex;
          }
          return null;
        }));
      }

      LOG.info("Workload started");
      long WORKLOAD_TIME_MS = 180000;
      long startTimeMs = System.currentTimeMillis();
      while (!stop.get() && System.currentTimeMillis() < startTimeMs + WORKLOAD_TIME_MS) {
        Thread.sleep(500);
      }
      LOG.info("Workload finishing after " + (System.currentTimeMillis() - startTimeMs) + " ms");
      stop.set(true);
      for (Future<Void> future : futures) {
        future.get();
      }
      LOG.info(String.format(
        "Workload stopped. Total workload time: %.1f sec",
        (System.currentTimeMillis() - startTimeMs) / 1000.0
      ));

      LOG.info("Number of swap transaction attempts: " + numSwapAttempts.get());
      LOG.info("Number of swap transaction successes: " + numSwapSuccesses.get());
      LOG.info("Number of successful verifications: " + numSuccessfulVerifications.get());
//      LOG.info("Number of deletion attempts: " + numDeletionAttempts.get());
//      LOG.info("Number of deletion successes: " + numDeletionSuccesses.get());
//      LOG.info("Number of reads where the row is not found: " + numReadsRowNotFound.get());
//      LOG.info("Number of reads where the row is found: " + numReadsRowFound.get());
//      LOG.info("Number of reads where row and index entry are found: " +
//        numReadsRowAndIndexFound.get());
      assertFalse(failed.get());
    }
  }

}
