package org.yb.cql;

import com.datastax.driver.core.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.yb.YBTestRunner;
import org.yb.minicluster.BaseMiniClusterTest;
import org.yb.minicluster.MiniYBDaemon;

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

      final int numSwapThreads = 8;
      final int numRotate3Threads = 2;
      final int numReaderThread = 1;
      ExecutorCompletionService ecs = new ExecutorCompletionService(
        Executors.newFixedThreadPool(
          numSwapThreads + numRotate3Threads + numReaderThread ));
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

      final List<AtomicInteger> lastValues = new ArrayList<AtomicInteger>();
      for (int i = 0; i <= numKeys; ++i) {
        lastValues.add(new AtomicInteger(i));
      }

      // Insertion / overwrite threads.
      for (int wThreadIndex = 1; wThreadIndex <= numSwapThreads; ++wThreadIndex) {
        final String threadName = "Workload writer thread (swapping 2 elements) " + wThreadIndex;
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
              if (false) {
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
              }
              iValue = lastValues.get(i).get();
              jValue = lastValues.get(j).get();

              session.execute(
                  compareAndSetTwoValues.bind(jValue, i, iValue, iValue, j, jValue));
              lastValues.get(i).set(jValue);
              lastValues.get(j).set(iValue);
              numSwapSuccesses.incrementAndGet();
            } catch (Exception ex) {
              if (isRetryableError(ex.getMessage())) {
                continue;
              }
              LOG.error("Exception in: {}", threadName, ex);
              stop.set(true);
              failed.set(true);
              break;
            }
          }
          return null;
        }));
      }

      final AtomicInteger numRotate3Attempts = new AtomicInteger(0);
      final AtomicInteger numRotate3Successes = new AtomicInteger(0);

      final PreparedStatement rotate3Statement = session.prepare(
          "BEGIN TRANSACTION " +
          "UPDATE ks.t SET v = ? WHERE k = ? IF v = ? ELSE ERROR; " +
          "UPDATE ks.t SET v = ? WHERE k = ? IF v = ? ELSE ERROR; " +
          "UPDATE ks.t SET v = ? WHERE k = ? IF v = ? ELSE ERROR; " +
          "END TRANSACTION;");
      final PreparedStatement select3ExistingValues = session.prepare(
        "SELECT k, v FROM ks.t WHERE k IN (?, ?, ?)");

      // Insertion / overwrite threads.
      for (int rotateThreadIndex = 1; rotateThreadIndex <= numRotate3Threads; ++rotateThreadIndex) {
        final String threadName =
            "Workload writer thread (rotating 3 elements) " + rotateThreadIndex;
        futures.add(ecs.submit(() -> {
          Thread.currentThread().setName(threadName);
          LOG.info("Thread starting: {}", threadName);
          while (!stop.get()) {
            try {
              numRotate3Attempts.incrementAndGet();
              int i = ThreadLocalRandom.current().nextInt(1, numKeys + 1);
              int j = ThreadLocalRandom.current().nextInt(1, numKeys);
              if (j == i) j++;
              int k;
              do {
                k = ThreadLocalRandom.current().nextInt(1, numKeys + 1);
              } while (k == i || k == j);

              int iValue = 0;
              int jValue = 0;
              int kValue = 0;
              if (false) {
                List<Row> existingValues = session.execute(select3ExistingValues.bind(i, j, k)).all();
                assertEquals(3, existingValues.size());
                for (Row row : existingValues) {
                  int key = row.getInt(0);
                  int v = row.getInt(1);
                  if (key == i) {
                    iValue = v;
                  } else if (key == j) {
                    jValue = v;
                  } else if (key == k) {
                    kValue = v;
                  } else {
                    throw new AssertionError("Unexpected key: " + key);
                  }
                }
                assertNotEquals(0, iValue);
                assertNotEquals(0, jValue);
                assertNotEquals(0, kValue);
              }
              iValue = lastValues.get(i).get();
              jValue = lastValues.get(j).get();
              kValue = lastValues.get(k).get();

              session.execute(
                rotate3Statement.bind(
                    jValue, i, iValue,
                    kValue, j, jValue,
                    iValue, k, kValue));
              numRotate3Successes.incrementAndGet();
              lastValues.get(i).set(jValue);
              lastValues.get(j).set(kValue);
              lastValues.get(k).set(iValue);
            } catch (Exception ex) {
              if (isRetryableError(ex.getMessage())) {
                continue;
              }
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
//      long WORKLOAD_TIME_MS = 180000;
      long startTimeMs = System.currentTimeMillis();

//      while (!stop.get() && System.currentTimeMillis() < startTimeMs + WORKLOAD_TIME_MS) {
//        Thread.sleep(500);
//      }
      for (MiniYBDaemon tserver : miniCluster.getTabletServers().values()) {
        Thread.sleep(60000);
        LOG.info("Restarting tablet server " + tserver);
        tserver.restart();
        LOG.info("Restarted tablet server " + tserver);
        Thread.sleep(60000);
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
      LOG.info("Number of rotate-3 transaction attempts: " + numRotate3Attempts.get());
      LOG.info("Number of rotate-3 transaction successes: " + numRotate3Successes.get());
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

  private boolean isRetryableError(String msg) {
    return msg.contains("Duplicate value disallowed") ||
           msg.contains("Duplicate request") ||
           msg.contains("Condition on table ") ||
           msg.contains("Transaction expired or aborted by a conflict");
  }

}
