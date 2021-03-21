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

import static org.yb.AssertionWrappers.assertEquals;
import static org.yb.AssertionWrappers.assertFalse;

@RunWith(value=YBTestRunner.class)
public class TestCqlIndexConsistency extends BaseCQLTest {

  @BeforeClass
  public static void SetUpBeforeClass() throws Exception {
    BaseMiniClusterTest.tserverArgs.add("--allow_index_table_read_write");
    BaseMiniClusterTest.tserverArgs.add(
      "--index_backfill_upperbound_for_user_enforced_txn_duration_ms=1000");
    BaseMiniClusterTest.tserverArgs.add(
      "--index_backfill_wait_for_old_txns_ms=100");
    BaseCQLTest.setUpBeforeClass();
  }


  private static final int MIN_KEY_ID = 100_000_000;
  private static final int NUM_KEY_IDS = 100;
  private static final int NUM_VALUES = 200;

  private static int NUM_KEY_TYPES = 2;
  private static String KEY_TYPE1 = "ITEMID_TO_WPID";
  private static String KEY_TYPE2 = "ANOTHERKEYTYPE";

  private static String DELIMITER = "_";

  private static final int NUM_LOCKS = NUM_KEY_IDS * NUM_KEY_TYPES;

  @Override
  public int getTestMethodTimeoutSec() {
    return 600;
  }

  private static String getKeyByIndex(int keyIndex) {
    return String.valueOf(MIN_KEY_ID + keyIndex);
  }

  private static String genRandomKeyId() {
    // Example: 184398429
    return getKeyByIndex(ThreadLocalRandom.current().nextInt(NUM_KEY_IDS));
  }

  public static int keyIdToIndex(String keyId) {
    return (int) (Long.valueOf(keyId) - MIN_KEY_ID);
  }

  private static String genRandomKeyType() {
    if (ThreadLocalRandom.current().nextBoolean()) {
      return KEY_TYPE1;
    }
    return KEY_TYPE2;
  }

  public static int keyTypeToIndex(String keyType) {
    if (keyType.equals(KEY_TYPE1)) {
      return 0;
    }
    if (keyType.equals(KEY_TYPE2)) {
      return 1;
    }
    throw new IllegalArgumentException("Unknown key type: " + keyType);
  }

  private static String genRandomValue() {
    //          012345678901
    // Example: 64AM11N6RLZB
    int minSuffix = 100_000_000;
    return "AAA" + ThreadLocalRandom.current().nextLong(minSuffix, minSuffix + NUM_VALUES);
  }

  private static String getKeyAndTypeStr(String k, String keyType) {
    return k + DELIMITER + keyType;
  }

  private int keyAndTypeToLockIndex(String key, String keyType) {
    return keyIdToIndex(key) * NUM_KEY_TYPES + keyTypeToIndex(keyType);

  }
  private int keyAndTypeToLockIndex(String keyAndType) {
    String[] items = keyAndType.split(DELIMITER);
    return keyAndTypeToLockIndex(items[0], items[1]);
  }

  @Test
  public void testCqlIndexConsistency() throws Exception {
    try (Cluster cluster = getDefaultClusterBuilder().build();
         final Session session = cluster.connect()) {
      session.execute("CREATE KEYSPACE global_keys");
      String createTable =
          "CREATE TABLE global_keys.global_akpk_one_to_one (" +
          "key_id text, " +
          "key_type text, " +
          "key_value text, " +
          "modified_dtm timestamp, " +
          "version bigint, " +
          "PRIMARY KEY (key_id, key_type) " +
          ") WITH CLUSTERING ORDER BY (key_type ASC) AND default_time_to_live = 0 AND " +
          "transactions = { 'enabled' : true };";
      String createIndex =
          "CREATE UNIQUE INDEX global_akpk_index_one_to_one ON " +
          "global_keys.global_akpk_one_to_one (key_value, key_type);";
      session.execute(createTable);
      session.execute(createIndex);
      final int numWriterThreads = 5;
      final int numDeletionThreads = 5;
      final int numReaderThread = 10;
      ExecutorCompletionService ecs = new ExecutorCompletionService(
          Executors.newFixedThreadPool(
              numWriterThreads + numReaderThread + numDeletionThreads));
      final AtomicBoolean stop = new AtomicBoolean(false);

      final int INSERTS_PER_TXN = 3;

      List<Future<Void>> futures = new ArrayList<>();

      final AtomicInteger numInsertTxnAttempts = new AtomicInteger(0);
      final AtomicInteger numInsertTxnSuccesses = new AtomicInteger(0);

      final AtomicInteger numDeletionAttempts = new AtomicInteger(0);
      final AtomicInteger numDeletionSuccesses = new AtomicInteger(0);

      final AtomicInteger numReadsRowNotFound = new AtomicInteger(0);
      final AtomicInteger numReadsRowFound = new AtomicInteger(0);
      final AtomicInteger numReadsRowAndIndexFound = new AtomicInteger(0);

      final List<Lock> locks = new ArrayList<>();
      for (int i = 0; i < NUM_LOCKS; ++i) {
        locks.add(new ReentrantLock());
      }
      final List<AtomicInteger> nextVersions = new ArrayList<>();
      for (int i = 0; i < NUM_LOCKS; ++i) {
        nextVersions.add(new AtomicInteger());
      }
      final AtomicBoolean failed = new AtomicBoolean(false);

      // Insertion / overwrite threads.
      for (int wThreadIndex = 1; wThreadIndex <= numWriterThreads; ++wThreadIndex) {
        final String threadName = "Workload writer thread " + wThreadIndex;
        futures.add(ecs.submit(() -> {
          Thread.currentThread().setName(threadName);
          LOG.info("Thread starting: {}", threadName);
          SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
          while (!stop.get()) {
            try {
              StringBuilder sb = new StringBuilder();
              sb.append("BEGIN TRANSACTION ");

              Set<String> keyAndTypeSet = new TreeSet<>();
              Set<String> valueAndTypeSet = new TreeSet<>();

              List<Integer> lockIndexes = new ArrayList<Integer>();

              for (int i = 0; i < INSERTS_PER_TXN; ++i) {
                String k = genRandomKeyId();
                String v = genRandomValue();
                String keyType = genRandomKeyType();
                String keyAndType = getKeyAndTypeStr(k, keyType);
                int lockIndex = keyAndTypeToLockIndex(k, keyType);
                lockIndexes.add(lockIndex);
                String valueAndType = v + "_" + keyType;
                AtomicInteger versionAtomic = nextVersions.get(lockIndex);
                int expectedVersion = versionAtomic.getAndIncrement();
                int newVersion = expectedVersion + 1;

                if (!keyAndTypeSet.contains(keyAndType) &&
                    !valueAndTypeSet.contains(valueAndType)) {
                  keyAndTypeSet.add(keyAndType);
                  valueAndTypeSet.add(valueAndType);
                  sb.append(
                    String.format(
                      "INSERT INTO global_keys.global_akpk_one_to_one " +
                      "(key_id, key_type, key_value, modified_dtm, version) VALUES " +
                      "('%s', '%s', '%s', '%s', %d) " +
                      "IF version = null OR version <= %d ELSE ERROR; ",
                      k, keyType, v, simpleDateFormat.format(new Date()), newVersion,
                      expectedVersion
                    )
                  );
                }
              }
              sb.append("END TRANSACTION;");

              numInsertTxnAttempts.incrementAndGet();
              Collections.sort(lockIndexes);

              int lockedUntil = 0;
              try {
                for (int i = 0; i < lockIndexes.size(); ++i) {
                  locks.get(lockIndexes.get(i)).lock();
                  lockedUntil = i + 1;
                }

                session.execute(sb.toString());
                numInsertTxnSuccesses.incrementAndGet();
              } finally {
                for (int i = lockedUntil - 1; i >= 0; --i) {
                  locks.get(lockIndexes.get(i)).unlock();
                }
              }

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

      // Deletion threads.
      final PreparedStatement preparedDeleteStatement = session.prepare(
          "DELETE FROM global_keys.global_akpk_one_to_one " +
          "WHERE key_id = ? AND key_type = ?");

      for (int dThreadIndex = 1; dThreadIndex <= numDeletionThreads; ++dThreadIndex) {
        final String threadName = "Workload deletion thread " + dThreadIndex;
        futures.add(ecs.submit(() -> {
          Thread.currentThread().setName(threadName);
          while (!stop.get()) {
            try {
              StringBuilder sb = new StringBuilder();
              String k = genRandomKeyId();
              String keyType = genRandomKeyType();

              BoundStatement boundDeleteStatement = preparedDeleteStatement.bind(k, keyType);
              numDeletionAttempts.incrementAndGet();

              int lockIndex = keyAndTypeToLockIndex(k, keyType);
              Lock lock = locks.get(lockIndex);
              lock.lock();

              try {
                session.execute(boundDeleteStatement);
                numDeletionSuccesses.incrementAndGet();
              } finally {
                lock.unlock();
              }
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

      // Reader (verification) threads.
      for (int rThreadIndex = 1; rThreadIndex <= numReaderThread; ++rThreadIndex) {
        final String threadName = "Workload reader thread " + rThreadIndex;
        futures.add(ecs.submit(() -> {
          Thread.currentThread().setName(threadName);
          LOG.info("Thread starting: {}", threadName);
          while (!stop.get()) {
            try {
              String keyType = genRandomKeyType();
              String keyId = genRandomKeyId();
              int lockIndex = keyAndTypeToLockIndex(keyId, keyType);
              locks.get(lockIndex).lock();
              try {
                String selectPrimaryRow = String.format(
                    "SELECT key_value FROM global_keys.global_akpk_one_to_one " +
                        "WHERE key_id = '%s' AND key_type = '%s'",
                    keyId, keyType);
                ResultSet primaryRowResult = session.execute(selectPrimaryRow);
                List<Row> rows = primaryRowResult.all();
                if (rows.isEmpty()) {
                  numReadsRowNotFound.incrementAndGet();
                } else {
                  assertEquals(1, rows.size());
                  numReadsRowFound.incrementAndGet();
                  String keyValue = rows.get(0).getString(0);
                  String selectFromIndex = String.format(
                      "SELECT key_id, key_value, key_type " +
                          "FROM global_keys.global_akpk_one_to_one " +
                          "WHERE key_value = '%s' AND key_type = '%s'",
                      keyValue, keyType);
                  ResultSet indexResult = session.execute(selectFromIndex);
                  List<Row> indexRows = indexResult.all();
                  assertEquals(1, indexRows.size());
                  assertEquals(keyId, indexRows.get(0).getString(0));
                  numReadsRowAndIndexFound.incrementAndGet();
                }
              } finally {
                locks.get(lockIndex).unlock();;
              }
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

      LOG.info("Number of insert transaction attempts: " + numInsertTxnAttempts.get());
      LOG.info("Number of insert transaction successes: " + numInsertTxnSuccesses.get());
      LOG.info("Number of deletion attempts: " + numDeletionAttempts.get());
      LOG.info("Number of deletion successes: " + numDeletionSuccesses.get());
      LOG.info("Number of reads where the row is not found: " + numReadsRowNotFound.get());
      LOG.info("Number of reads where the row is found: " + numReadsRowFound.get());
      LOG.info("Number of reads where row and index entry are found: " +
          numReadsRowAndIndexFound.get());
      assertFalse(failed.get());
    }
  }

}
