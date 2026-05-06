/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */

package herddb.server.hammer;

import static herddb.core.TestUtils.commitTransaction;
import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.core.TestUtils;
import herddb.core.stats.TableManagerStats;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.DataAccessor;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Concurrent updates
 *
 * @author enrico.olivelli
 */
public abstract class DirectMultipleConcurrentUpdatesSuite {

    private static final int TABLESIZE = 2000;
    private static final int MULTIPLIER = 2;
    private static final int THREADPOLSIZE = 20;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Dumps all JVM threads on any test failure (including JUnit's own
     * TestTimedOutException) so the CI surefire report contains the lock-holder
     * context needed to triage the checkpoint/DML hang (issue #417). Inherited
     * by all subclasses; fires regardless of whether the hang was detected by
     * the JUnit method timeout or by the inner per-future timeout.
     */
    @Rule
    public TestWatcher dumpOnFailure = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            dumpAllThreads(description.getMethodName() + " failed: " + e.getClass().getSimpleName());
        }
    };

    protected void performTest(boolean useTransactions, long checkPointPeriod, boolean withIndexes, boolean uniqueIndexes) throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort(baseDir);

        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 10 * 1024);
        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, 1024 * 1024 / 4);
        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, 1024 * 1024);
        serverConfiguration.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, checkPointPeriod);
        serverConfiguration.set(ServerConfiguration.PROPERTY_DATADIR, folder.newFolder().getAbsolutePath());
        serverConfiguration.set(ServerConfiguration.PROPERTY_LOGDIR, folder.newFolder().getAbsolutePath());

        ConcurrentHashMap<String, Long> expectedValue = new ConcurrentHashMap<>();
        // issue #157: per-key history of writes (thread, op, tx, oldValue -> newValue) — dumped
        // when erroredKeys is non-empty so we can match stale reads against the actual sequence
        // of committed writes. CopyOnWriteArrayList keeps appends from different threads safe.
        ConcurrentHashMap<String, List<String>> writeHistory = new ConcurrentHashMap<>();
        try (Server server = new Server(serverConfiguration)) {
            server.start();
            server.waitForStandaloneBoot();
            DBManager manager = server.getManager();
            execute(manager, "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", Collections.emptyList());

            if (withIndexes) {
                if (uniqueIndexes) {
                    // use n1 + id in order to not have collisions and lock timeouts
                    execute(manager, "CREATE UNIQUE INDEX theindex ON mytable (n1, id)", Collections.emptyList());
                } else {
                    execute(manager, "CREATE INDEX theindex ON mytable (n1)", Collections.emptyList());
                }
            }

            long tx = TestUtils.beginTransaction(manager, TableSpace.DEFAULT);
            for (int i = 0; i < TABLESIZE; i++) {
                TestUtils.executeUpdate(manager,
                        "INSERT INTO mytable (id,n1,n2) values(?,?,?)",
                        Arrays.asList("test_" + i, 1, 2), new TransactionContext(tx));
                expectedValue.put("test_" + i, 1L);
            }
            TestUtils.commitTransaction(manager, TableSpace.DEFAULT, tx);
            ExecutorService threadPool = Executors.newFixedThreadPool(THREADPOLSIZE);
            try {
                List<Future> futures = new ArrayList<>();
                AtomicLong updates = new AtomicLong();
                AtomicLong skipped = new AtomicLong();
                AtomicLong gets = new AtomicLong();
                for (int i = 0; i < TABLESIZE * MULTIPLIER; i++) {
                    futures.add(threadPool.submit(new Runnable() {
                                                      @Override
                                                      public void run() {
                                                          try {
                                                              boolean update = ThreadLocalRandom.current().nextBoolean();
                                                              int k = ThreadLocalRandom.current().nextInt(TABLESIZE);
                                                              long value = ThreadLocalRandom.current().nextInt(TABLESIZE);
                                                              long transactionId;
                                                              String key = "test_" + k;
                                                              Long actual = expectedValue.remove(key);
                                                              if (actual == null) {
                                                                  // another thread working on this entry, skip
                                                                  skipped.incrementAndGet();
                                                                  return;
                                                              }
                                                              if (update) {
                                                                  updates.incrementAndGet();
                                                                  DMLStatementExecutionResult updateResult =
                                                                          TestUtils.executeUpdate(manager,
                                                                                  "UPDATE mytable set n1=? WHERE id=?",
                                                                                  Arrays.asList(value, "test_" + k),
                                                                                  new TransactionContext(useTransactions ? TransactionContext.AUTOTRANSACTION_ID : TransactionContext.NOTRANSACTION_ID));
                                                                  long count = updateResult.getUpdateCount();
                                                                  transactionId = updateResult.transactionId;
                                                                  if (count <= 0) {
                                                                      throw new RuntimeException("not updated ?");
                                                                  }
                                                              } else {
                                                                  gets.incrementAndGet();
                                                                  DataScanner res = TestUtils.scanKeepReadLocks(manager,
                                                                          "SELECT * FROM mytable where id=?", Arrays.asList("test_" + k),
                                                                          new TransactionContext(useTransactions ? TransactionContext.AUTOTRANSACTION_ID : TransactionContext.NOTRANSACTION_ID));
                                                                  if (!res.hasNext()) {
                                                                      throw new RuntimeException("not found?");
                                                                  }
                                                                  res.close();
                                                                  transactionId = res.getTransactionId();
                                                                  // value did not change actually
                                                                  value = actual;
                                                              }
                                                              if (useTransactions) {
                                                                  if (transactionId <= 0) {
                                                                      throw new RuntimeException("no transaction ?");
                                                                  }
                                                                  commitTransaction(manager, TableSpace.DEFAULT, transactionId);
                                                              }
                                                              expectedValue.put(key, value);
                                                              writeHistory
                                                                      .computeIfAbsent(key, unused -> new CopyOnWriteArrayList<>())
                                                                      .add(String.format("t=%d tid=%d %s %d->%d",
                                                                              Thread.currentThread().getId(),
                                                                              transactionId,
                                                                              update ? "UPD" : "GET",
                                                                              actual, value));

                                                          } catch (Exception err) {
                                                              throw new RuntimeException(err);
                                                          }
                                                      }
                                                  }
                    ));
                }
                for (Future f : futures) {
                    // 90 s is strictly less than both outer @Test timeout values
                    // (180 s for no-checkpoint variants, 240 s for checkpoint variants)
                    // so a stuck future fires TimeoutException *before* JUnit interrupts
                    // the test thread. The dumpOnFailure TestWatcher rule is a second
                    // safety net for hangs that occur outside this loop (issue #417).
                    try {
                        f.get(90, TimeUnit.SECONDS);
                    } catch (TimeoutException e) {
                        dumpAllThreads("DirectMultipleConcurrentUpdatesSuite: future timed out after 90 s");
                        throw e;
                    }
                }

                System.out.println("stats::updates:" + updates);
                System.out.println("stats::get:" + gets);
                assertTrue(updates.get() > 0);
                assertTrue(gets.get() > 0);

                List<String> erroredKeys = new ArrayList<>();
                for (Map.Entry<String, Long> entry : expectedValue.entrySet()) {
                    List<DataAccessor> records;
                    DataAccessor data;
                    try (DataScanner res = scan(manager, "SELECT n1 FROM mytable where id=?", Arrays.asList(entry.getKey()))) {
                        records = res.consume();
                        data = records.get(0);
                    }
                    assertEquals(1, records.size());

                    if (!entry.getValue().equals(data.get("n1"))) {
                        System.out.println("expected value " + data.get("n1") + ", but got " + Long.valueOf(entry.getValue()) + " for key " + entry.getKey());
                        erroredKeys.add(entry.getKey());
                    }
                }
                if (!erroredKeys.isEmpty()) {
                    dumpHistory("PRE-RESTART", erroredKeys, expectedValue, writeHistory);
                }
                assertTrue(erroredKeys.isEmpty());

                TableManagerStats stats = server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableManager("mytable").getStats();
                System.out.println("stats::tablesize:" + stats.getTablesize());
                System.out.println("stats::dirty records:" + stats.getDirtyrecords());
                System.out.println("stats::unload count:" + stats.getUnloadedPagesCount());
                System.out.println("stats::load count:" + stats.getLoadedPagesCount());
                System.out.println("stats::buffers used mem:" + stats.getBuffersUsedMemory());

//                assertTrue(stats.getUnloadedPagesCount() > 0);
                assertEquals(TABLESIZE, stats.getTablesize());
            } finally {
                threadPool.shutdown();
                threadPool.awaitTermination(1, TimeUnit.MINUTES);
            }
        }

        // restart and recovery
        try (Server server = new Server(serverConfiguration)) {
            server.start();
            server.waitForTableSpaceBoot(TableSpace.DEFAULT, 300000, true);
            DBManager manager = server.getManager();
            List<String> erroredKeys = new ArrayList<>();
            for (Map.Entry<String, Long> entry : expectedValue.entrySet()) {
                List<DataAccessor> records;
                DataAccessor data;
                try (DataScanner res = scan(manager, "SELECT n1 FROM mytable where id=?", Arrays.asList(entry.getKey()))) {
                    records = res.consume();
                    data = records.get(0);
                }
                assertEquals(1, records.size());

                if (!entry.getValue().equals(data.get("n1"))) {
                    System.out.println("expected value " + data.get("n1") + ", but got " + Long.valueOf(entry.getValue()) + " for key " + entry.getKey());
                    erroredKeys.add(entry.getKey());
                }
            }
            if (!erroredKeys.isEmpty()) {
                dumpHistory("POST-RESTART", erroredKeys, expectedValue, writeHistory);
            }
            assertTrue(erroredKeys.isEmpty());
        }
    }

    /**
     * Diagnostics for issue #157. Print the full writer-thread history for every key that the
     * final scan returned a stale value for. "expected" is the last value any thread put into
     * {@code expectedValue}; the dump lets us correlate it with the (threadId, txId, oldValue,
     * newValue) sequence of committed writes on the same key.
     */
    private static void dumpHistory(
            String stage,
            List<String> erroredKeys,
            Map<String, Long> expectedValue,
            Map<String, List<String>> writeHistory) {
        System.out.println("=== issue-157 diagnostics [" + stage + "] — " + erroredKeys.size() + " errored keys ===");
        for (String key : erroredKeys) {
            Long expected = expectedValue.get(key);
            List<String> history = writeHistory.getOrDefault(key, Collections.emptyList());
            System.out.println("  key=" + key + " expected=" + expected + " history(" + history.size() + "):");
            for (String h : history) {
                System.out.println("    " + h);
            }
        }
        System.out.println("=== end issue-157 diagnostics [" + stage + "] ===");
    }

    /**
     * Dumps all JVM threads to stderr with full stack traces (not truncated),
     * locked monitors, and locked synchronizers, plus JVM deadlock detection.
     * Called both from the per-future TimeoutException catch and from the
     * dumpOnFailure TestWatcher rule so every hang produces a useful trace
     * in the CI surefire report (issue #417).
     *
     * <p>We walk {@code ti.getStackTrace()} directly rather than calling
     * {@code ThreadInfo.toString()} because the latter truncates to
     * {@code MAX_FRAMES = 8} by JDK spec, which hides the lock-acquisition
     * frames that identify the checkpoint/DML contention site.
     */
    static void dumpAllThreads(String context) {
        System.err.println("=== Thread dump [" + context + "] ===");
        ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
        long[] deadlocked = tmx.findDeadlockedThreads();
        if (deadlocked != null) {
            System.err.println("DEADLOCKED THREAD IDs: " + Arrays.toString(deadlocked));
        }
        for (ThreadInfo ti : tmx.dumpAllThreads(true, true)) {
            System.err.println("\"" + ti.getThreadName() + "\""
                    + (ti.isDaemon() ? " daemon" : "")
                    + " prio=" + ti.getPriority()
                    + " Id=" + ti.getThreadId()
                    + " " + ti.getThreadState());
            if (ti.getLockName() != null) {
                System.err.println("\t- waiting on " + ti.getLockName()
                        + (ti.getLockOwnerName() != null
                           ? " owned by \"" + ti.getLockOwnerName() + "\" Id=" + ti.getLockOwnerId()
                           : ""));
            }
            for (StackTraceElement ste : ti.getStackTrace()) {
                System.err.println("\tat " + ste);
            }
            for (MonitorInfo mi : ti.getLockedMonitors()) {
                System.err.println("\t- locked <" + mi + "> at " + mi.getLockedStackFrame());
            }
            for (LockInfo li : ti.getLockedSynchronizers()) {
                System.err.println("\t- locked <" + li + ">");
            }
            System.err.println();
        }
        System.err.println("=== End thread dump [" + context + "] ===");
    }
}
