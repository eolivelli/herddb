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

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.client.DMLResult;
import herddb.client.GetResult;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.core.stats.TableManagerStats;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import herddb.utils.RawString;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Concurrent updates
 *
 * @author enrico.olivelli
 */
public class MultipleConcurrentUpdatesTest {

    private static final int TABLESIZE = 2000;
    private static final int MULTIPLIER = 2;
    private static final int THREADPOLSIZE = 100;

    private static final RawString N1 = RawString.of("n1");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Dumps all JVM threads on any test failure (including JUnit's own
     * TestTimedOutException) so the CI surefire report contains the lock-holder
     * context needed to triage the checkpoint/DML hang (issue #417). This rule
     * fires regardless of whether the hang was detected by the JUnit method
     * timeout or by the inner per-future timeout in performTest.
     */
    @Rule
    public TestWatcher dumpOnFailure = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            dumpAllThreads(description.getMethodName() + " failed: " + e.getClass().getSimpleName());
        }
    };

    // No-checkpoint variants: 180 s JUnit ceiling, 90 s inner per-future limit.
    // The inner limit is strictly smaller than the outer so a stuck future fires
    // TimeoutException before JUnit interrupts the test thread (issue #417).
    @Test(timeout = 180_000)
    public void test() throws Exception {
        performTest(false, 0, false, 90);
    }

    @Test(timeout = 180_000)
    public void testWithTransactions() throws Exception {
        performTest(true, 0, false, 90);
    }

    // Checkpoint variants: 240 s JUnit ceiling, 200 s inner per-future limit.
    // The 200 s inner limit is below the 240 s outer so a stuck checkpoint-phase
    // future fires TimeoutException first, triggering the thread dump (issue #417).
    @Test(timeout = 240_000)
    public void testWithCheckpoints() throws Exception {
        performTest(false, 2000, false, 200);
    }

    @Test(timeout = 240_000)
    public void testWithTransactionsWithCheckpoints() throws Exception {
        performTest(true, 2000, false, 200);
    }

    @Test(timeout = 180_000)
    public void testWithIndexes() throws Exception {
        performTest(false, 0, true, 90);
    }

    @Test(timeout = 180_000)
    public void testWithTransactionsAndIndexes() throws Exception {
        performTest(true, 0, true, 90);
    }

    @Test(timeout = 240_000)
    public void testWithCheckpointsAndIndexes() throws Exception {
        performTest(false, 2000, true, 200);
    }

    @Test(timeout = 240_000)
    public void testWithTransactionsWithCheckpointsAndIndexes() throws Exception {
        performTest(true, 2000, true, 200);
    }

    private void performTest(boolean useTransactions, long checkPointPeriod, boolean withIndexes,
            int futureTimeoutSeconds) throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort(baseDir);

        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 10 * 1024);
        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, 1024 * 1024 / 4);
        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, 1024 * 1024);
        serverConfiguration.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, checkPointPeriod);
        serverConfiguration.set(ServerConfiguration.PROPERTY_DATADIR, folder.newFolder().getAbsolutePath());
        serverConfiguration.set(ServerConfiguration.PROPERTY_LOGDIR, folder.newFolder().getAbsolutePath());

        ConcurrentHashMap<String, Long> expectedValue = new ConcurrentHashMap<>();

        try (Server server = new Server(serverConfiguration)) {
            server.start();
            server.waitForStandaloneBoot();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            /*
             * Set the client request timeout to 600 s — twice the server-side
             * CHECKPOINT_LOCK_READ_TIMEOUT (300 s). Phase C of a checkpoint holds the
             * checkpoint write lock while doing disk I/O (keyToPage.checkpoint +
             * tableCheckpoint). On a loaded CI runner this can approach 300 s, causing
             * the default 300 s client timeout to fire before the server's own tryLock
             * expires, producing a spurious TimeoutException. Setting the client timeout
             * to CHECKPOINT_LOCK_WRITE_TIMEOUT (600 s) ensures the client never gives up
             * before the server's read-lock side does. See issue #267.
             */
            clientConfiguration.set(ClientConfiguration.PROPERTY_TIMEOUT, 600_000);
            try (HDBClient client = new HDBClient(clientConfiguration);
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                if (withIndexes) {
                    long resultCreateIndex = connection.executeUpdate(TableSpace.DEFAULT,
                            "CREATE INDEX theindex ON mytable (n1 long)", 0, false, true, Collections.emptyList()).updateCount;
                    Assert.assertEquals(1, resultCreateIndex);

                }

                long tx = connection.beginTransaction(TableSpace.DEFAULT);
                for (int i = 0; i < TABLESIZE; i++) {
                    connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id,n1,n2) values(?,?,?)", tx, false, true,
                            Arrays.asList("test_" + i, 1, 2));

                    expectedValue.put("test_" + i, 1L);

                }
                connection.commitTransaction(TableSpace.DEFAULT, tx);
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

                                                                      DMLResult updateResult = connection.executeUpdate(TableSpace.DEFAULT,
                                                                              "UPDATE mytable set n1=? WHERE id=?", useTransactions ? TransactionContext.AUTOTRANSACTION_ID : TransactionContext.NOTRANSACTION_ID, false, true,
                                                                              Arrays.asList(value, "test_" + k));

                                                                      long count = updateResult.updateCount;
                                                                      transactionId = updateResult.transactionId;
                                                                      if (count <= 0) {
                                                                          throw new RuntimeException("not updated ?");
                                                                      }
                                                                  } else {
                                                                      gets.incrementAndGet();
                                                                      GetResult res = connection.executeGet(TableSpace.DEFAULT, "SELECT * FROM mytable where id=?",
                                                                              useTransactions ? TransactionContext.AUTOTRANSACTION_ID : TransactionContext.NOTRANSACTION_ID, true, Arrays.asList("test_" + k));

                                                                      if (res.data == null) {
                                                                          throw new RuntimeException("not found?");
                                                                      }
                                                                      if (!res.data.get(N1).equals(actual)) {
                                                                          throw new RuntimeException("unspected value " + res.data + ", expected: " + actual);
                                                                      }
                                                                      transactionId = res.transactionId;
                                                                      // value did not change actually
                                                                      value = actual;
                                                                  }
                                                                  if (useTransactions) {
                                                                      if (transactionId <= 0) {
                                                                          throw new RuntimeException("no transaction ?");
                                                                      }
                                                                      connection.commitTransaction(TableSpace.DEFAULT, transactionId);
                                                                  }
                                                                  expectedValue.put(key, value);
                                                              } catch (Exception err) {
                                                                  throw new RuntimeException(err);
                                                              }
                                                          }
                                                      }
                        ));
                    }
                    for (Future f : futures) {
                        // futureTimeoutSeconds is always strictly less than the enclosing
                        // @Test(timeout=…) value, so a stuck future fires TimeoutException
                        // *before* JUnit interrupts the test thread. The TimeoutException
                        // catch below emits the thread dump; the dumpOnFailure TestWatcher
                        // rule is a second safety net that fires even if the hang happens
                        // outside this loop (e.g. waitForTableSpaceBoot). See issue #417.
                        try {
                            f.get(futureTimeoutSeconds, TimeUnit.SECONDS);
                        } catch (TimeoutException e) {
                            dumpAllThreads("MultipleConcurrentUpdatesTest: future timed out after "
                                    + futureTimeoutSeconds + " s");
                            throw e;
                        }
                    }

                    System.out.println("stats::updates:" + updates);
                    System.out.println("stats::get:" + gets);
                    System.out.println("stats::skipped:" + skipped);
                    assertTrue(updates.get() > 0);
                    assertTrue(gets.get() > 0);

                    List<String> erroredKeys = new ArrayList<>();
                    for (Map.Entry<String, Long> entry : expectedValue.entrySet()) {
                        GetResult res = connection.executeGet(TableSpace.DEFAULT, "SELECT n1 FROM mytable where id=?",
                                TransactionContext.NOTRANSACTION_ID, true, Arrays.asList(entry.getKey()));
                        assertNotNull(res.data);
                        if (!entry.getValue().equals(res.data.get(N1))) {
                            if (!entry.getValue().equals(res.data.get(N1))) {
                                System.out.println("expected value " + res.data.get(N1) + ", but got " + Long.valueOf(entry.getValue()) + " for key " + entry.getKey());
                                erroredKeys.add(entry.getKey());
                            }
                        }
                    }
                    assertTrue(erroredKeys.isEmpty());

                    TableManagerStats stats = server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableManager("mytable").getStats();
                    System.out.println("stats::tablesize:" + stats.getTablesize());
                    System.out.println("stats::dirty records:" + stats.getDirtyrecords());
                    System.out.println("stats::unload count:" + stats.getUnloadedPagesCount());
                    System.out.println("stats::load count:" + stats.getLoadedPagesCount());
                    System.out.println("stats::buffers used mem:" + stats.getBuffersUsedMemory());

                    assertTrue(stats.getUnloadedPagesCount() > 0);
                    assertEquals(TABLESIZE, stats.getTablesize());
                } finally {
                    threadPool.shutdown();
                    threadPool.awaitTermination(1, TimeUnit.MINUTES);
                }
            }
        }

        // restart and recovery
        try (Server server = new Server(serverConfiguration)) {
            server.start();
            server.waitForTableSpaceBoot(TableSpace.DEFAULT, 300000, true);
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            try (HDBClient client = new HDBClient(clientConfiguration);
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                for (Map.Entry<String, Long> entry : expectedValue.entrySet()) {
                    GetResult res = connection.executeGet(TableSpace.DEFAULT, "SELECT n1 FROM mytable where id=?",
                            TransactionContext.NOTRANSACTION_ID, true, Arrays.asList(entry.getKey()));
                    assertNotNull(res.data);
                    assertEquals(entry.getValue(), res.data.get(N1));
                }
            }
        }
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
    private static void dumpAllThreads(String context) {
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
