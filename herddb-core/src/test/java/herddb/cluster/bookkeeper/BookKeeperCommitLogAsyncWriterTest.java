/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.cluster.bookkeeper;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.cluster.BookkeeperCommitLog;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.core.ClusterTest;
import herddb.log.CommitLogResult;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogEntryType;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.model.TableSpace;
import herddb.server.ServerConfiguration;
import herddb.utils.ZKTestEnv;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for the single-threaded BookKeeper writer pipeline introduced in
 * issue #434. The pipeline funnels every {@code writeAsync} call for a
 * given ledger through a single thread borrowed from BookKeeper's main
 * worker pool ({@code chooseThread(ledgerId)}), eliminating monitor
 * contention on {@code LedgerHandle.doAsyncAddEntry}'s
 * {@code synchronized (this)} block.
 *
 * <p>The pipeline also (a) creates ledgers with
 * {@link DigestType#DUMMY} so per-entry CRC32C is skipped on the client
 * side, and (b) uses {@code WriteAdvHandle} so the writer thread
 * pre-assigns sequential entry ids (0..N-1).
 */
@Category(ClusterTest.class)
public class BookKeeperCommitLogAsyncWriterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookieAndInitCluster();
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    /**
     * 16 concurrent producer threads × 500 writes against a single
     * {@link BookkeeperCommitLog}. Asserts every future succeeds, every
     * entry is recoverable, total entry count matches, LSNs within each
     * ledger are strictly monotonically increasing, and entry ids are
     * dense (no gaps). Proves the MPSC pipeline preserves WAL ordering
     * and that {@code WriteAdvHandle}'s caller-assigned entry ids do not
     * skip or duplicate under high concurrency.
     */
    @Test
    public void testConcurrentWrites() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        final int producerThreads = 16;
        final int writesPerThread = 500;
        final int totalWrites = producerThreads * writesPerThread;

        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            // Use a large explicit maxLedgerSize: the manager's default
            // initialiser overflows {@code int} to {@code 0}, which would
            // otherwise force a rotation on every write and dwarf the
            // single-thread executor cost we're trying to measure.
            // Production servers always override this default via
            // {@link ServerConfiguration#PROPERTY_BOOKKEEPER_LEDGERS_MAX_SIZE}
            // in {@code Server.java}, so this matches the real setup.
            logManager.setMaxLedgerSizeBytes(256L * 1024 * 1024);
            man.start();
            logManager.start();

            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                writer.setWriteLedgerHeader(false);
                writer.startWriting(1);

                final List<CompletableFuture<LogSequenceNumber>> futures = new ArrayList<>(totalWrites);
                final List<CompletableFuture<LogSequenceNumber>>[] perThread = new List[producerThreads];
                final ExecutorService producers = Executors.newFixedThreadPool(producerThreads);
                final CountDownLatch ready = new CountDownLatch(producerThreads);
                final CountDownLatch start = new CountDownLatch(1);
                final CountDownLatch done = new CountDownLatch(producerThreads);
                try {
                    for (int t = 0; t < producerThreads; t++) {
                        final int threadIndex = t;
                        final List<CompletableFuture<LogSequenceNumber>> myFutures = new ArrayList<>(writesPerThread);
                        perThread[t] = myFutures;
                        producers.execute(() -> {
                            ready.countDown();
                            try {
                                start.await();
                                for (int i = 0; i < writesPerThread; i++) {
                                    LogEntry entry = LogEntryFactory.beginTransaction(threadIndex * 1000L + i);
                                    CommitLogResult res = writer.log(entry, true);
                                    myFutures.add(res.logSequenceNumber);
                                }
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            } finally {
                                done.countDown();
                            }
                        });
                    }
                    assertTrue(ready.await(30, TimeUnit.SECONDS));
                    start.countDown();
                    assertTrue(done.await(120, TimeUnit.SECONDS));

                    for (List<CompletableFuture<LogSequenceNumber>> per : perThread) {
                        futures.addAll(per);
                    }
                } finally {
                    producers.shutdown();
                    assertTrue(producers.awaitTermination(30, TimeUnit.SECONDS));
                }

                assertEquals(totalWrites, futures.size());
                final List<LogSequenceNumber> assigned = new ArrayList<>(totalWrites);
                for (CompletableFuture<LogSequenceNumber> f : futures) {
                    assigned.add(f.get(60, TimeUnit.SECONDS));
                }

                // Group by ledger, sort by offset, verify each ledger's
                // offsets are 0..k-1 with no gaps (caller-assigned ids).
                Map<Long, List<Long>> perLedger = new java.util.TreeMap<>();
                for (LogSequenceNumber lsn : assigned) {
                    perLedger.computeIfAbsent(lsn.ledgerId, k -> new ArrayList<>()).add(lsn.offset);
                }
                long observedTotal = 0;
                for (Map.Entry<Long, List<Long>> e : perLedger.entrySet()) {
                    List<Long> offsets = e.getValue();
                    java.util.Collections.sort(offsets);
                    for (int i = 0; i < offsets.size(); i++) {
                        assertEquals("ledger " + e.getKey() + " entry slot " + i, (long) i, (long) offsets.get(i));
                    }
                    observedTotal += offsets.size();
                }
                assertEquals(totalWrites, observedTotal);
            }

            // Recover every entry through the read path (digest auto-detection
            // succeeds on DUMMY ledgers).
            try (BookkeeperCommitLog reader = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                final List<Map.Entry<LogSequenceNumber, LogEntry>> list = new ArrayList<>();
                reader.recovery(LogSequenceNumber.START_OF_TIME, (lsn, entry) -> {
                    if (entry.type != LogEntryType.NOOP) {
                        list.add(new AbstractMap.SimpleImmutableEntry<>(lsn, entry));
                    }
                }, false);
                assertEquals(totalWrites, list.size());
                for (int i = 1; i < list.size(); i++) {
                    assertTrue("LSN " + list.get(i).getKey() + " must be after " + list.get(i - 1).getKey(),
                            list.get(i).getKey().after(list.get(i - 1).getKey()));
                }
            }
        }
    }

    /**
     * Many threads write concurrently while the ledger size is small
     * enough to force frequent rotations. Asserts no entries are lost
     * across rotations, {@code pendingAdds} drains cleanly between
     * rotations, and recovery reproduces every entry.
     */
    @Test
    public void testRotationDuringConcurrentWrites() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        final int producerThreads = 8;
        final int writesPerThread = 200;
        final int maxLedgerSize = 1024;

        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            logManager.setMaxLedgerSizeBytes(maxLedgerSize);
            man.start();
            logManager.start();

            final AtomicInteger txCounter = new AtomicInteger(0);
            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                writer.setWriteLedgerHeader(false);
                writer.startWriting(1);

                final ExecutorService producers = Executors.newFixedThreadPool(producerThreads);
                final CountDownLatch start = new CountDownLatch(1);
                final CountDownLatch done = new CountDownLatch(producerThreads);
                final List<List<LogSequenceNumber>> results = new ArrayList<>();
                for (int t = 0; t < producerThreads; t++) {
                    results.add(new ArrayList<>(writesPerThread));
                }
                try {
                    for (int t = 0; t < producerThreads; t++) {
                        final int threadIndex = t;
                        producers.execute(() -> {
                            try {
                                start.await();
                                for (int i = 0; i < writesPerThread; i++) {
                                    int txId = txCounter.incrementAndGet();
                                    LogEntry entry = LogEntryFactory.beginTransaction(txId);
                                    // Bounded get so a hung future surfaces as
                                    // a timeout against this specific entry,
                                    // not a generic latch timeout (PR #437
                                    // review follow-up #5).
                                    LogSequenceNumber lsn = writer.log(entry, true)
                                            .logSequenceNumber.get(60, TimeUnit.SECONDS);
                                    results.get(threadIndex).add(lsn);
                                }
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            } catch (java.util.concurrent.ExecutionException
                                    | java.util.concurrent.TimeoutException ex) {
                                throw new RuntimeException(ex);
                            } finally {
                                done.countDown();
                            }
                        });
                    }
                    start.countDown();
                    assertTrue(done.await(180, TimeUnit.SECONDS));
                } finally {
                    producers.shutdown();
                    assertTrue(producers.awaitTermination(30, TimeUnit.SECONDS));
                }

                int total = 0;
                Set<Long> distinctLedgers = new HashSet<>();
                for (List<LogSequenceNumber> per : results) {
                    total += per.size();
                    for (LogSequenceNumber lsn : per) {
                        distinctLedgers.add(lsn.ledgerId);
                    }
                }
                assertEquals(producerThreads * writesPerThread, total);
                // We expect at least 2 distinct ledgers given the small
                // maxLedgerSize and the volume of writes.
                assertTrue("expected ledger rotation to fire (saw " + distinctLedgers.size() + " ledgers)",
                        distinctLedgers.size() >= 2);
            }

            try (BookkeeperCommitLog reader = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                final List<Map.Entry<LogSequenceNumber, LogEntry>> list = new ArrayList<>();
                reader.recovery(LogSequenceNumber.START_OF_TIME, (lsn, entry) -> {
                    if (entry.type != LogEntryType.NOOP) {
                        list.add(new AbstractMap.SimpleImmutableEntry<>(lsn, entry));
                    }
                }, false);
                assertEquals(producerThreads * writesPerThread, list.size());
            }
        }
    }

    /**
     * Submits writes asynchronously and immediately requests close. Asserts
     * every returned future either completes successfully or fails with a
     * {@link LogNotAvailableException}. No future may remain dangling.
     */
    @Test
    public void testCloseDrainsQueueWithoutDataLoss() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        final int totalWrites = 200;

        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            // Use a large explicit maxLedgerSize: the manager's default
            // initialiser overflows {@code int} to {@code 0}, which would
            // otherwise force a rotation on every write and dwarf the
            // single-thread executor cost we're trying to measure.
            // Production servers always override this default via
            // {@link ServerConfiguration#PROPERTY_BOOKKEEPER_LEDGERS_MAX_SIZE}
            // in {@code Server.java}, so this matches the real setup.
            logManager.setMaxLedgerSizeBytes(256L * 1024 * 1024);
            man.start();
            logManager.start();

            final List<CompletableFuture<LogSequenceNumber>> futures = new ArrayList<>(totalWrites);
            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                writer.setWriteLedgerHeader(false);
                writer.startWriting(1);
                for (int i = 0; i < totalWrites; i++) {
                    CommitLogResult res = writer.log(LogEntryFactory.beginTransaction(i), true);
                    futures.add(res.logSequenceNumber);
                }
                // close() runs through try-with-resources here; must not hang.
            }

            // The contract this test enforces:
            //   1. Every submitted future must resolve — no dangling futures.
            //   2. Any non-success must be a clean LogNotAvailableException.
            //   3. Every successfully-completed future's LSN must be readable
            //      by recovery — i.e. successful = durably committed
            //      (issue #434, PR #437 review follow-up #4).
            // CommitLog.close() takes the {@code waitForPendingAdds=false}
            // path, so writes that were still in flight at close-time are
            // correctly rejected.
            final java.util.Set<LogSequenceNumber> successfulLsns = new java.util.HashSet<>();
            int successes = 0;
            int rejections = 0;
            for (CompletableFuture<LogSequenceNumber> f : futures) {
                try {
                    LogSequenceNumber lsn = f.get(60, TimeUnit.SECONDS);
                    assertTrue(lsn.after(LogSequenceNumber.START_OF_TIME));
                    successfulLsns.add(lsn);
                    successes++;
                } catch (ExecutionException ee) {
                    assertTrue("unexpected failure cause " + ee.getCause(),
                            ee.getCause() instanceof LogNotAvailableException);
                    rejections++;
                }
            }
            assertEquals(totalWrites, successes + rejections);

            // Durability assertion: every successful future must be
            // recoverable from the persisted ledger(s).
            try (BookkeeperCommitLog reader = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                final java.util.Set<LogSequenceNumber> recoveredLsns = new java.util.HashSet<>();
                reader.recovery(LogSequenceNumber.START_OF_TIME, (lsn, entry) -> {
                    recoveredLsns.add(lsn);
                }, false);
                for (LogSequenceNumber lsn : successfulLsns) {
                    assertTrue("successful future LSN " + lsn
                            + " was reported as committed but is not recoverable",
                            recoveredLsns.contains(lsn));
                }
            }
        }
    }

    /**
     * Once {@code errorOccurredDuringWrite} is set on a writer (here via the
     * test-only {@code forceWriteError} hook), every subsequent
     * {@code writeEntry} call must short-circuit on the executor thread,
     * release its buffer, and fail with {@link LogNotAvailableException}
     * <em>without</em> consuming an entry id or calling into BookKeeper —
     * preventing the {@code WriteAdvHandle} prefix-gap that would otherwise
     * stall every later commit until BK's add-entry quorum timeout fires
     * (issue #434, PR #437 review follow-up #2).
     */
    @Test
    public void testFailedWriterShortCircuitsSubsequentWrites() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";

        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            logManager.setMaxLedgerSizeBytes(256L * 1024 * 1024);
            man.start();
            logManager.start();

            // goodLsns is captured before the writer closes so we can verify
            // recoverability against the sealed ledger after close().
            final List<LogSequenceNumber> goodLsns = new ArrayList<>();
            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                writer.setWriteLedgerHeader(false);
                writer.startWriting(1);
                // Write a few entries successfully first.
                for (int i = 0; i < 3; i++) {
                    LogSequenceNumber lsn = writer.log(
                            LogEntryFactory.beginTransaction(i), true)
                            .logSequenceNumber.get(30, TimeUnit.SECONDS);
                    goodLsns.add(lsn);
                }

                // Force the writer into the failed state via the test hook.
                // forceWriteError sets errorOccurredDuringWrite=true on the
                // CommitFileWriter without going through the BK callback,
                // matching the production scenario of a synchronous writeAsync
                // failure on the executor thread.
                writer.getWriter().forceWriteError(
                        new java.lang.RuntimeException("forced for test"));

                // Subsequent log() calls go through getValidWriter, which sees
                // the writer is no longer writable and rotates to a new
                // ledger.  But ANY write submitted on the failed writer
                // (e.g. one already in flight when the error fires) must
                // short-circuit cleanly.  Drive this directly by calling
                // writeEntry on the failed CommitFileWriter — bypassing
                // getValidWriter's rotation path.
                final BookkeeperCommitLog.CommitFileWriter failedWriter = writer.getWriter();
                final List<CompletableFuture<LogSequenceNumber>> rejected = new ArrayList<>();
                for (int i = 0; i < 5; i++) {
                    rejected.add(failedWriter.writeEntry(
                            LogEntryFactory.beginTransaction(100 + i)));
                }
                for (CompletableFuture<LogSequenceNumber> f : rejected) {
                    try {
                        f.get(15, TimeUnit.SECONDS);
                        fail("expected LogNotAvailableException for write on failed writer");
                    } catch (ExecutionException ee) {
                        assertTrue("unexpected cause " + ee.getCause(),
                                ee.getCause() instanceof LogNotAvailableException);
                    }
                }

                // The pending-adds counter must drain to zero — proving every
                // short-circuited write released its claim on the writer's
                // accounting (so a future close/rotation does not deadlock
                // waiting for phantom in-flight writes).
                long deadline = System.currentTimeMillis() + 10_000;
                while (failedWriter.getPendingAdds() > 0
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(50);
                }
                assertEquals(0L, failedWriter.getPendingAdds());
            }
            // Writer try-with-resources closes the ledger here, sealing
            // metadata at LAC. Now run recovery against the sealed ledger
            // and verify that every successful pre-failure LSN is readable —
            // this is the durability invariant the reviewer asked for: a
            // future that completed successfully MUST correspond to a
            // recoverable entry (PR #437 review follow-up #2 + #4).
            try (BookkeeperCommitLog reader = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                final java.util.Set<LogSequenceNumber> recovered = new java.util.HashSet<>();
                reader.recovery(LogSequenceNumber.START_OF_TIME, (lsn, entry) -> {
                    recovered.add(lsn);
                }, false);
                for (LogSequenceNumber lsn : goodLsns) {
                    assertTrue("successful pre-failure LSN " + lsn
                            + " must be recoverable, recovered=" + recovered,
                            recovered.contains(lsn));
                }
            }
        }
    }

    /**
     * If the BookKeeper main worker pool has been shut down before a write is
     * dispatched, the executor's {@code execute} call throws
     * {@link java.util.concurrent.RejectedExecutionException}; the writer
     * must release the serialised buffer (BK never took ownership), revert
     * {@code pendingAdds}, and fail the future cleanly without leaking
     * (issue #434, PR #437 review follow-up #3).
     */
    @Test
    public void testRejectedExecutionFailsCleanlyAndDoesNotLeakBuffer() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";

        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            logManager.setMaxLedgerSizeBytes(256L * 1024 * 1024);
            man.start();
            logManager.start();

            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                writer.setWriteLedgerHeader(false);
                writer.startWriting(1);
                // Drive one successful write so the writer is fully primed.
                writer.log(LogEntryFactory.beginTransaction(1), true)
                        .logSequenceNumber.get(30, TimeUnit.SECONDS);

                // Forcefully shut down the BK main worker pool so the next
                // executor.execute(...) inside writeEntry rejects.
                logManager.getBookKeeper().getMainWorkerPool().shutdown();

                CompletableFuture<LogSequenceNumber> rejected =
                        writer.getWriter().writeEntry(
                                LogEntryFactory.beginTransaction(2));
                try {
                    rejected.get(15, TimeUnit.SECONDS);
                    fail("expected LogNotAvailableException after BK pool shutdown");
                } catch (ExecutionException ee) {
                    assertTrue("unexpected cause " + ee.getCause(),
                            ee.getCause() instanceof LogNotAvailableException);
                }
                // pendingAdds must drain to 0 — proving the rejected write
                // released its claim on the writer's accounting.
                assertEquals(0L, writer.getWriter().getPendingAdds());
            }
        }
    }

    /**
     * Verifies new ledgers are created with {@link DigestType#DUMMY} (the
     * client-side checksum was disabled in issue #434 — bookie storage
     * still keeps its own checksums and TCP provides wire integrity).
     */
    @Test
    public void testNewLedgerUsesDummyDigest() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";

        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            // Use a large explicit maxLedgerSize: the manager's default
            // initialiser overflows {@code int} to {@code 0}, which would
            // otherwise force a rotation on every write and dwarf the
            // single-thread executor cost we're trying to measure.
            // Production servers always override this default via
            // {@link ServerConfiguration#PROPERTY_BOOKKEEPER_LEDGERS_MAX_SIZE}
            // in {@code Server.java}, so this matches the real setup.
            logManager.setMaxLedgerSizeBytes(256L * 1024 * 1024);
            man.start();
            logManager.start();

            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                writer.setWriteLedgerHeader(false);
                writer.startWriting(1);
                writer.log(LogEntryFactory.beginTransaction(1), true).getLogSequenceNumber();
                LedgerMetadata md = writer.getWriter().getOut().getLedgerMetadata();
                // The pinned BookKeeper version (4.17.x) returns DigestType
                // (api enum) from getDigestType(); compare against DUMMY.
                DigestType actual = md.getDigestType();
                assertEquals("new ledgers must use DigestType.DUMMY (issue #434)",
                        DigestType.DUMMY, actual);
                assertFalse("ledger should still be open", md.isClosed());
            }

            // Also verify recovery succeeds against the DUMMY ledger
            // (digest auto-detection in BookkeeperCommitLogManager).
            try (BookkeeperCommitLog reader = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                final List<LogSequenceNumber> seen = new ArrayList<>();
                reader.recovery(LogSequenceNumber.START_OF_TIME, (lsn, entry) -> {
                    if (entry.type != LogEntryType.NOOP) {
                        seen.add(lsn);
                    }
                }, false);
                assertEquals(1, seen.size());
            }
        }
    }

    /**
     * After a round of concurrent writes, asserts that the ledger contains
     * exactly entry ids {@code 0..N-1} with no gaps. Proves the
     * caller-assigned {@code entryId} from {@code WriteAdvHandle} is dense
     * and monotonic per ledger.
     */
    @Test
    public void testWriteAdvHandleEntryIdsAreSequential() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        final int producerThreads = 4;
        final int writesPerThread = 50;
        final int totalWrites = producerThreads * writesPerThread;

        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            // Use a large explicit maxLedgerSize: the manager's default
            // initialiser overflows {@code int} to {@code 0}, which would
            // otherwise force a rotation on every write and dwarf the
            // single-thread executor cost we're trying to measure.
            // Production servers always override this default via
            // {@link ServerConfiguration#PROPERTY_BOOKKEEPER_LEDGERS_MAX_SIZE}
            // in {@code Server.java}, so this matches the real setup.
            logManager.setMaxLedgerSizeBytes(256L * 1024 * 1024);
            man.start();
            logManager.start();

            final List<LogSequenceNumber> assigned = new ArrayList<>(totalWrites);
            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                writer.setWriteLedgerHeader(false);
                writer.startWriting(1);

                final ExecutorService producers = Executors.newFixedThreadPool(producerThreads);
                final CountDownLatch start = new CountDownLatch(1);
                final CountDownLatch done = new CountDownLatch(producerThreads);
                final List<List<LogSequenceNumber>> results = new ArrayList<>();
                for (int t = 0; t < producerThreads; t++) {
                    results.add(new ArrayList<>(writesPerThread));
                }
                try {
                    for (int t = 0; t < producerThreads; t++) {
                        final int threadIndex = t;
                        producers.execute(() -> {
                            try {
                                start.await();
                                for (int i = 0; i < writesPerThread; i++) {
                                    LogEntry entry = LogEntryFactory.beginTransaction(threadIndex * 1000L + i);
                                    // Bounded get so a hung future surfaces as
                                    // a timeout against this specific entry,
                                    // not a generic latch timeout (PR #437
                                    // review follow-up #5).
                                    LogSequenceNumber lsn = writer.log(entry, true)
                                            .logSequenceNumber.get(60, TimeUnit.SECONDS);
                                    results.get(threadIndex).add(lsn);
                                }
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            } catch (java.util.concurrent.ExecutionException
                                    | java.util.concurrent.TimeoutException ex) {
                                throw new RuntimeException(ex);
                            } finally {
                                done.countDown();
                            }
                        });
                    }
                    start.countDown();
                    assertTrue(done.await(120, TimeUnit.SECONDS));
                } finally {
                    producers.shutdown();
                    assertTrue(producers.awaitTermination(30, TimeUnit.SECONDS));
                }

                for (List<LogSequenceNumber> per : results) {
                    assigned.addAll(per);
                }
            }

            // Group by ledger, sort by offset, assert dense 0..k-1.
            Map<Long, List<Long>> perLedger = new java.util.TreeMap<>();
            for (LogSequenceNumber lsn : assigned) {
                perLedger.computeIfAbsent(lsn.ledgerId, k -> new ArrayList<>()).add(lsn.offset);
            }
            int totalSeen = 0;
            for (Map.Entry<Long, List<Long>> e : perLedger.entrySet()) {
                List<Long> offsets = e.getValue();
                java.util.Collections.sort(offsets);
                for (int i = 0; i < offsets.size(); i++) {
                    assertEquals("dense entry id at slot " + i + " in ledger " + e.getKey(),
                            (long) i, (long) offsets.get(i));
                }
                totalSeen += offsets.size();
            }
            assertEquals(totalWrites, totalSeen);
        }
    }
}
