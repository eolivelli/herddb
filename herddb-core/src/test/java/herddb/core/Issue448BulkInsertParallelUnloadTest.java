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

package herddb.core;

import static herddb.core.TestUtils.beginTransaction;
import static herddb.core.TestUtils.commitTransaction;
import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static herddb.core.TestUtils.scan;
import static herddb.model.TransactionContext.NO_TRANSACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import herddb.storage.DataStorageManagerException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #448: parallelize eviction-driven page unloads in
 * {@link TableManager#onTransactionCommit}.
 *
 * <p>Before the fix, every {@code applyInsert} that filled a page during a
 * transaction commit called {@code allocateLivePage} → {@code pageReplacementPolicy.add(victim)}
 * → {@code unload(victim)} synchronously on the commit thread. For a remote
 * (S3-backed) data storage manager, each unload of a mutable page issues a
 * {@code RemoteFileDataStorageManager.writeAsMultipart} round-trip, so
 * committing a transaction with thousands of new vector-bearing records ends
 * up serializing thousands of remote writes on a single thread.
 *
 * <p>The fix dispatches those unloads to the DBManager checkpoint-flush
 * executor (the same pool used by Phase B and the Phase-C drain), bounded by
 * the existing {@code checkpointFlushParallelism} semaphore. The tests in
 * this file cover:
 *
 * <ul>
 *   <li>{@link #bulkInsertCommitDispatchesParallelUnloads()} — the new
 *       batched path is exercised by a transaction full of inserts;</li>
 *   <li>{@link #bulkUpdateCommitDispatchesParallelUnloads()} — the same
 *       batched path is exercised by a transaction full of updates that
 *       force page rotation via {@code applyUpdate};</li>
 *   <li>{@link #mixedInsertUpdateDeleteCommitDispatchesParallelUnloads()} —
 *       a single transaction interleaving inserts, updates, and deletes
 *       still produces the right durable end state under eviction;</li>
 *   <li>{@link #commitFailureDrainsBatchAndReleasesApplySlot()} — when the
 *       async unload throws (e.g., remote storage write fails), the commit
 *       fails fast, the apply slot is released, and a follow-on commit
 *       still completes successfully;</li>
 *   <li>{@link #autocommitInsertsKeepLegacySynchronousPath()} — autocommit
 *       DML, which goes through {@code apply()} → {@code applyInsert(..., false)}
 *       (the no-batch overload), drives the legacy synchronous path under
 *       eviction without ever bumping the parallel-unloads counter;</li>
 *   <li>{@link #recoveryReplayUnderEvictionPressurePipelinesFlushes()} —
 *       recovery / log replay of autocommit entries routes eviction-driven
 *       unloads through the recovery-scoped batch (issue #559), even under
 *       eviction pressure, and reproduces all rows after restart.</li>
 * </ul>
 */
public class Issue448BulkInsertParallelUnloadTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Small enough that a few rows fill the page, so eviction kicks in well
     * within each test's row count.
     */
    private static final long PAGE_SIZE_BYTES = 2 * 1024L;

    /**
     * Cap data-page memory at ~16 pages so the page replacement policy is at
     * capacity quickly and every subsequent page rotation yields an
     * eviction victim — exactly the scenario the fix optimizes.
     */
    private static final long DATA_MEMORY_BYTES = 32 * 1024L;

    /**
     * One transaction with this many inserts. Generously over the 16-page
     * budget so the parallel-unloads counter has comfortable margin against
     * per-record-overhead variance on different runners (per pr-reviewer
     * follow-up #6).
     */
    private static final int RECORDS_PER_TXN = 800;

    /**
     * Padded value attached to every test row. ~200 B together with the
     * key plus serializer overhead means roughly half a dozen rows per 2 KiB
     * page, so {@link #RECORDS_PER_TXN} rows cause many page rotations.
     */
    private static final String PADDED_VALUE = repeat("x", 200);

    private static ServerConfiguration baseConfig() {
        ServerConfiguration cfg = newServerConfigurationWithAutoPort();
        cfg.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, PAGE_SIZE_BYTES);
        cfg.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, DATA_MEMORY_BYTES);
        // Keep the PK budget large so BLink-internal evictions do not
        // dominate; this test is about data-page evictions in the commit
        // path, not BLink behaviour.
        cfg.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, 4 * 1024 * 1024L);
        return cfg;
    }

    /**
     * One transaction with {@value #RECORDS_PER_TXN} inserts must dispatch at
     * least one eviction-driven unload to the parallel batch (and typically
     * many more), and the data must survive a restart.
     */
    @Test
    public void bulkInsertCommitDispatchesParallelUnloads() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmpDir").toPath();

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            createTableSpaceAndTable(manager);
            TableManager t1Manager = tableManager(manager);

            assertEquals("counter must start at zero",
                    0L, t1Manager.getParallelUnloadsCount());

            long tx = beginTransaction(manager, "tblspace1");
            for (int i = 0; i < RECORDS_PER_TXN; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(k1,v1) values(?,?)",
                        Arrays.asList(String.format("k_%05d", i), PADDED_VALUE),
                        new TransactionContext(tx));
            }
            commitTransaction(manager, "tblspace1", tx);

            long parallelUnloads = t1Manager.getParallelUnloadsCount();
            assertTrue("commit-time parallel unloads must have been dispatched: counter=" + parallelUnloads,
                    parallelUnloads > 0);
            assertRowCount(manager, RECORDS_PER_TXN);
        }

        // Restart and verify durability — no record may be lost just because
        // its hosting page was unloaded asynchronously during the commit.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10_000);
            assertRowCount(manager, RECORDS_PER_TXN);
        }
    }

    /**
     * A transaction that updates many existing rows (growing each row enough
     * to overflow its current page) drives {@code applyUpdate} →
     * {@code allocateLivePage} → eviction; the same parallel batch must be
     * used. Covers the {@code applyUpdate(..., unloadBatch)} overload at
     * {@link TableManager} (issue #448 review follow-up #2).
     */
    @Test
    public void bulkUpdateCommitDispatchesParallelUnloads() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmpDir").toPath();

        int rowCount = 500;
        String shortValue = repeat("a", 20);
        String longValue = repeat("z", 250);

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            createTableSpaceAndTable(manager);
            TableManager t1Manager = tableManager(manager);

            // Pre-populate with autocommit (legacy path) and checkpoint so
            // pages are immutable on disk before we start the test.
            for (int i = 0; i < rowCount; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(k1,v1) values(?,?)",
                        Arrays.asList(String.format("k_%05d", i), shortValue),
                        NO_TRANSACTION);
            }
            manager.checkpoint();

            long counterBeforeUpdates = t1Manager.getParallelUnloadsCount();

            // Now the transaction-mode commit: many UPDATEs that grow each
            // row. applyUpdate sees the writeTargetPage as null (immutable
            // after checkpoint), falls back to the insertion-page path, and
            // invokes allocateLivePage(insertionPageId, unloadBatch). The
            // batched path must drive at least one eviction.
            long tx = beginTransaction(manager, "tblspace1");
            for (int i = 0; i < rowCount; i++) {
                executeUpdate(manager,
                        "UPDATE tblspace1.t1 set v1=? where k1=?",
                        Arrays.asList(longValue, String.format("k_%05d", i)),
                        new TransactionContext(tx));
            }
            commitTransaction(manager, "tblspace1", tx);

            long counterAfterUpdates = t1Manager.getParallelUnloadsCount();
            long delta = counterAfterUpdates - counterBeforeUpdates;
            assertTrue("UPDATE-driven parallel unloads must have been dispatched: delta=" + delta
                            + " before=" + counterBeforeUpdates + " after=" + counterAfterUpdates,
                    delta > 0);

            assertRowCount(manager, rowCount);
            assertAllRowsHaveValue(manager, longValue, rowCount);
        }

        // Durability across restart.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10_000);
            assertRowCount(manager, rowCount);
            assertAllRowsHaveValue(manager, longValue, rowCount);
        }
    }

    /**
     * One transaction interleaving inserts, updates, and deletes under
     * eviction pressure must produce the correct end state and survive
     * restart (issue #448 review follow-up #3).
     */
    @Test
    public void mixedInsertUpdateDeleteCommitDispatchesParallelUnloads() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmpDir").toPath();

        int prepopulated = 200;
        int updates = 80;
        int deletes = 50;
        int inserts = 200;
        String preValue = repeat("p", 20);
        String updatedValue = repeat("u", 250);
        String insertedValue = repeat("n", 250);

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            createTableSpaceAndTable(manager);
            TableManager t1Manager = tableManager(manager);

            // Pre-populate (autocommit).
            for (int i = 0; i < prepopulated; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(k1,v1) values(?,?)",
                        Arrays.asList(String.format("k_%05d", i), preValue),
                        NO_TRANSACTION);
            }
            manager.checkpoint();
            long counterBefore = t1Manager.getParallelUnloadsCount();

            long tx = beginTransaction(manager, "tblspace1");
            // updates: keys 0..updates-1
            for (int i = 0; i < updates; i++) {
                executeUpdate(manager,
                        "UPDATE tblspace1.t1 set v1=? where k1=?",
                        Arrays.asList(updatedValue, String.format("k_%05d", i)),
                        new TransactionContext(tx));
            }
            // deletes: keys updates..updates+deletes-1
            for (int i = updates; i < updates + deletes; i++) {
                executeUpdate(manager,
                        "DELETE FROM tblspace1.t1 where k1=?",
                        Arrays.asList(String.format("k_%05d", i)),
                        new TransactionContext(tx));
            }
            // inserts: keys prepopulated..prepopulated+inserts-1
            for (int i = 0; i < inserts; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(k1,v1) values(?,?)",
                        Arrays.asList(String.format("k_%05d", prepopulated + i), insertedValue),
                        new TransactionContext(tx));
            }
            commitTransaction(manager, "tblspace1", tx);

            long delta = t1Manager.getParallelUnloadsCount() - counterBefore;
            assertTrue("mixed-DML commit should have dispatched parallel unloads: delta=" + delta,
                    delta > 0);

            int expectedRowCount = (prepopulated - deletes) + inserts;
            assertRowCount(manager, expectedRowCount);
        }

        // Durability across restart.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10_000);
            int expectedRowCount = (prepopulated - deletes) + inserts;
            assertRowCount(manager, expectedRowCount);
        }
    }

    /**
     * If a {@code writePage} call dispatched by the commit-time batch fails,
     * the commit must throw, but the apply slot must still be released and
     * a subsequent commit must complete successfully — proving the
     * {@code finally activeCommitApplies.decrementAndGet()} block in
     * {@link TableManager#onTransactionCommit} is not bypassed (issue #448
     * review follow-up #4).
     */
    @Test
    public void commitFailureDrainsBatchAndReleasesApplySlot() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmpDir").toPath();

        FailingFileDataStorageManager failingDsm = new FailingFileDataStorageManager(dataPath);

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                failingDsm,
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            createTableSpaceAndTable(manager);
            TableManager t1Manager = tableManager(manager);

            // Arm the failure: the very next writePage call from this
            // DBManager will throw. The first transaction-mode commit below
            // should drive several evictions, the first eviction's
            // writePage will fail, and the parallel batch's awaitAll() will
            // re-throw.
            failingDsm.armFailure();

            long failingTx = beginTransaction(manager, "tblspace1");
            for (int i = 0; i < RECORDS_PER_TXN; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(k1,v1) values(?,?)",
                        Arrays.asList(String.format("fail_%05d", i), PADDED_VALUE),
                        new TransactionContext(failingTx));
            }
            try {
                commitTransaction(manager, "tblspace1", failingTx);
                fail("commitTransaction should have failed because writePage was rigged to throw");
            } catch (Exception expected) {
                // Either the commit itself or its tracking future surfaces
                // a DataStorageManagerException (possibly wrapped). Walk
                // the cause chain to confirm.
                Throwable t = expected;
                boolean foundDsmFailure = false;
                while (t != null) {
                    if (t instanceof DataStorageManagerException
                            && t.getMessage() != null
                            && t.getMessage().contains("test-injected")) {
                        foundDsmFailure = true;
                        break;
                    }
                    t = t.getCause();
                }
                assertTrue("expected the test-injected DataStorageManagerException in the cause chain, got: "
                                + expected, foundDsmFailure);
            }

            // Disarm, then a subsequent commit must complete normally —
            // this is the load-bearing assertion: if the failed commit
            // had not decremented activeCommitApplies, the next commit
            // would block forever (or time out) while Phase C waits.
            failingDsm.disarmFailure();

            long okTx = beginTransaction(manager, "tblspace1");
            for (int i = 0; i < 50; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(k1,v1) values(?,?)",
                        Arrays.asList(String.format("ok_%05d", i), PADDED_VALUE),
                        new TransactionContext(okTx));
            }
            commitTransaction(manager, "tblspace1", okTx);

            // The follow-on commit must have advanced the LSN (verified
            // indirectly via successful query of the new rows).
            try (DataScanner scanner = scan(manager,
                    "SELECT count(*) as c FROM tblspace1.t1 where k1 like 'ok_%'",
                    Collections.emptyList())) {
                assertTrue("count(*) must return a row", scanner.hasNext());
                Object[] row = (Object[]) scanner.next().getValues();
                assertNotNull(row);
                assertEquals("follow-on commit's rows must be visible",
                        50L, ((Number) row[0]).longValue());
            }

            // The parallel-unloads counter must have grown — both the
            // failed commit and the successful one ran the batched path.
            assertTrue("parallel unloads must have been dispatched: counter="
                            + t1Manager.getParallelUnloadsCount(),
                    t1Manager.getParallelUnloadsCount() > 0);
        }
    }

    /**
     * Autocommit / log-replay inserts go through {@code apply()} →
     * {@code applyInsert(key, value, false)} (no-batch overload). This test
     * drives enough autocommit inserts to force eviction (so the legacy
     * synchronous {@code unload} path is actually exercised under load) and
     * asserts:
     *
     * <ul>
     *   <li>{@code getParallelUnloadsCount()} stays at zero — proving the
     *       no-batch overload remained on the autocommit path;</li>
     *   <li>all rows are queryable and durable across a restart — proving
     *       the legacy synchronous path remains correct under eviction.</li>
     * </ul>
     *
     * <p>Issue #448 review follow-up #1: replaces a previous version of this
     * test that was trivially satisfied because autocommit does not go
     * through {@link TableManager#onTransactionCommit} at all.
     */
    @Test
    public void autocommitInsertsKeepLegacySynchronousPath() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmpDir").toPath();

        // Big enough to force >16 page rotations and therefore many
        // eviction-driven unloads on the synchronous path.
        int autocommitRows = 200;

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            createTableSpaceAndTable(manager);
            TableManager t1Manager = tableManager(manager);

            for (int i = 0; i < autocommitRows; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(k1,v1) values(?,?)",
                        Arrays.asList(String.format("k_%05d", i), PADDED_VALUE),
                        NO_TRANSACTION);
            }

            // The number of unloads on the synchronous path can be observed
            // indirectly via TableManager.getStats().getUnloadedPagesCount();
            // confirm at least a few happened, otherwise this test does not
            // actually exercise the no-batch eviction path it claims to.
            long unloadedPages = t1Manager.getStats().getUnloadedPagesCount();
            assertTrue("test must drive enough autocommit inserts to actually evict pages: unloaded="
                            + unloadedPages, unloadedPages > 0);

            // The commit-time batched-unload counter must remain at zero —
            // autocommit DML never reaches onTransactionCommit, so the
            // batch is never instantiated, and no unload is ever dispatched
            // through it.
            assertEquals("autocommit path must not bump the parallel-unloads counter under eviction",
                    0L, t1Manager.getParallelUnloadsCount());

            assertRowCount(manager, autocommitRows);
        }

        // Durability across restart.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10_000);
            assertRowCount(manager, autocommitRows);
        }
    }

    /**
     * Recovery / log replay routes eviction-driven page unloads through a
     * recovery-scoped {@code CheckpointFlushBatch} (issue #559), even under
     * eviction pressure. This test produces a commit log with many
     * autocommit-mode INSERT entries (which are NOT followed by a checkpoint,
     * so they remain in the log), restarts, and asserts that:
     *
     * <ul>
     *   <li>recovery completes and every row is present;</li>
     *   <li>the parallel-unloads counter on the restarted
     *       {@link TableManager} is greater than zero — proving the recovery
     *       replay dispatched eviction-driven unloads through the
     *       recovery-scoped batch instead of flushing them synchronously on
     *       the recovery thread (issue #559).</li>
     * </ul>
     *
     * <p>This supersedes the issue #448 behaviour where recovery used the
     * no-batch synchronous overload: recovery runs single-threaded with no
     * concurrent Phase C checkpoint, so pipelining its page flushes is safe
     * and keeps slow remote writes off the recovery critical path.
     */
    @Test
    public void recoveryReplayUnderEvictionPressurePipelinesFlushes() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmpDir").toPath();

        int loggedRows = 300;

        // Phase 1: write rows with autocommit, do NOT checkpoint, close.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            createTableSpaceAndTable(manager);
            for (int i = 0; i < loggedRows; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(k1,v1) values(?,?)",
                        Arrays.asList(String.format("k_%05d", i), PADDED_VALUE),
                        NO_TRANSACTION);
            }
            // No checkpoint here — DBManager.close() does not force one,
            // so the next start replays the log (verified by extensive
            // existing tests like CheckpointStaleLsnRecoveryTest).
        }

        // Phase 2: restart; recovery replays the log through the
        // recovery-scoped batch. The parallel-unloads counter must grow.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10_000);
            TableManager t1Manager = tableManager(manager);

            assertTrue("recovery replay must dispatch eviction-driven unloads through the "
                            + "recovery-scoped batch: counter=" + t1Manager.getParallelUnloadsCount(),
                    t1Manager.getParallelUnloadsCount() > 0);
            assertRowCount(manager, loggedRows);
        }
    }

    // ---------- helpers ----------

    private static void createTableSpaceAndTable(DBManager manager) throws Exception {
        CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1",
                Collections.singleton("localhost"), "localhost", 1, 0, 0);
        manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                NO_TRANSACTION);
        manager.waitForTablespace("tblspace1", 10_000);
        execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, v1 string)",
                Collections.emptyList());
    }

    private static TableManager tableManager(DBManager manager) {
        return (TableManager) manager.getTableSpaceManager("tblspace1").getTableManager("t1");
    }

    private static void assertRowCount(DBManager manager, int expectedRows) throws Exception {
        try (DataScanner scanner = scan(manager,
                "SELECT k1 FROM tblspace1.t1", Collections.emptyList())) {
            int actual = 0;
            List<String> seenKeys = new ArrayList<>();
            while (scanner.hasNext()) {
                Object[] row = (Object[]) scanner.next().getValues();
                assertNotNull("scan row must contain a key", row);
                seenKeys.add(String.valueOf(row[0]));
                actual++;
            }
            assertEquals("scan returned wrong number of rows: " + seenKeys.size()
                            + " keys (showing first 10 if too many): "
                            + seenKeys.subList(0, Math.min(10, seenKeys.size())),
                    expectedRows, actual);
        }
    }

    private static void assertAllRowsHaveValue(DBManager manager,
            String expectedValue, int expectedRows) throws Exception {
        try (DataScanner scanner = scan(manager,
                "SELECT k1, v1 FROM tblspace1.t1", Collections.emptyList())) {
            int actual = 0;
            while (scanner.hasNext()) {
                Object[] row = (Object[]) scanner.next().getValues();
                assertNotNull(row);
                assertNotNull("v1 must not be null for key " + row[0], row[1]);
                assertEquals("each row must carry the expected updated value (key=" + row[0] + ")",
                        expectedValue, String.valueOf(row[1]));
                actual++;
            }
            assertEquals(expectedRows, actual);
        }
    }

    private static String repeat(String fragment, int count) {
        StringBuilder sb = new StringBuilder(fragment.length() * count);
        for (int i = 0; i < count; i++) {
            sb.append(fragment);
        }
        return sb.toString();
    }

    /**
     * A {@link FileDataStorageManager} that throws
     * {@link DataStorageManagerException} from {@link #writePage} when
     * armed. Used by {@link #commitFailureDrainsBatchAndReleasesApplySlot()}
     * to verify the failure path inside
     * {@link TableManager#onTransactionCommit} releases the apply slot.
     */
    private static final class FailingFileDataStorageManager extends FileDataStorageManager {
        private final AtomicBoolean armed = new AtomicBoolean();
        private final AtomicInteger failedCalls = new AtomicInteger();

        FailingFileDataStorageManager(Path baseDir) {
            super(baseDir);
        }

        void armFailure() {
            armed.set(true);
        }

        void disarmFailure() {
            armed.set(false);
        }

        @Override
        public void writePage(String tableSpace, String tableName, long pageId,
                Collection<Record> newPage) throws DataStorageManagerException {
            if (armed.get()) {
                failedCalls.incrementAndGet();
                throw new DataStorageManagerException("test-injected failure on writePage for page " + pageId);
            }
            super.writePage(tableSpace, tableName, pageId, newPage);
        }
    }
}
