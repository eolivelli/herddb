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

import static herddb.core.TestUtils.beginTransaction;
import static herddb.core.TestUtils.commitTransaction;
import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.DBManager;
import herddb.core.TableManager;
import herddb.core.stats.TableManagerStats;
import herddb.model.DataScanner;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.DataAccessor;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Hammer suite for issue #448 — multi-operation transactions under eviction
 * pressure, with optional concurrent checkpoints.
 *
 * <p>The pre-existing {@link DirectMultipleConcurrentUpdatesSuite} hammer
 * exercises {@link herddb.core.TableManager#onTransactionCommit
 * onTransactionCommit} only in its <em>pre-populate</em> phase (one big
 * INSERT-only transaction); its hot loop uses single-statement
 * auto-transactions, so each commit applies exactly one record. That leaves
 * a coverage gap exactly where issue #448 changes behaviour: the
 * commit-time apply loop in {@code onTransactionCommit} that now dispatches
 * eviction-driven {@code unload} calls to the checkpoint-flush executor via
 * {@code CheckpointFlushBatch}.
 *
 * <p>This suite closes the gap by driving many concurrent <em>multi-op</em>
 * transactions (random INSERT / UPDATE / DELETE inside one BEGIN/COMMIT)
 * against a tiny page-cache budget so that almost every commit triggers
 * several eviction-driven unloads. Every worker thread owns a disjoint key
 * range — concurrent commits cannot collide on locks, so any failure must
 * come from the page / keyToPage / LSN machinery this PR touches.
 *
 * <p>Subclasses parameterize:
 * <ul>
 *   <li>presence and uniqueness of a secondary index (no-index, non-unique,
 *       unique) — exercises the {@code applyUpdate} +
 *       {@code AbstractIndexManager.recordUpdated} fan-out under eviction;</li>
 *   <li>whether a periodic checkpoint runs concurrently — exercises the
 *       interleaving between Phase B / Phase C and the new commit-batched
 *       unload dispatch (issue #157 / #431 / #448 invariants).</li>
 * </ul>
 *
 * <p>Each test verifies both the in-memory state immediately after the run
 * AND the post-restart state (after replaying the commit log), so any
 * regression in keyToPage durability or LSN ordering surfaces immediately.
 */
public abstract class Issue448BulkCommitHammerSuite {

    /** Number of worker threads driving transactions in parallel. */
    protected static final int THREADS = 8;

    /** Number of multi-op transactions each worker thread runs. */
    protected static final int TXNS_PER_THREAD = 25;

    /** Minimum number of operations per transaction (inclusive). */
    protected static final int MIN_OPS_PER_TXN = 5;

    /** Maximum number of operations per transaction (inclusive). */
    protected static final int MAX_OPS_PER_TXN = 15;

    /**
     * Per-thread initial population. Each thread starts with this many rows
     * keyed in its own disjoint range; subsequent transactions grow,
     * shrink, or modify that set.
     */
    protected static final int INITIAL_ROWS_PER_THREAD = 60;

    /**
     * Per-future timeout for the worker pool. Strictly less than each
     * subclass's {@code @Test(timeout=...)} so a stuck future fires
     * {@link TimeoutException} before JUnit interrupts the test thread —
     * lets the {@link #dumpOnFailure} TestWatcher capture a meaningful
     * thread dump (same pattern as
     * {@link DirectMultipleConcurrentUpdatesSuite}).
     */
    protected static final long FUTURE_TIMEOUT_SECONDS = 90L;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TestWatcher dumpOnFailure = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            DirectMultipleConcurrentUpdatesSuite.dumpAllThreads(
                    description.getMethodName() + " failed: " + e.getClass().getSimpleName());
        }
    };

    /**
     * @param checkPointPeriod 0 disables the periodic checkpoint thread,
     *                         &gt; 0 sets the checkpoint interval in
     *                         milliseconds.
     * @param withIndexes      if true, a secondary index on column n1 is
     *                         created (forces every applyInsert and
     *                         applyUpdate inside the commit to also walk
     *                         the index manager).
     * @param uniqueIndexes    if {@code withIndexes}, controls UNIQUE vs
     *                         non-unique. Ignored otherwise.
     */
    protected void performHammer(long checkPointPeriod, boolean withIndexes, boolean uniqueIndexes) throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration cfg = newServerConfigurationWithAutoPort(baseDir);

        // Tiny page-cache budget so commits routinely evict pages.
        // ~16 pages of 2 KiB; per-thread row sizes (~250 B each) mean a few
        // rows per page, so many commits trigger 2-5 evictions each.
        cfg.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 2 * 1024L);
        cfg.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, 32 * 1024L);
        cfg.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, 1024 * 1024L);
        cfg.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, checkPointPeriod);
        cfg.set(ServerConfiguration.PROPERTY_DATADIR, folder.newFolder().getAbsolutePath());
        cfg.set(ServerConfiguration.PROPERTY_LOGDIR, folder.newFolder().getAbsolutePath());

        // Expected end state: shared across threads only because each
        // thread writes into its own key range (no overlap). ConcurrentHashMap
        // is sufficient because no key is ever touched by more than one thread.
        ConcurrentHashMap<String, Long> expectedState = new ConcurrentHashMap<>();

        try (Server server = new Server(cfg)) {
            server.start();
            server.waitForStandaloneBoot();
            DBManager manager = server.getManager();

            execute(manager, "CREATE TABLE mytable (id string primary key, n1 long, payload string)",
                    Collections.emptyList());
            if (withIndexes) {
                if (uniqueIndexes) {
                    execute(manager, "CREATE UNIQUE INDEX theindex ON mytable (n1, id)",
                            Collections.emptyList());
                } else {
                    execute(manager, "CREATE INDEX theindex ON mytable (n1)",
                            Collections.emptyList());
                }
            }

            // Pre-populate per-thread key ranges with autocommit so the page
            // cache fills up before the first transaction-mode commit.
            String basePayload = repeat("p", 220);
            for (int t = 0; t < THREADS; t++) {
                for (int i = 0; i < INITIAL_ROWS_PER_THREAD; i++) {
                    String key = String.format("t%02d_k%05d", t, i);
                    long val = (long) (t * 1_000_000L + i);
                    executeUpdate(manager,
                            "INSERT INTO mytable(id,n1,payload) values(?,?,?)",
                            Arrays.asList(key, val, basePayload),
                            TransactionContext.NO_TRANSACTION);
                    expectedState.put(key, val);
                }
            }
            // Force a checkpoint so most pre-populate pages are now immutable
            // on disk; later commits will evict immutable pages too.
            manager.checkpoint();

            // Snapshot the parallel-unloads counter so we can assert that
            // the new code path is actually exercised by the hammer.
            TableManager tableManager = (TableManager) server.getManager()
                    .getTableSpaceManager(TableSpace.DEFAULT).getTableManager("mytable");
            long parallelUnloadsBefore = tableManager.getParallelUnloadsCount();

            // Hot loop: each thread runs TXNS_PER_THREAD multi-op transactions
            // against its own disjoint key range. Each transaction interleaves
            // INSERTs, UPDATEs, and DELETEs.
            ExecutorService pool = Executors.newFixedThreadPool(THREADS);
            List<Future<?>> futures = new ArrayList<>(THREADS);
            try {
                for (int t = 0; t < THREADS; t++) {
                    final int threadId = t;
                    futures.add(pool.submit(() -> runWorker(manager, threadId, expectedState, basePayload)));
                }
                for (Future<?> f : futures) {
                    try {
                        f.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    } catch (TimeoutException te) {
                        DirectMultipleConcurrentUpdatesSuite.dumpAllThreads(
                                "issue #448 hammer worker timed out");
                        throw te;
                    }
                }
            } finally {
                pool.shutdown();
                pool.awaitTermination(1, TimeUnit.MINUTES);
            }

            long parallelUnloadsAfter = tableManager.getParallelUnloadsCount();
            long delta = parallelUnloadsAfter - parallelUnloadsBefore;
            // The hot loop must have driven at least one eviction-driven
            // unload through the new commit-time CheckpointFlushBatch — else
            // the test does not actually exercise the code path it claims.
            assertTrue("issue #448 hammer must dispatch parallel unloads from onTransactionCommit:"
                            + " delta=" + delta + " before=" + parallelUnloadsBefore
                            + " after=" + parallelUnloadsAfter,
                    delta > 0);

            // In-memory consistency after the run.
            verifyAgainstExpected(manager, expectedState, "PRE-RESTART");

            TableManagerStats stats = tableManager.getStats();
            System.out.println("issue-448 hammer stats:: tablesize=" + stats.getTablesize()
                    + " unloadedPages=" + stats.getUnloadedPagesCount()
                    + " loadedPages=" + stats.getLoadedPagesCount()
                    + " parallelUnloads=" + parallelUnloadsAfter);
            assertEquals("table-size after hot loop must equal expected map size",
                    expectedState.size(), stats.getTablesize());
        }

        // Restart and verify durability — any commit whose pages were
        // unloaded asynchronously must still be replayed correctly.
        try (Server server = new Server(cfg)) {
            server.start();
            server.waitForTableSpaceBoot(TableSpace.DEFAULT, 300_000, true);
            verifyAgainstExpected(server.getManager(), expectedState, "POST-RESTART");
        }
    }

    /**
     * Worker body: runs {@link #TXNS_PER_THREAD} multi-op transactions on
     * keys in this worker's disjoint range. Mutates {@code expectedState}
     * in lock-step with successful commits so the post-run verification has
     * an authoritative reference.
     *
     * <p>Each transaction picks a random ops-per-txn count in
     * {@code [MIN_OPS_PER_TXN, MAX_OPS_PER_TXN]} and, for each op,
     * randomly chooses an INSERT / UPDATE / DELETE biased by the current
     * size of the live key set. Transactions are committed atomically;
     * {@code expectedState} is updated only after a successful commit so a
     * commit-failure path (e.g. a hypothetical regression that throws mid
     * apply) cannot corrupt the reference map.
     */
    private void runWorker(DBManager manager, int threadId,
            ConcurrentHashMap<String, Long> expectedState, String basePayload) {
        try {
            // Reconstruct this worker's local view of its own keys from the
            // pre-population. Subsequent inserts grow this set; deletes shrink
            // it. All keys in this set are exclusive to this thread.
            Set<String> liveKeys = new HashSet<>();
            for (int i = 0; i < INITIAL_ROWS_PER_THREAD; i++) {
                liveKeys.add(String.format("t%02d_k%05d", threadId, i));
            }
            int nextInsertSeq = INITIAL_ROWS_PER_THREAD;

            ThreadLocalRandom rng = ThreadLocalRandom.current();

            for (int txn = 0; txn < TXNS_PER_THREAD; txn++) {
                long tx = beginTransaction(manager, TableSpace.DEFAULT);

                // Stage local mutations and only apply them to expectedState
                // after the commit succeeds.
                List<Map.Entry<String, Long>> staged = new ArrayList<>();
                List<String> stagedDeletes = new ArrayList<>();

                int ops = rng.nextInt(MIN_OPS_PER_TXN, MAX_OPS_PER_TXN + 1);
                for (int op = 0; op < ops; op++) {
                    int choice = chooseOp(liveKeys.size(), rng);
                    if (choice == 0) {
                        // INSERT a brand-new key.
                        String key = String.format("t%02d_k%05d", threadId, nextInsertSeq++);
                        long val = (long) (threadId * 1_000_000L + 100_000L + op + txn);
                        executeUpdate(manager,
                                "INSERT INTO mytable(id,n1,payload) values(?,?,?)",
                                Arrays.asList(key, val, basePayload),
                                new TransactionContext(tx));
                        liveKeys.add(key);
                        staged.add(Map.entry(key, val));
                    } else if (choice == 1) {
                        // UPDATE an existing key.
                        String key = pickRandomKey(liveKeys, rng);
                        long val = (long) (threadId * 1_000_000L + 200_000L + op + txn);
                        executeUpdate(manager,
                                "UPDATE mytable SET n1=? WHERE id=?",
                                Arrays.asList(val, key),
                                new TransactionContext(tx));
                        staged.add(Map.entry(key, val));
                    } else {
                        // DELETE an existing key.
                        String key = pickRandomKey(liveKeys, rng);
                        executeUpdate(manager,
                                "DELETE FROM mytable WHERE id=?",
                                Arrays.asList(key),
                                new TransactionContext(tx));
                        liveKeys.remove(key);
                        stagedDeletes.add(key);
                    }
                }
                commitTransaction(manager, TableSpace.DEFAULT, tx);

                // Commit succeeded — propagate staged mutations to the
                // shared expected state.
                for (Map.Entry<String, Long> e : staged) {
                    expectedState.put(e.getKey(), e.getValue());
                }
                for (String k : stagedDeletes) {
                    expectedState.remove(k);
                }
            }
        } catch (RuntimeException err) {
            // Worker-thread death guard: any unexpected failure must surface
            // as a wrapped RuntimeException so the future captures it.
            // CLAUDE.md: this broad catch is the worker-thread top-level
            // diagnostic boundary — narrower would lose checked exceptions
            // from executeUpdate / beginTransaction.
            throw err;
        } catch (Exception err) {
            throw new RuntimeException("worker " + threadId + " failed", err);
        }
    }

    /**
     * Op-selector biased by {@code liveKeys.size()} so the live set neither
     * collapses to zero (which would starve UPDATE / DELETE forever) nor
     * grows unboundedly. Returns 0 for INSERT, 1 for UPDATE, 2 for DELETE.
     */
    private static int chooseOp(int liveKeyCount, ThreadLocalRandom rng) {
        if (liveKeyCount == 0) {
            return 0; // must INSERT — nothing to update or delete
        }
        // Roll on a small biased die: insert ~40 %, update ~40 %, delete ~20 %.
        int r = rng.nextInt(10);
        if (r < 4) {
            return 0;
        }
        if (r < 8) {
            return 1;
        }
        return 2;
    }

    /**
     * Pick a uniformly-random key from a {@link Set} without allocating a
     * snapshot list each time (the live set can have hundreds of keys).
     */
    private static String pickRandomKey(Set<String> liveKeys, ThreadLocalRandom rng) {
        int target = rng.nextInt(liveKeys.size());
        int i = 0;
        for (String k : liveKeys) {
            if (i++ == target) {
                return k;
            }
        }
        throw new AssertionError("liveKeys mutated during pickRandomKey");
    }

    private static void verifyAgainstExpected(DBManager manager,
            Map<String, Long> expectedState, String stage) throws Exception {
        // (1) Row count matches.
        try (DataScanner sc = scan(manager,
                "SELECT count(*) AS c FROM mytable", Collections.emptyList())) {
            assertTrue(sc.hasNext());
            Object[] row = (Object[]) sc.next().getValues();
            long actualCount = ((Number) row[0]).longValue();
            assertEquals(stage + ": row count must equal expected map size",
                    expectedState.size(), actualCount);
        }
        // (2) Each expected key carries the expected n1 value.
        List<String> errors = new ArrayList<>();
        for (Map.Entry<String, Long> e : expectedState.entrySet()) {
            try (DataScanner sc = scan(manager,
                    "SELECT n1 FROM mytable WHERE id=?",
                    Arrays.asList(e.getKey()))) {
                List<DataAccessor> rs = sc.consume();
                if (rs.isEmpty()) {
                    errors.add(stage + ": missing key " + e.getKey()
                            + ", expected n1=" + e.getValue());
                } else if (!e.getValue().equals(rs.get(0).get("n1"))) {
                    errors.add(stage + ": key " + e.getKey() + " has n1="
                            + rs.get(0).get("n1") + ", expected " + e.getValue());
                }
            }
        }
        if (!errors.isEmpty()) {
            // Fail with up to 20 errors to keep the surefire report readable.
            int show = Math.min(20, errors.size());
            StringBuilder sb = new StringBuilder(stage)
                    .append(": ").append(errors.size())
                    .append(" key-level mismatches; first ").append(show).append(":\n");
            for (int i = 0; i < show; i++) {
                sb.append("  ").append(errors.get(i)).append('\n');
            }
            fail(sb.toString());
        }
    }

    private static String repeat(String fragment, int count) {
        StringBuilder sb = new StringBuilder(fragment.length() * count);
        for (int i = 0; i < count; i++) {
            sb.append(fragment);
        }
        return sb.toString();
    }
}
