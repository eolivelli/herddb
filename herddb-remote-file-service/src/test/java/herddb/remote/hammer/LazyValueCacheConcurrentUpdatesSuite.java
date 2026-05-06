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

package herddb.remote.hammer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import herddb.core.DBManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.ScanResult;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.remote.LazyValueCache;
import herddb.server.ServerConfiguration;
import herddb.remote.RemoteFileDataStorageManager;
import herddb.remote.RemoteFileServer;
import herddb.remote.RemoteFileServiceClient;
import herddb.sql.TranslatedQuery;
import herddb.utils.DataAccessor;
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
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #411 — concurrent-DML hammer suite that drives every read through
 * {@link RemoteFileDataStorageManager} (lazy DSM) so the
 * {@link LazyValueCache} sees the same churn that the file-DSM hammer
 * suite ({@code DirectMultipleConcurrentUpdatesSuite} in
 * {@code herddb-core}) applies to {@code FileDataStorageManager}.
 *
 * <p>The shape mirrors the file-DSM suite (same TABLESIZE × MULTIPLIER
 * × thread-pool sizing, same insert-then-update-or-get loop, same
 * post-restart consistency check) but the storage backend is wired
 * end-to-end through a real {@link RemoteFileServer} +
 * {@link RemoteFileServiceClient} +
 * {@link RemoteFileDataStorageManager} stack so the lazy read path
 * (page header + index byte-range read, per-record value byte-range
 * read, value cache hits/misses, weight-based eviction, cache
 * invalidation on page overwrite) is exercised under heavy concurrent
 * load.
 *
 * <p>Sizing is reduced relative to the file-DSM suite because every
 * read in the hot loop is mediated by the gRPC byte-range path; we keep
 * enough volume to fill several data pages and force checkpoint-driven
 * page rotation while staying inside the 60 s default JUnit timeout per
 * test method.
 *
 * @see LazyValueCacheConcurrentUpdatesSuiteNoIndexesTest
 * @see LazyValueCacheConcurrentUpdatesSuiteWithNonUniqueIndexesTest
 * @see LazyValueCacheConcurrentUpdatesSuiteWithUniqueIndexesTest
 */
public abstract class LazyValueCacheConcurrentUpdatesSuite {

    private static final int TABLESIZE = 500;
    private static final int MULTIPLIER = 2;
    private static final int THREADPOLSIZE = 12;
    /**
     * Cache budget kept tight so the post-restart consistency scan
     * generates plenty of value-cache traffic (every value read is a
     * pooled-direct-buffer allocation; the cache evicts under churn).
     */
    private static final long CACHE_BUDGET_BYTES = 64L * 1024L;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Run the same insert-then-concurrent-update workload that
     * {@code DirectMultipleConcurrentUpdatesSuite} runs against the file
     * DSM, but route every read through the lazy DSM so the
     * {@link LazyValueCache} is the cache being exercised.
     *
     * @param useTransactions   wrap each DML in an auto-transaction
     * @param checkPointPeriod  ms between automatic checkpoints; pass
     *                          {@code 0} to disable so the test only
     *                          checkpoints once at the end
     * @param withIndexes       create a secondary index on {@code n1}
     * @param uniqueIndexes     when {@code withIndexes}, make it UNIQUE
     */
    protected void performTest(boolean useTransactions, long checkPointPeriod,
            boolean withIndexes, boolean uniqueIndexes) throws Exception {
        Path remoteDir = folder.newFolder("remote").toPath();
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpPath = folder.newFolder("tmp").toPath();
        String nodeId = "localhost";

        ConcurrentHashMap<String, Long> expectedValue = new ConcurrentHashMap<>();
        // Per-key history of writes (thread, op, tx, oldValue -> newValue) — dumped when
        // erroredKeys is non-empty so we can match stale reads against the actual sequence
        // of committed writes. Mirrors the file-DSM suite's diagnostics.
        ConcurrentHashMap<String, List<String>> writeHistory = new ConcurrentHashMap<>();

        try (RemoteFileServer server = new RemoteFileServer(0, remoteDir)) {
            server.start();
            String addr = "localhost:" + server.getPort();

            // Phase 1: populate + concurrent workload.
            CacheStats statsAfterPhase1;
            try (RemoteFileServiceClient client = new RemoteFileServiceClient(Arrays.asList(addr));
                 LazyValueCache cache = new LazyValueCache(CACHE_BUDGET_BYTES);
                 RemoteFileDataStorageManager dsm = new RemoteFileDataStorageManager(
                         dataPath, tmpPath, 1000, client, cache);
                 DBManager manager = new DBManager(nodeId,
                         new FileMetadataStorageManager(metadataPath),
                         dsm,
                         new FileCommitLogManager(logsPath),
                         tmpPath, null,
                         tightMemoryConfiguration(),
                         null)) {
                manager.setCheckpointPeriod(checkPointPeriod);
                manager.start();
                // DBManager.start() creates the default tablespace
                // automatically when running standalone; just wait for it.
                assertTrue("tablespace must be ready",
                        manager.waitForBootOfLocalTablespaces(30000));

                execute(manager, TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)",
                        Collections.emptyList(), TransactionContext.NO_TRANSACTION);
                if (withIndexes) {
                    if (uniqueIndexes) {
                        // n1 + id keeps the UNIQUE constraint satisfied as values churn.
                        execute(manager, TableSpace.DEFAULT,
                                "CREATE UNIQUE INDEX theindex ON mytable (n1, id)",
                                Collections.emptyList(), TransactionContext.NO_TRANSACTION);
                    } else {
                        execute(manager, TableSpace.DEFAULT,
                                "CREATE INDEX theindex ON mytable (n1)",
                                Collections.emptyList(), TransactionContext.NO_TRANSACTION);
                    }
                }

                long tx = beginTransaction(manager, TableSpace.DEFAULT);
                for (int i = 0; i < TABLESIZE; i++) {
                    executeUpdate(manager,
                            "INSERT INTO mytable (id,n1,n2) values(?,?,?)",
                            Arrays.asList("test_" + i, 1L, 2),
                            new TransactionContext(tx));
                    expectedValue.put("test_" + i, 1L);
                }
                commitTransaction(manager, TableSpace.DEFAULT, tx);

                // Force the pages that hold the inserted rows to be flushed
                // to remote storage so the subsequent read traffic actually
                // exercises the lazy load + value-cache path. Without this
                // the entire dataset stays in mutable on-heap pages and the
                // cache never sees a single getOrFetch on the hot loop.
                manager.checkpoint();

                ExecutorService threadPool = Executors.newFixedThreadPool(THREADPOLSIZE);
                try {
                    List<Future<?>> futures = new ArrayList<>();
                    AtomicLong updates = new AtomicLong();
                    AtomicLong skipped = new AtomicLong();
                    AtomicLong gets = new AtomicLong();
                    for (int i = 0; i < TABLESIZE * MULTIPLIER; i++) {
                        futures.add(threadPool.submit(() -> {
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
                                    DMLStatementExecutionResult updateResult = executeUpdate(
                                            manager,
                                            "UPDATE mytable set n1=? WHERE id=?",
                                            Arrays.asList(value, "test_" + k),
                                            new TransactionContext(useTransactions
                                                    ? TransactionContext.AUTOTRANSACTION_ID
                                                    : TransactionContext.NOTRANSACTION_ID));
                                    long count = updateResult.getUpdateCount();
                                    transactionId = updateResult.transactionId;
                                    if (count <= 0) {
                                        throw new RuntimeException("not updated ?");
                                    }
                                } else {
                                    gets.incrementAndGet();
                                    DataScanner res = scanKeepReadLocks(manager,
                                            "SELECT * FROM mytable where id=?",
                                            Arrays.asList("test_" + k),
                                            new TransactionContext(useTransactions
                                                    ? TransactionContext.AUTOTRANSACTION_ID
                                                    : TransactionContext.NOTRANSACTION_ID));
                                    if (!res.hasNext()) {
                                        throw new RuntimeException("not found?");
                                    }
                                    res.close();
                                    transactionId = res.getTransactionId();
                                    value = actual; // value did not change
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
                        }));
                    }
                    for (Future<?> f : futures) {
                        f.get(180, TimeUnit.SECONDS);
                    }

                    System.out.println("stats::updates:" + updates);
                    System.out.println("stats::get:" + gets);
                    assertTrue(updates.get() > 0);
                    assertTrue(gets.get() > 0);

                    List<String> erroredKeys = consistencyCheck(manager, expectedValue);
                    if (!erroredKeys.isEmpty()) {
                        dumpHistory("PRE-RESTART", erroredKeys, expectedValue, writeHistory);
                    }
                    assertTrue(erroredKeys.isEmpty());

                    // Force one final checkpoint so any dirty pages from the
                    // hot loop end up on remote storage and the post-restart
                    // recovery covers the same ground we just exercised.
                    manager.checkpoint();
                } finally {
                    threadPool.shutdown();
                    threadPool.awaitTermination(1, TimeUnit.MINUTES);
                }

                // Snapshot the cache stats BEFORE storage.close() drains the
                // cache (close() resets internal counters via invalidateAll).
                statsAfterPhase1 = cache.stats();
            }
            assertNotNull("Caffeine stats must be available in Phase 1", statsAfterPhase1);
            System.out.println("stats::phase1 cache misses:" + statsAfterPhase1.missCount()
                    + " hits:" + statsAfterPhase1.hitCount()
                    + " evictions:" + statsAfterPhase1.evictionCount());

            // Phase 2: restart and recover. The remote file server stays up
            // across the close so the recovery path reads from the same
            // backing storage. Every page in this phase starts cold (no
            // in-memory pages map) so the post-restart consistency scan
            // *must* drive value-cache traffic — this is the deterministic
            // regression gate.
            CacheStats statsAfterPhase2;
            try (RemoteFileServiceClient client = new RemoteFileServiceClient(Arrays.asList(addr));
                 LazyValueCache cache = new LazyValueCache(CACHE_BUDGET_BYTES);
                 RemoteFileDataStorageManager dsm = new RemoteFileDataStorageManager(
                         dataPath, tmpPath, 1000, client, cache);
                 DBManager manager = new DBManager(nodeId,
                         new FileMetadataStorageManager(metadataPath),
                         dsm,
                         new FileCommitLogManager(logsPath),
                         tmpPath, null,
                         tightMemoryConfiguration(),
                         null)) {
                manager.start();
                assertTrue(manager.waitForBootOfLocalTablespaces(60000));

                List<String> erroredKeys = consistencyCheck(manager, expectedValue);
                if (!erroredKeys.isEmpty()) {
                    dumpHistory("POST-RESTART", erroredKeys, expectedValue, writeHistory);
                }
                assertTrue(erroredKeys.isEmpty());

                statsAfterPhase2 = cache.stats();
            }
            assertNotNull("Caffeine stats must be available in Phase 2", statsAfterPhase2);
            System.out.println("stats::phase2 cache misses:" + statsAfterPhase2.missCount()
                    + " hits:" + statsAfterPhase2.hitCount()
                    + " evictions:" + statsAfterPhase2.evictionCount());

            // Issue #411 regression gate: across both phases the cache must
            // have served at least one miss. If a future refactor bypasses
            // the lazy read path entirely (e.g. by switching the DSM back
            // to eager `readPage`) this assertion fails. The Phase 2
            // post-restart scan is the deterministic source of misses.
            long totalMisses = statsAfterPhase1.missCount() + statsAfterPhase2.missCount();
            assertTrue("LazyValueCache must serve at least one miss across the"
                            + " hammer workload + post-restart scan"
                            + " (phase1=" + statsAfterPhase1.missCount()
                            + " phase2=" + statsAfterPhase2.missCount() + ")",
                    totalMisses > 0);
        }
    }

    private List<String> consistencyCheck(DBManager manager,
            ConcurrentHashMap<String, Long> expectedValue) throws Exception {
        List<String> erroredKeys = new ArrayList<>();
        for (Map.Entry<String, Long> entry : expectedValue.entrySet()) {
            List<DataAccessor> records;
            DataAccessor data;
            try (DataScanner res = scan(manager, "SELECT n1 FROM mytable where id=?",
                    Arrays.asList(entry.getKey()))) {
                records = res.consume();
                data = records.get(0);
            }
            assertEquals(1, records.size());
            if (!entry.getValue().equals(data.get("n1"))) {
                System.out.println("expected value " + data.get("n1")
                        + ", but got " + entry.getValue() + " for key " + entry.getKey());
                erroredKeys.add(entry.getKey());
            }
        }
        return erroredKeys;
    }

    /**
     * Diagnostics for issue #157-style stale-read failures (mirrored from
     * the file-DSM suite). Print the full writer-thread history for every
     * key that the final scan returned a stale value for.
     */
    private static void dumpHistory(
            String stage,
            List<String> erroredKeys,
            Map<String, Long> expectedValue,
            Map<String, List<String>> writeHistory) {
        System.out.println("=== lazy-DSM hammer diagnostics [" + stage + "] — "
                + erroredKeys.size() + " errored keys ===");
        for (String key : erroredKeys) {
            Long expected = expectedValue.get(key);
            List<String> history = writeHistory.getOrDefault(key, Collections.emptyList());
            System.out.println("  key=" + key + " expected=" + expected
                    + " history(" + history.size() + "):");
            for (String h : history) {
                System.out.println("    " + h);
            }
        }
        System.out.println("=== end lazy-DSM hammer diagnostics [" + stage + "] ===");
    }

    /**
     * Build a {@link ServerConfiguration} with a tight data-page memory
     * budget so the in-memory page cache evicts aggressively and
     * subsequent reads have to re-fetch through the lazy DSM (which is
     * what exercises the {@link LazyValueCache}).
     *
     * <p>Mirrors {@code DirectMultipleConcurrentUpdatesSuite}'s budget
     * (256 KiB data, 1 MiB PK index, 10 KiB max logical page size) so the
     * two suites stress the same eviction shape.
     */
    private static ServerConfiguration tightMemoryConfiguration() {
        ServerConfiguration cfg = new ServerConfiguration();
        cfg.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 10 * 1024);
        cfg.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, 1024 * 1024 / 4);
        cfg.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, 1024 * 1024);
        return cfg;
    }

    // -------------------------------------------------------------------------
    // Self-contained DBManager helpers (the herddb-core TestUtils class is not
    // on this module's classpath because herddb-core is not declared as a
    // test-jar dependency; mirrors the helper pattern used by RemoteTestUtils).
    // -------------------------------------------------------------------------

    private static StatementExecutionResult execute(DBManager manager, String tableSpace,
            String query, List<Object> parameters, TransactionContext txCtx)
            throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(
                tableSpace, query, parameters, true, true, false, -1);
        return manager.executePlan(translated.plan, translated.context, txCtx);
    }

    private static DMLStatementExecutionResult executeUpdate(DBManager manager, String query,
            List<Object> parameters) throws StatementExecutionException {
        return executeUpdate(manager, query, parameters, TransactionContext.NO_TRANSACTION);
    }

    private static DMLStatementExecutionResult executeUpdate(DBManager manager, String query,
            List<Object> parameters, TransactionContext txCtx) throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(
                TableSpace.DEFAULT, query, parameters, true, true, false, -1);
        return (DMLStatementExecutionResult) manager.executePlan(
                translated.plan, translated.context, txCtx);
    }

    private static DataScanner scan(DBManager manager, String query, List<Object> parameters)
            throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(
                TableSpace.DEFAULT, query, parameters, true, true, false, -1);
        return ((ScanResult) manager.executePlan(
                translated.plan, translated.context, TransactionContext.NO_TRANSACTION)).dataScanner;
    }

    private static DataScanner scanKeepReadLocks(DBManager manager, String query,
            List<Object> parameters, TransactionContext txCtx) throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(
                TableSpace.DEFAULT, query, parameters, true, true, false, -1);
        translated.context.setForceRetainReadLock(true);
        return ((ScanResult) manager.executePlan(
                translated.plan, translated.context, txCtx)).dataScanner;
    }

    private static long beginTransaction(DBManager manager, String tableSpace)
            throws StatementExecutionException {
        return ((TransactionResult) execute(manager, tableSpace,
                "BEGIN TRANSACTION '" + tableSpace + "'", Collections.emptyList(),
                TransactionContext.NO_TRANSACTION)).getTransactionId();
    }

    private static void commitTransaction(DBManager manager, String tableSpace, long tx)
            throws StatementExecutionException {
        execute(manager, tableSpace,
                "COMMIT TRANSACTION '" + tableSpace + "','" + tx + "'",
                Collections.emptyList(), TransactionContext.NO_TRANSACTION);
    }
}
