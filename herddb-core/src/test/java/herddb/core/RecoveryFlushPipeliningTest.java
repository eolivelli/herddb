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
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #559: pipeline eviction-driven page flushes during
 * WAL recovery.
 *
 * <p>Before the fix, recovery replayed the commit log one entry at a time and,
 * for every commit, dispatched its eviction-driven page unloads to a
 * <em>per-commit</em> {@code CheckpointFlushBatch} that was {@code awaitAll()}-ed
 * before the next commit could be replayed (issue #448). Non-transactional
 * recovery DML did not batch at all — it flushed each victim page synchronously
 * on the recovery thread. With a remote (S3/MinIO-backed) data storage manager
 * each flush is a slow multi-block upload, so recovery serialised those uploads
 * onto its critical path.
 *
 * <p>The fix gives each {@code TableManager} a single recovery-scoped
 * {@code CheckpointFlushBatch}: page flushes of commit N overlap the replay of
 * commit N+1, and the batch is joined exactly once — by
 * {@code awaitRecoveryFlushes()} — before the post-recovery checkpoint runs, so
 * a checkpoint never persists a {@code keyToPage} entry pointing at a page
 * whose {@code writePage} has not completed.
 *
 * <p>These tests exercise recovery under page-eviction pressure and assert
 * that:
 * <ul>
 *   <li>recovery of transactional commits routes eviction-driven unloads
 *       through the recovery-scoped batch ({@code getParallelUnloadsCount() > 0}
 *       on the restarted table manager);</li>
 *   <li>recovery of non-transactional (autocommit) DML does the same;</li>
 *   <li>no record is lost or corrupted — every row survives the restart, and
 *       the post-recovery checkpoint is itself consistent, verified by a
 *       second restart that recovers from that checkpoint.</li>
 * </ul>
 */
public class RecoveryFlushPipeliningTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Small enough that a handful of rows fill a page, so eviction kicks in
     * well within each test's row count.
     */
    private static final long PAGE_SIZE_BYTES = 2 * 1024L;

    /**
     * Cap data-page memory at ~16 pages so the page replacement policy is at
     * capacity quickly and every subsequent page rotation during recovery
     * yields an eviction victim — exactly the scenario the fix optimizes.
     */
    private static final long DATA_MEMORY_BYTES = 32 * 1024L;

    /**
     * Padded value attached to every row. ~200 B together with the key plus
     * serializer overhead means roughly half a dozen rows per 2 KiB page, so
     * a few hundred rows cause many page rotations.
     */
    private static final String PADDED_VALUE = repeat("x", 200);

    private static ServerConfiguration baseConfig() {
        ServerConfiguration cfg = newServerConfigurationWithAutoPort();
        cfg.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, PAGE_SIZE_BYTES);
        cfg.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, DATA_MEMORY_BYTES);
        // Keep the PK budget large so BLink-internal evictions do not dominate;
        // this test is about data-page evictions on the recovery path.
        cfg.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, 4 * 1024 * 1024L);
        return cfg;
    }

    /**
     * Recovery that replays many transactional commits under eviction pressure
     * must dispatch eviction-driven page unloads through the recovery-scoped
     * batch, and every row must survive the restart.
     */
    @Test
    public void transactionalRecoveryPipelinesPageFlushes() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmpDir").toPath();

        int txnCount = 5;
        int recordsPerTxn = 200;
        int totalRows = txnCount * recordsPerTxn;

        // Phase 1: write rows across several transactions, NO checkpoint, close.
        // DBManager.close() does not force a checkpoint, so the next start
        // replays the whole commit log.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            createTableSpaceAndTable(manager);
            for (int t = 0; t < txnCount; t++) {
                long tx = beginTransaction(manager, "tblspace1");
                for (int i = 0; i < recordsPerTxn; i++) {
                    int row = t * recordsPerTxn + i;
                    executeUpdate(manager,
                            "INSERT INTO tblspace1.t1(k1,v1) values(?,?)",
                            Arrays.asList(String.format("k_%06d", row), PADDED_VALUE),
                            new TransactionContext(tx));
                }
                commitTransaction(manager, "tblspace1", tx);
            }
            assertRowCount(manager, totalRows);
        }

        // Phase 2: restart — recovery replays the transactional commits. The
        // recovery-scoped batch must have absorbed eviction-driven unloads.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10_000);
            TableManager t1Manager = tableManager(manager);

            assertTrue("recovery of transactional commits must dispatch eviction-driven "
                            + "unloads through the recovery-scoped batch: counter="
                            + t1Manager.getParallelUnloadsCount(),
                    t1Manager.getParallelUnloadsCount() > 0);
            assertAllRowsPresent(manager, totalRows);
        }

        // Phase 3: a second restart recovers from the post-recovery checkpoint
        // written in phase 2. If that checkpoint had persisted a keyToPage
        // entry pointing at a page whose async writePage had not completed,
        // this recovery would lose rows or fail.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10_000);
            assertAllRowsPresent(manager, totalRows);
        }
    }

    /**
     * Recovery that replays many non-transactional (autocommit) INSERT entries
     * under eviction pressure must also route eviction-driven unloads through
     * the recovery-scoped batch, and every row must survive the restart.
     */
    @Test
    public void nonTransactionalRecoveryPipelinesPageFlushes() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmpDir").toPath();

        int totalRows = 600;

        // Phase 1: write rows with autocommit, NO checkpoint, close.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            createTableSpaceAndTable(manager);
            for (int i = 0; i < totalRows; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(k1,v1) values(?,?)",
                        Arrays.asList(String.format("k_%06d", i), PADDED_VALUE),
                        NO_TRANSACTION);
            }
            assertRowCount(manager, totalRows);
        }

        // Phase 2: restart — recovery replays the autocommit INSERT entries.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10_000);
            TableManager t1Manager = tableManager(manager);

            assertTrue("recovery of non-transactional DML must dispatch eviction-driven "
                            + "unloads through the recovery-scoped batch: counter="
                            + t1Manager.getParallelUnloadsCount(),
                    t1Manager.getParallelUnloadsCount() > 0);
            assertAllRowsPresent(manager, totalRows);
        }

        // Phase 3: second restart recovers from the post-recovery checkpoint.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10_000);
            assertAllRowsPresent(manager, totalRows);
        }
    }

    /**
     * A recovery workload that mixes transactional commits and autocommit DML,
     * including updates and deletes, must produce the correct durable end
     * state across two restarts.
     */
    @Test
    public void mixedRecoveryWorkloadIsConsistentAcrossRestarts() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmpDir").toPath();

        int base = 400;
        int deletes = 120;
        int extraInserts = 300;
        int expectedRows = (base - deletes) + extraInserts;
        String updatedValue = repeat("u", 200);

        // Phase 1: build a mixed workload, NO checkpoint, close.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, baseConfig(), null)) {
            manager.start();
            createTableSpaceAndTable(manager);

            // base rows via autocommit
            for (int i = 0; i < base; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(k1,v1) values(?,?)",
                        Arrays.asList(String.format("k_%06d", i), PADDED_VALUE),
                        NO_TRANSACTION);
            }
            // update the first 'base' rows inside one transaction
            long updTx = beginTransaction(manager, "tblspace1");
            for (int i = 0; i < base; i++) {
                executeUpdate(manager,
                        "UPDATE tblspace1.t1 set v1=? where k1=?",
                        Arrays.asList(updatedValue, String.format("k_%06d", i)),
                        new TransactionContext(updTx));
            }
            commitTransaction(manager, "tblspace1", updTx);
            // delete a prefix via autocommit
            for (int i = 0; i < deletes; i++) {
                executeUpdate(manager,
                        "DELETE FROM tblspace1.t1 where k1=?",
                        Arrays.asList(String.format("k_%06d", i)),
                        NO_TRANSACTION);
            }
            // extra inserts inside one transaction
            long insTx = beginTransaction(manager, "tblspace1");
            for (int i = 0; i < extraInserts; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(k1,v1) values(?,?)",
                        Arrays.asList(String.format("k_%06d", base + i), PADDED_VALUE),
                        new TransactionContext(insTx));
            }
            commitTransaction(manager, "tblspace1", insTx);
            assertRowCount(manager, expectedRows);
        }

        // Phase 2 + 3: two restarts, each must reproduce the exact end state.
        for (int restart = 0; restart < 2; restart++) {
            try (DBManager manager = new DBManager("localhost",
                    new FileMetadataStorageManager(metadataPath),
                    new FileDataStorageManager(dataPath),
                    new FileCommitLogManager(logsPath),
                    tmpDir, null, baseConfig(), null)) {
                manager.start();
                manager.waitForTablespace("tblspace1", 10_000);
                assertRowCount(manager, expectedRows);
                Map<String, String> rows = readAllRows(manager);
                assertEquals("recovery must reproduce the expected number of rows",
                        expectedRows, rows.size());
                // deleted rows stay deleted
                for (int i = 0; i < deletes; i++) {
                    String key = String.format("k_%06d", i);
                    assertTrue("deleted key " + key + " must stay deleted",
                            !rows.containsKey(key));
                }
                // surviving updated rows keep their updated value
                for (int i = deletes; i < base; i++) {
                    String key = String.format("k_%06d", i);
                    assertEquals("updated key " + key + " must carry the updated value",
                            updatedValue, rows.get(key));
                }
                // extra inserts are present with the original padded value
                for (int i = 0; i < extraInserts; i++) {
                    String key = String.format("k_%06d", base + i);
                    assertEquals("inserted key " + key + " must be present",
                            PADDED_VALUE, rows.get(key));
                }
            }
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
            while (scanner.hasNext()) {
                scanner.next();
                actual++;
            }
            assertEquals("scan returned wrong number of rows", expectedRows, actual);
        }
    }

    /**
     * Asserts that exactly {@code expectedRows} distinct keys named
     * {@code k_000000..k_<expectedRows-1>} are present — i.e. no row was lost
     * and no duplicate/garbage key appeared during recovery.
     */
    private static void assertAllRowsPresent(DBManager manager, int expectedRows) throws Exception {
        Set<String> seen = new HashSet<>();
        try (DataScanner scanner = scan(manager,
                "SELECT k1 FROM tblspace1.t1", Collections.emptyList())) {
            while (scanner.hasNext()) {
                Object[] row = (Object[]) scanner.next().getValues();
                assertNotNull("scan row must contain a key", row);
                seen.add(String.valueOf(row[0]));
            }
        }
        assertEquals("recovery must reproduce exactly the expected number of distinct keys",
                expectedRows, seen.size());
        List<String> missing = new ArrayList<>();
        for (int i = 0; i < expectedRows; i++) {
            String key = String.format("k_%06d", i);
            if (!seen.contains(key)) {
                missing.add(key);
            }
        }
        assertTrue("recovery lost rows: " + missing.subList(0, Math.min(10, missing.size())),
                missing.isEmpty());
    }

    /**
     * Reads the whole table into a {@code key -> value} map via a full scan.
     */
    private static Map<String, String> readAllRows(DBManager manager) throws Exception {
        Map<String, String> rows = new HashMap<>();
        try (DataScanner scanner = scan(manager,
                "SELECT k1, v1 FROM tblspace1.t1", Collections.emptyList())) {
            while (scanner.hasNext()) {
                Object[] row = (Object[]) scanner.next().getValues();
                assertNotNull("scan row must not be null", row);
                rows.put(String.valueOf(row[0]), String.valueOf(row[1]));
            }
        }
        return rows;
    }

    private static String repeat(String fragment, int count) {
        StringBuilder sb = new StringBuilder(fragment.length() * count);
        for (int i = 0; i < count; i++) {
            sb.append(fragment);
        }
        return sb.toString();
    }
}
