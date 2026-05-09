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
package herddb.sql;

import static herddb.core.TestUtils.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.core.indexes.MockRemoteVectorIndexService;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import herddb.utils.SystemInstrumentation;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Verifies that {@link herddb.core.TableSpaceManager#createIndex} takes a
 * synchronous single-table checkpoint BEFORE writing the {@code CREATE_INDEX}
 * log entry when the new index is a vector index on a non-empty table
 * (issue #471).
 *
 * <p>The test installs a {@link SystemInstrumentation} listener on the
 * {@code "createVectorIndex.checkpointTaken"} hook that the production code
 * fires immediately after the {@code tableManager.checkpoint(false)} call.
 * The hook records the order of events relative to the
 * {@code "tablespace.applyDdl"} hook fired by the existing apply pipeline,
 * so the test pins a correctness contract: <strong>checkpoint must run
 * before the CREATE_INDEX entry is applied</strong>. Without that ordering
 * the IndexingService would scan the table at an LSN earlier than the
 * CREATE_INDEX LSN and miss DML that was applied just before the index
 * was created.
 *
 * @author enrico.olivelli
 */
public class CreateVectorIndexCheckpointTriggerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Class-wide timeout (60 s) — see {@link CreateVectorIndexRebuildPropertyTest}
     * for the rationale: a tablespace-lock leak would otherwise hang the
     * suite indefinitely on a regression.
     */
    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    @After
    public void clearInstrumentation() {
        // Make sure no listener leaks into a downstream test if this one
        // fails partway through.
        SystemInstrumentation.clear();
    }

    private DBManager buildManager(Path dataPath, Path logsPath, Path metadataPath, Path tmpDir)
            throws Exception {
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_PLANNER_TYPE,
                ServerConfiguration.PLANNER_TYPE_JSQLPARSER);
        DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, config, null);
        manager.setRemoteVectorIndexService(new MockRemoteVectorIndexService());
        return manager;
    }

    private void bootstrapTablespaceAndTable(DBManager manager) throws Exception {
        CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
        manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
        manager.waitForTablespace("tblspace1", 10000);
        execute(manager,
                "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                Collections.emptyList());
    }

    @Test
    public void nonEmptyTable_vectorIndex_firesCheckpointHook() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        AtomicInteger hookHits = new AtomicInteger(0);
        StringBuilder lastArgs = new StringBuilder();

        SystemInstrumentation.addListener(new SystemInstrumentation.SingleInstrumentationPointListener(
                "createVectorIndex.checkpointTaken") {
            @Override
            public void acceptSingle(Object... args) {
                hookHits.incrementAndGet();
                lastArgs.setLength(0);
                lastArgs.append(args[0]).append('.').append(args[1]).append('.').append(args[2]);
                // args[3] is the pinned LogSequenceNumber — verify the
                // hook is wired with the LSN payload that step 3 needs.
                if (args.length >= 4) {
                    lastArgs.append("@").append(args[3]);
                }
            }
        });

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            // Insert a single row so the table is non-empty.
            execute(manager,
                    "INSERT INTO tblspace1.t1 (id, vec) VALUES (?, ?)",
                    Arrays.asList(1, new float[]{1.0f, 2.0f, 3.0f}));

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            assertEquals("checkpoint hook must fire exactly once",
                    1, hookHits.get());
            assertTrue("hook must carry tablespace.table.index@<lsn> identity, got: "
                            + lastArgs.toString(),
                    lastArgs.toString().startsWith("tblspace1.t1.vidx@"));
        }
    }

    @Test
    public void emptyTable_vectorIndex_doesNotFireCheckpointHook() throws Exception {
        // The hook must not fire on the empty-table fast path: there is no
        // back-fill work for the IndexingService to do, so taking a
        // checkpoint would be wasted I/O.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        AtomicInteger hookHits = new AtomicInteger(0);

        SystemInstrumentation.addListener(new SystemInstrumentation.SingleInstrumentationPointListener(
                "createVectorIndex.checkpointTaken") {
            @Override
            public void acceptSingle(Object... args) {
                hookHits.incrementAndGet();
            }
        });

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            // No INSERTs — table is empty before CREATE INDEX.

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            assertEquals("checkpoint hook must NOT fire on empty table",
                    0, hookHits.get());
        }
    }

    @Test
    public void nonEmptyTable_hashIndex_doesNotFireCheckpointHook() throws Exception {
        // The hook must not fire for non-vector indexes — they have an
        // existing in-process rebuild path that does not need the
        // server-side checkpoint we take for VECTOR.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        AtomicInteger hookHits = new AtomicInteger(0);

        SystemInstrumentation.addListener(new SystemInstrumentation.SingleInstrumentationPointListener(
                "createVectorIndex.checkpointTaken") {
            @Override
            public void acceptSingle(Object... args) {
                hookHits.incrementAndGet();
            }
        });

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            // We need a table with a non-vector column to put the hash on.
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager,
                    "CREATE TABLE tblspace1.t1 (id int primary key, n int)",
                    Collections.emptyList());
            execute(manager,
                    "INSERT INTO tblspace1.t1 (id, n) VALUES (?, ?)",
                    Arrays.asList(1, 100));

            execute(manager,
                    "CREATE INDEX hidx ON tblspace1.t1(n)",
                    Collections.emptyList());

            assertEquals("checkpoint hook must NOT fire for hash index",
                    0, hookHits.get());
        }
    }

    @Test
    public void hookFiresBeforeIndexIsAvailableInIndexesMap() throws Exception {
        // Pin the ordering contract: when the hook fires, the index does NOT
        // yet exist in the tablespace's indexes map (the CREATE_INDEX log
        // entry has not been applied). After the CREATE INDEX statement
        // returns, the index is present. This guards against a future
        // refactor that accidentally moves the checkpoint AFTER the apply()
        // call.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        // 1 = "indexes did not contain vidx at hook time" (correct)
        // 2 = "indexes already contained vidx at hook time" (regression)
        AtomicInteger orderingObservation = new AtomicInteger(0);

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            execute(manager,
                    "INSERT INTO tblspace1.t1 (id, vec) VALUES (?, ?)",
                    Arrays.asList(1, new float[]{1.0f, 2.0f, 3.0f}));

            // 99 = "listener body threw" — distinguishable from the two
            // expected outcomes (1 = correct, 2 = regression).
            final java.util.concurrent.atomic.AtomicReference<Throwable> listenerError =
                    new java.util.concurrent.atomic.AtomicReference<>();
            SystemInstrumentation.addListener(
                    new SystemInstrumentation.SingleInstrumentationPointListener(
                            "createVectorIndex.checkpointTaken") {
                @Override
                public void acceptSingle(Object... args) {
                    try {
                        java.util.Map<String, herddb.core.AbstractIndexManager> idxs =
                                manager.getTableSpaceManager("tblspace1")
                                        .getIndexesOnTable("t1");
                        boolean indexAlreadyPresent = idxs != null && idxs.containsKey("vidx");
                        orderingObservation.set(indexAlreadyPresent ? 2 : 1);
                    } catch (Throwable t) {
                        // Wrap and surface — bubbling up out of the
                        // instrumentation point would otherwise abort
                        // CREATE INDEX with an opaque error and the
                        // assertion below would lie about the cause.
                        orderingObservation.set(99);
                        listenerError.set(t);
                    }
                }
            });

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            if (orderingObservation.get() == 99) {
                throw new AssertionError(
                        "listener body threw — diagnostic before the ordering assertion",
                        listenerError.get());
            }
            assertEquals(
                    "checkpoint hook must fire BEFORE the CREATE_INDEX entry is applied "
                            + "(observed=" + orderingObservation.get() + ")",
                    1, orderingObservation.get());
            // Sanity: after CREATE INDEX returns, the index IS visible.
            assertTrue("index vidx must be visible after CREATE INDEX returns",
                    manager.getTableSpaceManager("tblspace1")
                            .getIndexesOnTable("t1").containsKey("vidx"));
        }
    }

    @Test
    public void twoBackToBackVectorIndexes_eachFiresOwnCheckpointHook() throws Exception {
        // Two CREATE VECTOR INDEX statements back-to-back on the same
        // non-empty table must each take their own checkpoint and fire
        // the hook independently — there is no caching that would
        // suppress the second hook firing.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        AtomicInteger hookHits = new AtomicInteger(0);

        SystemInstrumentation.addListener(new SystemInstrumentation.SingleInstrumentationPointListener(
                "createVectorIndex.checkpointTaken") {
            @Override
            public void acceptSingle(Object... args) {
                hookHits.incrementAndGet();
            }
        });

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            // Two vector columns so we can register two distinct vector
            // indexes on the same table.
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"),
                    "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager,
                    "CREATE TABLE tblspace1.t1 (id int primary key, "
                            + "vec1 floata not null, vec2 floata not null)",
                    Collections.emptyList());
            for (int i = 0; i < 4; i++) {
                execute(manager,
                        "INSERT INTO tblspace1.t1 (id, vec1, vec2) VALUES (?, ?, ?)",
                        Arrays.asList(i,
                                new float[]{i * 0.1f, 0f, 0f},
                                new float[]{0f, i * 0.2f, 0f}));
            }

            execute(manager,
                    "CREATE VECTOR INDEX v1 ON tblspace1.t1(vec1)",
                    Collections.emptyList());
            execute(manager,
                    "CREATE VECTOR INDEX v2 ON tblspace1.t1(vec2)",
                    Collections.emptyList());

            assertEquals("hook must fire twice — once per CREATE VECTOR INDEX",
                    2, hookHits.get());
        }
    }
}
