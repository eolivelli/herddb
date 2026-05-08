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
import static org.junit.Assert.assertFalse;
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
            assertEquals("hook must carry tablespace.table.index identity",
                    "tblspace1.t1.vidx", lastArgs.toString());
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

            SystemInstrumentation.addListener(
                    new SystemInstrumentation.SingleInstrumentationPointListener(
                            "createVectorIndex.checkpointTaken") {
                @Override
                public void acceptSingle(Object... args) {
                    boolean indexAlreadyPresent = manager.getTableSpaceManager("tblspace1")
                            .getIndexesOnTable("t1") != null
                            && manager.getTableSpaceManager("tblspace1")
                                    .getIndexesOnTable("t1").containsKey("vidx");
                    orderingObservation.set(indexAlreadyPresent ? 2 : 1);
                }
            });

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            assertEquals(
                    "checkpoint hook must fire BEFORE the CREATE_INDEX entry is applied "
                            + "(observed=" + orderingObservation.get() + ")",
                    1, orderingObservation.get());
            // Sanity: the CREATE INDEX did succeed and the index is now present.
            assertTrue("index must be visible after CREATE INDEX returns",
                    manager.getTableSpaceManager("tblspace1")
                            .getIndexesOnTable("t1").containsKey("vidx"));
            assertFalse("table must still exist",
                    manager.getTableSpaceManager("tblspace1")
                            .getIndexesOnTable("t1").isEmpty());
        }
    }
}
