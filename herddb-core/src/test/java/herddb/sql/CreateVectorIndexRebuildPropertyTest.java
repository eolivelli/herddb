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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import herddb.core.DBManager;
import herddb.core.indexes.MockRemoteVectorIndexService;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.vector.VectorIndexManager;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Server-side coverage for the issue #471 {@code rebuild=true} marking on the
 * {@code Index} produced by {@code CREATE VECTOR INDEX}.
 *
 * <p>The marking must fire only when (a) the new index is a vector index AND
 * (b) the underlying table already has rows. For empty tables the property
 * is never set; for non-vector index types (hash, brin) the property is
 * never set even on non-empty tables, because their existing in-process
 * rebuild path on {@code bootIndex} / {@code scanForIndexRebuild} already
 * back-fills the data.
 *
 * @author enrico.olivelli
 */
public class CreateVectorIndexRebuildPropertyTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private DBManager buildManager(Path dataPath, Path logsPath, Path metadataPath, Path tmpDir)
            throws Exception {
        ServerConfiguration config = new ServerConfiguration();
        // JSQLParserPlanner is the only planner that handles CREATE VECTOR INDEX.
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
                "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null, n int)",
                Collections.emptyList());
    }

    private void insertSomeRows(DBManager manager, int rows) throws Exception {
        for (int i = 0; i < rows; i++) {
            execute(manager,
                    "INSERT INTO tblspace1.t1 (id, vec, n) VALUES (?, ?, ?)",
                    Arrays.asList(i, new float[]{i * 0.1f, i * 0.2f, i * 0.3f}, i));
        }
    }

    private Index getCreatedIndex(DBManager manager, String table, String indexName) {
        return manager.getTableSpaceManager("tblspace1")
                .getIndexesOnTable(table).get(indexName).getIndex();
    }

    @Test
    public void emptyTable_vectorIndex_doesNotSetRebuildProperty() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            // No INSERTs — the table is empty before CREATE INDEX.

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH m=16 numShards=4",
                    Collections.emptyList());

            Index idx = getCreatedIndex(manager, "t1", "vidx");
            assertNotNull("vector index must exist", idx);
            assertEquals(Index.TYPE_VECTOR, idx.type);
            assertNull("empty table must NOT receive rebuild=true",
                    idx.properties.get(VectorIndexManager.PROP_REBUILD));
            assertFalse("empty table must NOT receive rebuild=true (key absent)",
                    idx.properties.containsKey(VectorIndexManager.PROP_REBUILD));
        }
    }

    @Test
    public void nonEmptyTable_vectorIndex_setsRebuildPropertyTrue() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 10);

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH m=16 numShards=4",
                    Collections.emptyList());

            Index idx = getCreatedIndex(manager, "t1", "vidx");
            assertNotNull("vector index must exist", idx);
            assertEquals(Index.TYPE_VECTOR, idx.type);
            assertEquals("non-empty table MUST receive rebuild=true",
                    "true", idx.properties.get(VectorIndexManager.PROP_REBUILD));
            // The user-supplied properties must be preserved alongside the
            // auto-injected rebuild marker.
            assertEquals("16", idx.properties.get(VectorIndexManager.PROP_M));
            assertEquals("4", idx.properties.get(VectorIndexManager.PROP_NUM_SHARDS));
        }
    }

    @Test
    public void nonEmptyTable_hashIndex_doesNotSetRebuildProperty() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 5);

            execute(manager,
                    "CREATE HASH INDEX hidx ON tblspace1.t1(n)",
                    Collections.emptyList());

            Index idx = getCreatedIndex(manager, "t1", "hidx");
            assertNotNull("hash index must exist", idx);
            assertEquals(Index.TYPE_HASH, idx.type);
            assertNull("non-vector index must NOT receive rebuild=true",
                    idx.properties.get(VectorIndexManager.PROP_REBUILD));
            assertFalse("non-vector index must NOT receive rebuild=true (key absent)",
                    idx.properties.containsKey(VectorIndexManager.PROP_REBUILD));
        }
    }

    @Test
    public void nonEmptyTable_brinIndex_doesNotSetRebuildProperty() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 5);

            execute(manager,
                    "CREATE BRIN INDEX bidx ON tblspace1.t1(n)",
                    Collections.emptyList());

            Index idx = getCreatedIndex(manager, "t1", "bidx");
            assertNotNull("brin index must exist", idx);
            assertEquals(Index.TYPE_BRIN, idx.type);
            assertNull("brin index must NOT receive rebuild=true",
                    idx.properties.get(VectorIndexManager.PROP_REBUILD));
            assertFalse("brin index must NOT receive rebuild=true (key absent)",
                    idx.properties.containsKey(VectorIndexManager.PROP_REBUILD));
        }
    }

    @Test
    public void nonEmptyTable_vectorIndex_rebuildMarkerSurvivesSerialization() throws Exception {
        // The CREATE_INDEX log entry serialises the Index — the rebuild=true
        // marker must round-trip through serialize/deserialize so the
        // IndexingService observes it on the live tailer path.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 3);

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH numShards=2",
                    Collections.emptyList());

            Index idx = getCreatedIndex(manager, "t1", "vidx");
            byte[] serialized = idx.serialize();
            Index deserialized = Index.deserialize(serialized);
            assertEquals("rebuild=true must survive Index serialize/deserialize",
                    "true", deserialized.properties.get(VectorIndexManager.PROP_REBUILD));
            assertEquals("2", deserialized.properties.get(VectorIndexManager.PROP_NUM_SHARDS));
        }
    }
}
