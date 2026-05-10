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
package herddb.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.AbstractTableManager;
import herddb.core.DBManager;
import herddb.core.MemoryManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.vector.RemoteVectorIndexService;
import herddb.index.vector.VectorIndexManager;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.remote.RemoteFileDataStorageManager;
import herddb.remote.RemoteFileServer;
import herddb.remote.RemoteFileServiceClient;
import herddb.server.ServerConfiguration;
import herddb.sql.TranslatedQuery;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Issue #471 — end-to-end multi-IS + RemoteFileServer rebuild test.
 *
 * <p>The user explicitly called this scenario out: the rebuild
 * mechanism must work when (a) the table data lives on a shared
 * remote storage (file server or S3) and (b) more than one
 * IndexingService instance reads from that same storage. This test
 * spins up:
 * <ul>
 *   <li>One {@link RemoteFileServer} backed by a temp directory.</li>
 *   <li>One herddb {@link DBManager} backed by a
 *       {@link RemoteFileDataStorageManager} that writes data pages
 *       and table-checkpoints to the file server.</li>
 *   <li>Three {@link IndexingServiceEngine} instances, each with its
 *       own {@code RemoteFileDataStorageManager} pointing at the
 *       <strong>same</strong> file server.</li>
 * </ul>
 *
 * <p>Flow:
 * <ol>
 *   <li>The herddb DBManager creates a tablespace, a table, and
 *       inserts {@code numRecords} rows.</li>
 *   <li>It then runs {@code CREATE VECTOR INDEX} — which exercises
 *       step 2's server-side path: take a pinned checkpoint, mark
 *       the Index with {@code rebuild=true} +
 *       {@code _rebuildLsn=<pinned LSN>}.</li>
 *   <li>The test reads the persisted {@code Index} from the
 *       tablespace manager and observes the rebuild marker is
 *       present.</li>
 *   <li>The three IS engines each receive synthetic CREATE_TABLE +
 *       CREATE_INDEX log entries built from the same Table/Index
 *       objects (mirroring what BookKeeper would deliver in
 *       production). Each engine's {@code triggerRebuildIfNeeded}
 *       fires; each rebuilder's
 *       {@code dataStorageManager.fullTableScan} reads from the
 *       shared file server at the pinned LSN; each engine
 *       back-fills only its shard.</li>
 *   <li>The test asserts the union of the three engines' local
 *       stores covers every staged record exactly once.</li>
 * </ol>
 *
 * @author enrico.olivelli
 */
public class MultiInstanceRebuildRemoteFileServerE2ETest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Generous timeout — the test starts a real RemoteFileServer,
     * runs an INSERT-checkpoint cycle through a real DBManager, and
     * spins up 3 IS engines with 3 separate RemoteFileServiceClient
     * connections. On a constrained CI runner this can take 10-30
     * seconds.
     */
    @Rule
    public Timeout globalTimeout = Timeout.seconds(120);

    private static final int NUM_INSTANCES = 3;
    private static final int NUM_SHARDS = 6;
    private static final int NUM_RECORDS = 60;

    private DBManager buildHerddbServer(Path dataPath, Path logsPath, Path metadataPath,
                                        Path tmpPath, RemoteFileServiceClient client,
                                        herddb.remote.SharedCheckpointMetadataManager shared)
            throws Exception {
        ServerConfiguration config = new ServerConfiguration();
        // JSQLParserPlanner — the CREATE VECTOR INDEX path lives there.
        config.set(ServerConfiguration.PROPERTY_PLANNER_TYPE,
                ServerConfiguration.PLANNER_TYPE_JSQLPARSER);
        RemoteFileDataStorageManager dsm = new RemoteFileDataStorageManager(
                dataPath, tmpPath, 1000, client);
        // Wire the shared metadata manager so tableCheckpoint
        // publishes TableStatus to shared remote storage where the
        // IS engines can read it via the fallback path.
        dsm.setSharedCheckpointMetadataManager(shared);
        DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                dsm,
                new FileCommitLogManager(logsPath),
                tmpPath, null, config, null);
        // Inline RemoteVectorIndexService stub — herddb-core's
        // MockRemoteVectorIndexService is not visible from
        // herddb-indexing-service test scope (no test-jar dep), so we
        // wire a do-nothing stub: search returns empty, status is a
        // valid sentinel, retention floor is empty (= no constraint).
        // The test never calls VectorIndexManager.search; the stub
        // exists only to satisfy bootIndex's "RemoteVectorIndexService
        // is required" precondition.
        manager.setRemoteVectorIndexService(new InlineRemoteVectorIndexServiceStub());
        return manager;
    }

    private static final class InlineRemoteVectorIndexServiceStub
            implements RemoteVectorIndexService {
        @Override
        public List<Map.Entry<Bytes, Float>> search(String tablespace, String table,
                                                     String index, float[] vector, int topK) {
            return Collections.emptyList();
        }

        @Override
        public IndexStatusInfo getIndexStatus(String tablespace, String table, String index) {
            return new IndexStatusInfo(0, 0,
                    0L, 0L, 0L, 0L, 0L, 0L, "stub", 0, 0);
        }

        @Override
        public boolean waitForCatchUp(String tablespace, LogSequenceNumber sequenceNumber,
                                      long timeoutMs) {
            return true;
        }

        @Override
        public java.util.Optional<LogSequenceNumber> getMinProcessedLsn(String tablespace) {
            return java.util.Optional.empty();
        }

        @Override
        public void dropIndex(String tablespace, String table, String indexName) {
            // no-op in stub — eager cleanup not exercised by this test
        }

        @Override
        public void close() {
        }
    }

    /**
     * Inline replica of {@code TestUtils.execute(DBManager, String, List)}.
     * herddb-core's TestUtils is not visible from this module's test
     * scope (no test-jar dep), so the few helpers we need are inlined.
     */
    private void execute(DBManager manager, String query, List<Object> parameters)
            throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(
                TableSpace.DEFAULT, query, parameters, true, true, false, -1);
        manager.executePlan(translated.plan, translated.context,
                TransactionContext.NO_TRANSACTION);
    }

    private IndexingServiceEngine buildIsEngine(int instanceId, int numInstances,
                                                RemoteFileServiceClient client,
                                                Path metadata, Path tmpDir,
                                                Path logDir, Path dataDir,
                                                herddb.remote.SharedCheckpointMetadataManager shared)
            throws Exception {
        Properties props = new Properties();
        // Use storage.type=memory so the engine does not overwrite our
        // explicitly-set DSM in start(). The IS-side rebuild reads from
        // the wired DSM, which IS the RemoteFileDataStorageManager
        // pointing at the shared file server.
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_INSTANCE_ID,
                String.valueOf(instanceId));
        props.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES,
                String.valueOf(numInstances));
        IndexingServerConfiguration cfg = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, cfg);
        MemoryMetadataStorageManager mem = new MemoryMetadataStorageManager();
        mem.start();
        mem.ensureDefaultTableSpace("local", "local", 0, 1);
        engine.setMetadataStorageManager(mem);
        RemoteFileDataStorageManager dsm = new RemoteFileDataStorageManager(
                metadata, tmpDir, 1000, client);
        // Wire the shared metadata manager so the IS engine's
        // getTableStatus falls back to shared remote storage when its
        // own local metadata directory does not have the file (issue
        // #471 fix). Without this, a fresh IS engine cannot scan a
        // table at the rebuild LSN.
        dsm.setSharedCheckpointMetadataManager(shared);
        engine.setDataStorageManager(dsm);
        engine.setMemoryManager(new MemoryManager(
                64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024));
        engine.start();
        return engine;
    }

    /**
     * <b>Production-grade metadata propagation</b>: each DSM has its
     * OWN local metadata directory (just like a real deployment),
     * but every DSM is wired to the same
     * {@link SharedCheckpointMetadataManager} so the herddb leader's
     * {@code tableCheckpoint(true)} call publishes the
     * {@code TableStatus} to shared remote storage, and each IS
     * engine's {@code getTableStatus} call falls back to that shared
     * storage when its own local metadata directory does not have
     * the file. Without this fallback (issue #471 fix), a fresh IS
     * engine cannot scan a table at the rebuild LSN because the
     * herddb leader is the only writer of local metadata.
     */
    @Test
    public void threeInstances_rebuildFromSharedRemoteFileServer_eachOwnsItsShard()
            throws Exception {
        Path fileServerData = folder.newFolder("file-server").toPath();
        Path serverData = folder.newFolder("server-data").toPath();
        Path serverLogs = folder.newFolder("server-logs").toPath();
        Path serverMeta = folder.newFolder("server-meta").toPath();
        Path serverTmp = folder.newFolder("server-tmp").toPath();

        try (RemoteFileServer fileServer = new RemoteFileServer(
                "localhost", 0, fileServerData, 4)) {
            fileServer.start();
            String fileServerAddr = "localhost:" + fileServer.getPort();

            // The herddb server-side client + shared metadata
            // manager. The shared manager publishes TableStatus
            // writes to the file server so IS engines (separate
            // local-metadata directories) can read them via the
            // RemoteFileDataStorageManager.getTableStatus fallback
            // path added by issue #471.
            try (RemoteFileServiceClient serverFsClient = new RemoteFileServiceClient(
                    Collections.singletonList(fileServerAddr))) {
                herddb.remote.SharedCheckpointMetadataManager sharedMeta =
                        new herddb.remote.SharedCheckpointMetadataManager(serverFsClient);
                try (DBManager manager = buildHerddbServer(serverData, serverLogs, serverMeta,
                        serverTmp, serverFsClient, sharedMeta)) {
                manager.start();

                CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                        "tblspace1", Collections.singleton("localhost"),
                        "localhost", 1, 0, 0);
                manager.executeStatement(st1,
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);
                manager.waitForTablespace("tblspace1", 30000);

                execute(manager,
                        "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                        Collections.emptyList());

                // Insert NUM_RECORDS rows.
                for (int i = 0; i < NUM_RECORDS; i++) {
                    execute(manager,
                            "INSERT INTO tblspace1.t1 (id, vec) VALUES (?, ?)",
                            Arrays.asList(i, new float[]{i * 0.1f, i * 0.2f, i * 0.3f}));
                }

                // CREATE VECTOR INDEX with sharding — this exercises
                // step 2's server-side flow: pin the table checkpoint,
                // mark the Index with rebuild=true + _rebuildLsn.
                execute(manager,
                        "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH numShards="
                                + NUM_SHARDS,
                        Collections.emptyList());

                // Read back the persisted Index + Table.
                AbstractTableManager tableManager = manager.getTableSpaceManager("tblspace1")
                        .getTableManager("t1");
                Table tableObj = tableManager.getTable();
                Index indexObj = manager.getTableSpaceManager("tblspace1")
                        .getIndexesOnTable("t1").get("vidx").getIndex();
                String tableSpaceUuid = manager.getTableSpaceManager("tblspace1")
                        .getTableSpaceUUID();

                // Sanity: the Index must carry the rebuild markers
                // (this is the contract step 2 establishes).
                assertEquals("non-empty table must produce rebuild=true",
                        "true", indexObj.properties.get(VectorIndexManager.PROP_REBUILD));
                assertNotNull("non-empty table must produce _rebuildLsn",
                        indexObj.properties.get(VectorIndexManager.PROP_REBUILD_LSN));

                // Spin up NUM_INSTANCES IS engines, each with its own
                // RemoteFileServiceClient pointing at the SAME file
                // server. Each engine's DSM reads from the shared
                // remote storage when its rebuilder runs.
                List<IndexingServiceEngine> engines = new ArrayList<>();
                List<RemoteFileServiceClient> isClients = new ArrayList<>();
                try {
                    for (int i = 0; i < NUM_INSTANCES; i++) {
                        RemoteFileServiceClient isClient = new RemoteFileServiceClient(
                                Collections.singletonList(fileServerAddr));
                        isClients.add(isClient);
                        // Each IS engine has its OWN local metadata
                        // directory — no sharing. The
                        // SharedCheckpointMetadataManager fallback
                        // wired via setSharedCheckpointMetadataManager
                        // is what makes the IS-side getTableStatus
                        // see the herddb leader's TableStatus.
                        Path isMeta = folder.newFolder("is-meta-" + i).toPath();
                        Path isTmp = folder.newFolder("is-tmp-" + i).toPath();
                        Path isLog = folder.newFolder("is-log-" + i).toPath();
                        Path isData = folder.newFolder("is-data-" + i).toPath();
                        IndexingServiceEngine engine = buildIsEngine(i, NUM_INSTANCES,
                                isClient, isMeta, isTmp, isLog, isData, sharedMeta);
                        engines.add(engine);

                        // Each engine MUST share the herddb server's
                        // tablespace UUID so its DSM lookups hit the same
                        // remote-storage path. We override the engine's
                        // tablespaceUUID via reflection — the
                        // MetadataStorageManager-driven path would
                        // otherwise pick a different UUID.
                        java.lang.reflect.Field tsField =
                                IndexingServiceEngine.class.getDeclaredField("tableSpaceUUID");
                        tsField.setAccessible(true);
                        tsField.set(engine, tableSpaceUuid);

                        // Apply the SCHEMA log entries through
                        // processEntryForTest so the production tailer
                        // code path is exercised (classifyForMetrics,
                        // lastProcessedLsn advancement, applyEntry
                        // switch). Each engine's
                        // triggerRebuildIfNeeded reads the
                        // _rebuildLsn from the index properties and
                        // calls dataStorageManager.fullTableScan
                        // against the shared file server.
                        engine.processEntryForTest(new LogSequenceNumber(1, 1),
                                LogEntryFactory.createTable(tableObj, null));
                        engine.processEntryForTest(new LogSequenceNumber(1, 2),
                                LogEntryFactory.createIndex(indexObj, null));
                    }

                    // Each engine must have indexed a non-empty proper
                    // subset, and the union of all engines must cover
                    // every staged record exactly once with no
                    // duplicates.
                    Set<Bytes> unionOfLocalKeys = new HashSet<>();
                    long totalIndexed = 0;
                    float[] queryVector = new float[]{0f, 0f, 0f};
                    for (int i = 0; i < NUM_INSTANCES; i++) {
                        IndexingServiceEngine engine = engines.get(i);
                        long indexed = engine.getRebuildMetricsForTest()
                                .recordsIndexed.sum();
                        assertTrue("instance " + i + " must own SOME records (indexed="
                                        + indexed + ")",
                                indexed > 0);
                        assertTrue("instance " + i + " must own a SHARD subset only "
                                        + "(indexed=" + indexed + ", total=" + NUM_RECORDS + ")",
                                indexed < NUM_RECORDS);
                        totalIndexed += indexed;

                        List<Map.Entry<Bytes, Float>> hits = engine.search(
                                "default", "t1", "vidx", queryVector, NUM_RECORDS);
                        for (Map.Entry<Bytes, Float> hit : hits) {
                            assertTrue("key " + hit.getKey() + " must not appear in two "
                                            + "instances",
                                    unionOfLocalKeys.add(hit.getKey()));
                        }
                    }
                    assertEquals("union of local keys must cover every staged record",
                            NUM_RECORDS, unionOfLocalKeys.size());
                    assertEquals("sum of recordsIndexed across engines must equal total",
                            NUM_RECORDS, totalIndexed);
                } finally {
                    for (IndexingServiceEngine e : engines) {
                        try {
                            e.close();
                        } catch (Exception ignore) {
                            // The test's correctness assertions ran
                            // BEFORE this finally; close() failures
                            // here are diagnostic-only and would
                            // otherwise mask real failures upstream.
                        }
                    }
                    for (RemoteFileServiceClient c : isClients) {
                        try {
                            c.close();
                        } catch (Exception ignore) {
                        }
                    }
                }
                }
            }
        }
    }
}
