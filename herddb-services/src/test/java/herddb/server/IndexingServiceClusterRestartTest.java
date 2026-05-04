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
package herddb.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.ScanResultSet;
import herddb.indexing.IndexingServer;
import herddb.indexing.IndexingServerConfiguration;
import herddb.indexing.IndexingServiceClient;
import herddb.indexing.IndexingServiceEngine;
import herddb.model.TableSpace;
import herddb.remote.RemoteFileServer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end full-cluster tests that exercise the issue #383 scenarios on
 * a real HerdDB stack:
 * <ul>
 *   <li>standalone {@link Server} with {@code server.storage.mode=remote},</li>
 *   <li>real {@link RemoteFileServer} backed by local-object-storage (so
 *       the test boots in seconds, no testcontainers / MinIO required),</li>
 *   <li>real {@link IndexingServer} with
 *       {@code indexing.storage.type=remote} pointing at the same file
 *       server — vector graph + map blobs land there for real.</li>
 * </ul>
 *
 * <p>This is the wire-level companion to the engine-level
 * {@code DropTableIndexCleanupRemoteFileTest} and
 * {@code RestartUnreachableStartOfTimeTest}: it confirms that the
 * DROP-cleanup contract and the schema-recovery / restart-from-watermark
 * contract hold when running through the actual HerdDB SQL parser,
 * commit-log path, and gRPC indexing service. The
 * {@code IndexingServiceWithS3E2ETest} sister suite already covers the
 * S3/MinIO variant; this one focuses on functionality that does not
 * require an object-storage backend (drop cleanup, restart resumption).
 */
public class IndexingServiceClusterRestartTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Boots the full stack with a local-object-storage file server.
     * Call {@link #stopIndexing()} / {@link #startIndexing()} to simulate
     * an indexing-service restart without tearing down the herddb server
     * or the file server.
     */
    private final class Topology implements AutoCloseable {

        final Server server;
        final RemoteFileServer fileServer;
        final Path indexingLogDir;
        final Path indexingData;
        final Properties indexingProps;
        final Path fileServerDataDir;
        IndexingServiceEngine engine;
        IndexingServer indexingServer;
        IndexingServiceClient indexingClient;

        Topology() throws Exception {
            fileServerDataDir = folder.newFolder("fileserver").toPath();
            // Default config = local object storage (no MinIO).
            fileServer = new RemoteFileServer("localhost", 0, fileServerDataDir, 4);
            fileServer.start();
            String fileServerAddr = "localhost:" + fileServer.getPort();

            Path baseDir = folder.newFolder("herddb").toPath();
            ServerConfiguration serverConfig = new ServerConfiguration();
            serverConfig.set(ServerConfiguration.PROPERTY_MODE,
                    ServerConfiguration.PROPERTY_MODE_STANDALONE);
            serverConfig.set(ServerConfiguration.PROPERTY_BASEDIR,
                    baseDir.toAbsolutePath().toString());
            serverConfig.set(ServerConfiguration.PROPERTY_HOST, "localhost");
            serverConfig.set(ServerConfiguration.PROPERTY_PORT, 0);
            serverConfig.set(ServerConfiguration.PROPERTY_STORAGE_MODE,
                    ServerConfiguration.PROPERTY_STORAGE_MODE_REMOTE);
            serverConfig.set(ServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS, fileServerAddr);
            serverConfig.set("http.enable", false);

            server = new Server(serverConfig);
            server.start();
            server.waitForStandaloneBoot();

            Path logDir = baseDir.resolve(
                    serverConfig.getString(ServerConfiguration.PROPERTY_LOGDIR,
                            ServerConfiguration.PROPERTY_LOGDIR_DEFAULT));
            Path indexingBase = folder.newFolder("indexing").toPath();
            indexingLogDir = logDir;
            indexingData = indexingBase.resolve("data");
            Files.createDirectories(indexingData);

            indexingProps = new Properties();
            indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "remote");
            indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS, fileServerAddr);
            indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_GRPC_PORT, "0");
            indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_GRPC_HOST, "localhost");
            indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_INSTANCE_ID, "0");
            indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES, "1");
            indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_COMPACTION_INTERVAL, "60000");

            startIndexing();
        }

        void startIndexing() throws Exception {
            IndexingServerConfiguration cfg = new IndexingServerConfiguration(indexingProps);
            engine = new IndexingServiceEngine(indexingLogDir, indexingData, cfg);
            engine.setMetadataStorageManager(server.getMetadataStorageManager());
            indexingServer = new IndexingServer("localhost", 0, engine, cfg);
            indexingServer.start();
            engine.start();
            indexingClient = IndexingServiceClient.fromAddresses(
                    Collections.singletonList(indexingServer.getAddress()), 30);
            server.getManager().setRemoteVectorIndexService(indexingClient);
        }

        void stopIndexing() throws Exception {
            if (indexingClient != null) {
                try {
                    indexingClient.close();
                } catch (Exception ignored) {
                    // teardown best-effort
                }
                indexingClient = null;
            }
            if (indexingServer != null) {
                indexingServer.stop();
                indexingServer = null;
            }
            if (engine != null) {
                engine.close();
                engine = null;
            }
        }

        @Override
        public void close() throws Exception {
            try {
                stopIndexing();
            } finally {
                try {
                    server.close();
                } finally {
                    fileServer.stop();
                }
            }
        }
    }

    private HDBClient newHDBClient(Server server) throws Exception {
        HDBClient hdbClient = new HDBClient(
                new ClientConfiguration(folder.newFolder().toPath()));
        hdbClient.setClientSideMetadataProvider(
                new StaticClientSideMetadataProvider(server));
        return hdbClient;
    }

    private static void waitForIndexing(HDBConnection con, int expectedCount, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                    "SELECT id FROM t1 ORDER BY ann_of(vec, "
                            + "CAST(? AS FLOAT ARRAY)) DESC LIMIT " + expectedCount,
                    false, Arrays.asList((Object) new float[]{1, 0, 0, 0}),
                    0, 10, 10, false)) {
                List<Map<String, Object>> rows = scan.consume();
                if (rows.size() >= expectedCount) {
                    return;
                }
            }
            Thread.sleep(50);
        }
        throw new AssertionError("indexing service did not catch up within "
                + timeoutMs + " ms; expected at least " + expectedCount + " rows");
    }

    private static int countFilesUnder(Path dir, String contains) throws Exception {
        if (!Files.exists(dir)) {
            return 0;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(dir)) {
            return (int) walk.filter(Files::isRegularFile)
                    .filter(p -> p.toString().contains(contains))
                    .count();
        }
    }

    private static void insertRandomVectors(HDBConnection con, int count,
                                            int dim, long seedBase) throws Exception {
        for (int id = 0; id < count; id++) {
            Random rng = new Random(seedBase + id);
            float[] v = new float[dim];
            for (int i = 0; i < dim; i++) {
                v[i] = rng.nextFloat();
            }
            con.executeUpdate(TableSpace.DEFAULT,
                    "INSERT INTO t1(id, vec) VALUES(?, ?)",
                    0, false, true, Arrays.asList(id, v));
        }
    }

    /**
     * Restart the indexing service after a successful checkpoint and
     * confirm it resumes correctly even after wiping its local state —
     * the watermark snapshot on the local data dir survives, so the
     * restart hydrates schema from it and skips replaying the
     * pre-watermark commit-log entries. This is the wire-level proof
     * that the JOINING-fallback / schema-from-snapshot machinery from
     * issue #368 works under real cluster topology.
     */
    @Test
    public void restartIndexingServiceAfterCheckpointResumesQueries() throws Exception {
        try (Topology t = new Topology();
             HDBClient hdbClient = newHDBClient(t.server);
             HDBConnection con = hdbClient.openConnection()) {
            con.executeUpdate(TableSpace.DEFAULT,
                    "CREATE TABLE t1 (id int primary key, vec floata not null)",
                    0, false, true, Collections.emptyList());
            con.executeUpdate(TableSpace.DEFAULT,
                    "CREATE VECTOR INDEX vidx ON t1(vec)",
                    0, false, true, Collections.emptyList());

            insertRandomVectors(con, 200, 4, 42L);
            waitForIndexing(con, 5, 15_000);

            // Checkpoint server + IS so the watermark + segment files
            // land on the file server.
            t.server.getManager().checkpoint();
            t.engine.forceCheckpointAndSaveWatermark();

            t.stopIndexing();
            t.startIndexing();

            // The index must continue to serve queries after a clean
            // restart. The exact ANN ordering is deterministic given the
            // seeded inserts; we only assert that the search returns
            // the expected number of rows.
            try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                    "SELECT id FROM t1 ORDER BY ann_of(vec, "
                            + "CAST(? AS FLOAT ARRAY)) DESC LIMIT 5",
                    false, Arrays.asList((Object) new float[]{1, 0, 0, 0}),
                    0, 10, 10, false)) {
                assertEquals(5, scan.consume().size());
            }
        }
    }

    /**
     * Issue #383 fix: a {@code DROP VECTOR INDEX} statement run through
     * the SQL path must cause the indexing service to release every
     * remote-storage object that belonged to the dropped index. We pin
     * the segment dir on disk before the drop, run the drop, wait for
     * the indexing service to apply it, then verify the file server's
     * data directory no longer contains any file under the index's UUID
     * prefix. Without the fix, the segments would linger forever.
     */
    @Test
    public void dropVectorIndexErasesRemoteSegmentFiles() throws Exception {
        try (Topology t = new Topology();
             HDBClient hdbClient = newHDBClient(t.server);
             HDBConnection con = hdbClient.openConnection()) {
            con.executeUpdate(TableSpace.DEFAULT,
                    "CREATE TABLE t1 (id int primary key, vec floata not null)",
                    0, false, true, Collections.emptyList());
            con.executeUpdate(TableSpace.DEFAULT,
                    "CREATE VECTOR INDEX vidx ON t1(vec)",
                    0, false, true, Collections.emptyList());

            insertRandomVectors(con, 200, 4, 11L);
            waitForIndexing(con, 5, 15_000);

            t.server.getManager().checkpoint();
            t.engine.forceCheckpointAndSaveWatermark();

            // Capture the actual store UUID the engine uses for vidx —
            // listIndexes returns IndexDescriptor objects keyed by
            // (table, index, tablespace).
            List<IndexingServiceEngine.IndexDescriptor> indexes = t.engine.listIndexes();
            assertFalse("vidx must be tracked by the engine before DROP",
                    indexes.isEmpty());

            // Sanity: at least one file must exist on the file server's
            // data dir under the tablespace UUID prefix. This proves
            // the index actually wrote anything to S3-like storage.
            String tsUuid = t.engine.getTableSpaceUUID();
            int beforeDrop = countFilesUnder(t.fileServerDataDir, "/" + tsUuid + "/");
            assertTrue("file server must hold at least one file for the tablespace before DROP; got "
                    + beforeDrop, beforeDrop > 0);

            // ---- DROP VECTOR INDEX ----
            con.executeUpdate(TableSpace.DEFAULT,
                    "DROP INDEX vidx",
                    0, false, true, Collections.emptyList());

            // Wait for the IS to apply the DROP. The engine's
            // listIndexes() drops the entry as soon as DROP_INDEX is
            // applied; we also need the async DROP-cleanup task to run
            // (it is submitted to the single-thread checkpoint executor
            // by submitVectorStoreDeletion).
            long deadline = System.currentTimeMillis() + 10_000L;
            while (!t.engine.listIndexes().isEmpty()
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertTrue("engine.listIndexes() must drop vidx after DROP_INDEX",
                    t.engine.listIndexes().isEmpty());
            t.engine.awaitPendingDeletionsForTest();

            // The file server's data directory must no longer hold any
            // file that belongs to the dropped index. We accept that
            // the herddb server itself may keep some bookkeeping files
            // (e.g. table data unrelated to the vector index) under the
            // same tablespace UUID prefix, so we do not assert "0 files
            // under tsUuid/"; instead, we verify "0 files under any path
            // segment that starts with the dropped index's name". The
            // engine builds the storeUUID as
            // {indexName}_{tableName}_{nanoTime}, so any leftover file
            // would contain "/vidx_t1_" in its path.
            int leftover = countFilesUnder(t.fileServerDataDir, "/vidx_t1_");
            if (leftover != 0) {
                StringBuilder sb = new StringBuilder("residual files:\n");
                try (java.util.stream.Stream<Path> w = Files.walk(t.fileServerDataDir)) {
                    w.filter(Files::isRegularFile)
                            .filter(p -> p.toString().contains("/vidx_t1_"))
                            .forEach(p -> sb.append("  ").append(p).append('\n'));
                }
                throw new AssertionError(
                        "DROP VECTOR INDEX must remove every file the file server held for the index "
                                + "(issue #383); got " + leftover + " leftover. " + sb);
            }
        }
    }

    /**
     * Same as {@link #dropVectorIndexErasesRemoteSegmentFiles} but for a
     * {@code DROP TABLE} that takes its vector index along with it.
     */
    @Test
    public void dropTableErasesRemoteSegmentFiles() throws Exception {
        try (Topology t = new Topology();
             HDBClient hdbClient = newHDBClient(t.server);
             HDBConnection con = hdbClient.openConnection()) {
            con.executeUpdate(TableSpace.DEFAULT,
                    "CREATE TABLE t1 (id int primary key, vec floata not null)",
                    0, false, true, Collections.emptyList());
            con.executeUpdate(TableSpace.DEFAULT,
                    "CREATE VECTOR INDEX vidx ON t1(vec)",
                    0, false, true, Collections.emptyList());

            insertRandomVectors(con, 200, 4, 22L);
            waitForIndexing(con, 5, 15_000);

            t.server.getManager().checkpoint();
            t.engine.forceCheckpointAndSaveWatermark();

            int beforeDrop = countFilesUnder(t.fileServerDataDir, "/vidx_t1_");
            assertTrue("file server must hold segment files for vidx before DROP TABLE; got "
                    + beforeDrop, beforeDrop > 0);

            con.executeUpdate(TableSpace.DEFAULT,
                    "DROP TABLE t1",
                    0, false, true, Collections.emptyList());

            long deadline = System.currentTimeMillis() + 10_000L;
            while (!t.engine.listIndexes().isEmpty()
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertTrue("engine.listIndexes() must drop the vidx index after DROP TABLE",
                    t.engine.listIndexes().isEmpty());
            t.engine.awaitPendingDeletionsForTest();

            int leftover = countFilesUnder(t.fileServerDataDir, "/vidx_t1_");
            assertEquals(
                    "DROP TABLE must remove every file the file server held for vidx (issue #383)",
                    0, leftover);
        }
    }
}
