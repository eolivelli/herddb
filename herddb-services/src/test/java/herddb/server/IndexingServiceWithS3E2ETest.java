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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * End-to-end test exercising the **full remote-storage stack** against a
 * real S3 backend (MinIO in testcontainers):
 *
 * <ul>
 *   <li><strong>MinIO</strong> — object storage.</li>
 *   <li><strong>RemoteFileServer</strong> with {@code storage.mode=s3} —
 *       gRPC front-end for the bucket.</li>
 *   <li><strong>HerdDB Server</strong> with {@code server.storage.mode=remote}
 *       — table data lives in S3. Only commit-log and metadata stay local.</li>
 *   <li><strong>IndexingServer</strong> with
 *       {@code indexing.storage.type=remote} — vector graph/map pages live in S3.</li>
 * </ul>
 *
 * <p>Scenarios covered:
 * <ol>
 *   <li>ANN search before any checkpoint (live shards only).</li>
 *   <li>ANN search after a server checkpoint (data flushed to S3).</li>
 *   <li>Restart of the indexing service <em>before</em> its vector-store
 *       checkpoint — replay from the (local) commit log must rebuild the
 *       live shards.</li>
 *   <li>Restart of the indexing service <em>after</em> its vector-store
 *       checkpoint — the store must reload multi-segment FusedPQ state
 *       from S3 via the multipart reader supplier and stream graph bytes
 *       on demand.</li>
 * </ol>
 */
public class IndexingServiceWithS3E2ETest {

    private static final String BUCKET = "herddb-e2e";
    private static final String MINIO_USER = "minioadmin";
    private static final String MINIO_PASS = "minioadmin";

    @ClassRule
    public static GenericContainer<?> minio = new GenericContainer<>("minio/minio:latest")
            .withExposedPorts(9000)
            .withEnv("MINIO_ROOT_USER", MINIO_USER)
            .withEnv("MINIO_ROOT_PASSWORD", MINIO_PASS)
            .withCommand("server /data --console-address :9001")
            .waitingFor(Wait.forHttp("/minio/health/live").forPort(9000));

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private String minioEndpoint;
    private String testPrefix;

    @Before
    public void createBucket() throws Exception {
        minioEndpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
        testPrefix = "e2e-" + UUID.randomUUID() + "/";
        try (S3AsyncClient client = buildS3Client()) {
            client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build()).get();
        } catch (Exception ignored) {
            // bucket already exists from a previous test
        }
    }

    @After
    public void cleanupBucketPrefix() throws Exception {
        try (S3AsyncClient client = buildS3Client()) {
            new herddb.remote.storage.S3ObjectStorage(client, BUCKET, testPrefix)
                    .deleteByPrefix("").get();
        }
    }

    private S3AsyncClient buildS3Client() {
        return S3AsyncClient.builder()
                .endpointOverride(URI.create(minioEndpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO_USER, MINIO_PASS)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
    }

    private Properties s3BackendConfig(Path fileServerDataDir) {
        Properties config = new Properties();
        config.setProperty("storage.mode", "s3");
        config.setProperty("s3.endpoint", minioEndpoint);
        config.setProperty("s3.bucket", BUCKET);
        config.setProperty("s3.region", "us-east-1");
        config.setProperty("s3.access.key", MINIO_USER);
        config.setProperty("s3.secret.key", MINIO_PASS);
        config.setProperty("s3.prefix", testPrefix);
        config.setProperty("cache.dir", fileServerDataDir.resolve("s3-cache").toString());
        config.setProperty("cache.max.bytes", String.valueOf(64 * 1024 * 1024));
        return config;
    }

    // -------------------------------------------------------------------------
    // Topology holder
    // -------------------------------------------------------------------------

    /**
     * Boots the full stack. Call {@link #stopIndexing()}/{@link #startIndexing(Server)}
     * to simulate an indexing-service restart.
     */
    private final class Topology implements AutoCloseable {

        final Server server;
        final RemoteFileServer fileServer;
        final Path indexingLogDir;
        final Path indexingData;
        final Properties indexingProps;
        final Properties fileServerConfig;
        final Path fileServerDataDir;
        IndexingServiceEngine engine;
        IndexingServer indexingServer;
        IndexingServiceClient indexingClient;

        Topology() throws Exception {
            this(60000L); // default compaction interval
        }

        Topology(long compactionIntervalMs) throws Exception {
            fileServerDataDir = folder.newFolder("fileserver").toPath();
            fileServerConfig = s3BackendConfig(fileServerDataDir);
            fileServer = new RemoteFileServer("localhost", 0,
                    fileServerDataDir, 4, fileServerConfig);
            fileServer.start();

            String fileServerAddr = "localhost:" + fileServer.getPort();

            // HerdDB Server — standalone + server.storage.mode=remote
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

            // IndexingService — remote storage pointing at the same file server
            Path logDir = baseDir.resolve(
                    serverConfig.getString(ServerConfiguration.PROPERTY_LOGDIR,
                            ServerConfiguration.PROPERTY_LOGDIR_DEFAULT));
            Path indexingBase = folder.newFolder("indexing").toPath();
            indexingLogDir = logDir;
            indexingData = indexingBase.resolve("data");
            Files.createDirectories(indexingData);

            indexingProps = new Properties();
            indexingProps.setProperty(
                    IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "remote");
            indexingProps.setProperty(
                    IndexingServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS, fileServerAddr);
            indexingProps.setProperty(
                    IndexingServerConfiguration.PROPERTY_GRPC_PORT, "0");
            indexingProps.setProperty(
                    IndexingServerConfiguration.PROPERTY_GRPC_HOST, "localhost");
            indexingProps.setProperty(
                    IndexingServerConfiguration.PROPERTY_INSTANCE_ID, "0");
            indexingProps.setProperty(
                    IndexingServerConfiguration.PROPERTY_NUM_INSTANCES, "1");
            indexingProps.setProperty(
                    IndexingServerConfiguration.PROPERTY_COMPACTION_INTERVAL,
                    String.valueOf(compactionIntervalMs));

            startIndexing(server);
        }

        void startIndexing(Server mainServer) throws Exception {
            IndexingServerConfiguration indexingConfig =
                    new IndexingServerConfiguration(indexingProps);
            engine = new IndexingServiceEngine(indexingLogDir, indexingData, indexingConfig);
            engine.setMetadataStorageManager(mainServer.getMetadataStorageManager());
            indexingServer = new IndexingServer("localhost", 0, engine, indexingConfig);
            indexingServer.start();
            engine.start();
            indexingClient = new IndexingServiceClient(
                    Collections.singletonList(indexingServer.getAddress()));
            mainServer.getManager().setRemoteVectorIndexService(indexingClient);
        }

        void stopIndexing() throws Exception {
            if (indexingClient != null) {
                try {
                    indexingClient.close();
                } catch (Exception ignored) {
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

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private HDBClient newHDBClient(Server server) throws Exception {
        HDBClient hdbClient = new HDBClient(
                new ClientConfiguration(folder.newFolder().toPath()));
        hdbClient.setClientSideMetadataProvider(
                new StaticClientSideMetadataProvider(server));
        return hdbClient;
    }

    private static void createTableAndIndex(HDBConnection con) throws Exception {
        con.executeUpdate(TableSpace.DEFAULT,
                "CREATE TABLE t1 (id int primary key, vec floata not null)",
                0, false, true, Collections.emptyList());
        con.executeUpdate(TableSpace.DEFAULT,
                "CREATE VECTOR INDEX vidx ON t1(vec)",
                0, false, true, Collections.emptyList());
    }

    private static void insertOrthogonalBasis(HDBConnection con) throws Exception {
        float[] vecX = {1.0f, 0.0f, 0.0f, 0.0f};
        float[] vecY = {0.0f, 1.0f, 0.0f, 0.0f};
        float[] vecZ = {0.0f, 0.0f, 1.0f, 0.0f};
        float[] vecW = {0.0f, 0.0f, 0.0f, 1.0f};
        con.executeUpdate(TableSpace.DEFAULT,
                "INSERT INTO t1(id, vec) VALUES(?, ?)",
                0, false, true, Arrays.asList(1, vecX));
        con.executeUpdate(TableSpace.DEFAULT,
                "INSERT INTO t1(id, vec) VALUES(?, ?)",
                0, false, true, Arrays.asList(2, vecY));
        con.executeUpdate(TableSpace.DEFAULT,
                "INSERT INTO t1(id, vec) VALUES(?, ?)",
                0, false, true, Arrays.asList(3, vecZ));
        con.executeUpdate(TableSpace.DEFAULT,
                "INSERT INTO t1(id, vec) VALUES(?, ?)",
                0, false, true, Arrays.asList(4, vecW));
    }

    private static int annTop1(HDBConnection con, float[] query) throws Exception {
        try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                "SELECT id FROM t1 ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT 3",
                false, Arrays.asList((Object) query), 0, 10, 10, false)) {
            List<Map<String, Object>> rows = scan.consume();
            assertFalse("ANN search must return results", rows.isEmpty());
            return ((Number) rows.get(0).get("id")).intValue();
        }
    }

    private static void waitForIndexing(Server server, HDBConnection con,
                                        long timeoutMs) throws Exception {
        // Force a flush of the client's write buffer and let the indexing
        // service drain it. Simplest pattern in the existing suite: a short
        // polling sleep loop with a search-result check. We don't have
        // getLastLSN via the public JDBC API, so we poll.
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                    "SELECT id FROM t1 ORDER BY ann_of(vec, "
                            + "CAST(? AS FLOAT ARRAY)) DESC LIMIT 1",
                    false, Arrays.asList((Object) new float[]{1, 0, 0, 0}),
                    0, 10, 10, false)) {
                if (!scan.consume().isEmpty()) {
                    return;
                }
            }
            Thread.sleep(50);
        }
        throw new AssertionError("indexing service did not catch up within " + timeoutMs + "ms");
    }

    // -------------------------------------------------------------------------
    // Scenarios
    // -------------------------------------------------------------------------

    @Test
    public void searchBeforeCheckpoint() throws Exception {
        try (Topology t = new Topology();
             HDBClient hdbClient = newHDBClient(t.server);
             HDBConnection con = hdbClient.openConnection()) {
            createTableAndIndex(con);
            insertOrthogonalBasis(con);
            waitForIndexing(t.server, con, 5000);

            // Query aimed at Y-axis → should return id=2.
            assertEquals(2, annTop1(con, new float[]{0, 1, 0, 0}));
            // Query aimed at X-axis → should return id=1.
            assertEquals(1, annTop1(con, new float[]{1, 0, 0, 0}));
        }
    }

    @Test
    public void searchAfterCheckpoint() throws Exception {
        try (Topology t = new Topology();
             HDBClient hdbClient = newHDBClient(t.server);
             HDBConnection con = hdbClient.openConnection()) {
            createTableAndIndex(con);
            insertOrthogonalBasis(con);
            waitForIndexing(t.server, con, 5000);

            // Checkpoint the main server — this flushes both table data AND
            // the vector index through RemoteFileDataStorageManager to S3.
            t.server.getManager().checkpoint();

            // Query is still served: data lives on-disk (in S3) for the
            // vector store, served via the multipart reader.
            assertEquals(3, annTop1(con, new float[]{0, 0, 1, 0}));
            assertEquals(4, annTop1(con, new float[]{0, 0, 0, 1}));
        }
    }

    @Test
    public void restartIndexingServiceBeforeCheckpoint() throws Exception {
        // Simulates a crash of the indexing service before any checkpoint has
        // completed. The S3 watermark object does not yet exist, so on restart
        // (even with a wiped local disk) the service loads START_OF_TIME and
        // replays the full commit log — which is the only correct behaviour.
        try (Topology t = new Topology();
             HDBClient hdbClient = newHDBClient(t.server);
             HDBConnection con = hdbClient.openConnection()) {
            createTableAndIndex(con);
            insertOrthogonalBasis(con);
            waitForIndexing(t.server, con, 5000);

            t.stopIndexing();
            // Ephemeral-pod simulation: wipe ALL local indexing state.
            wipeIndexingLocalState(t);
            t.startIndexing(t.server);
            waitForIndexing(t.server, con, 10000);

            // All 4 vectors must still be searchable after the crash+replay.
            assertEquals(1, annTop1(con, new float[]{1, 0, 0, 0}));
            assertEquals(2, annTop1(con, new float[]{0, 1, 0, 0}));
            assertEquals(3, annTop1(con, new float[]{0, 0, 1, 0}));
            assertEquals(4, annTop1(con, new float[]{0, 0, 0, 1}));
        }
    }

    @Test
    public void restartIndexingServiceAfterCheckpoint() throws Exception {
        // After inserts, force a checkpoint+watermark publish. Then stop the
        // indexing service, WIPE its local disk (simulating an ephemeral
        // Kubernetes pod restart), and restart. The service must:
        //   - hydrate its remote-metadata cache from S3 (IndexStatus markers)
        //   - load the watermark from S3
        //   - skip replay of entries already covered by the published checkpoint
        //   - and still return ANN results for every id.
        try (Topology t = new Topology(500L);
             HDBClient hdbClient = newHDBClient(t.server);
             HDBConnection con = hdbClient.openConnection()) {
            createTableAndIndex(con);
            // Enough vectors to trigger FusedPQ segments.
            insertRandomVectors(con, 300, 4, 42);
            waitForIdInTopK(con, queryForId(0, 4), 0, 5, 10000);

            t.server.getManager().checkpoint();
            // Wait for the vector store's background compaction to turn
            // the live shards into at least one sealed on-disk segment.
            waitUntilSegmentsExistInS3(t, 15000);

            // Publish IndexStatus + watermark to S3 atomically before restart.
            t.engine.forceCheckpointAndSaveWatermark();

            t.stopIndexing();
            // Ephemeral-pod simulation: wipe every byte of local indexing state.
            wipeIndexingLocalState(t);
            t.startIndexing(t.server);
            waitForIdInTopK(con, queryForId(0, 4), 0, 5, 15000);

            // Several ids must be retrievable via ANN — we exercise a
            // couple to be sure the reloaded/replayed state is queryable.
            waitForIdInTopK(con, queryForId(100, 4), 100, 5, 10000);
            waitForIdInTopK(con, queryForId(250, 4), 250, 5, 10000);

            // New inserts after restart still work.
            float[] vecDiag = new float[4];
            Arrays.fill(vecDiag, 0.5f);
            con.executeUpdate(TableSpace.DEFAULT,
                    "INSERT INTO t1(id, vec) VALUES(?, ?)",
                    0, false, true, Arrays.asList(9999, vecDiag));
            waitForIdInTopK(con, vecDiag, 9999, 5, 10000);
        }
    }

    /**
     * Simulates an ephemeral-pod restart by wiping the indexing service's
     * local data directory. After this, ALL persistent state (watermark +
     * IndexStatus checkpoint markers) must be read back from S3 on restart.
     */
    private static void wipeIndexingLocalState(Topology t) throws Exception {
        if (!Files.exists(t.indexingData)) {
            return;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(t.indexingData)) {
            walk.sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (java.io.IOException ignored) {
                            // best-effort
                        }
                    });
        }
        Files.createDirectories(t.indexingData);
    }

    /**
     * Polls the MinIO bucket until at least one index-page object exists
     * under the test prefix. This confirms that a vector-store checkpoint
     * has flushed segments to S3.
     */
    private void waitUntilSegmentsExistInS3(Topology t, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (S3AsyncClient client = buildS3Client()) {
                software.amazon.awssdk.services.s3.model.ListObjectsV2Response resp =
                        client.listObjectsV2(
                                software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder()
                                        .bucket(BUCKET).prefix(testPrefix).build()).get();
                long indexPages = resp.contents().stream()
                        .filter(o -> o.key().contains("/index/"))
                        .count();
                if (indexPages > 0) {
                    return;
                }
            }
            Thread.sleep(200);
        }
        throw new AssertionError(
                "no index-page objects appeared in S3 within " + timeoutMs + " ms");
    }

    private static float[] queryForId(int id, int dim) {
        // Deterministic "query vector near vector id": reuse the same seed
        // that insertRandomVectors uses for each row.
        java.util.Random rng = new java.util.Random(42L + id);
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private static void insertRandomVectors(HDBConnection con, int count,
                                            int dim, long seedBase) throws Exception {
        for (int id = 0; id < count; id++) {
            java.util.Random rng = new java.util.Random(seedBase + id);
            float[] v = new float[dim];
            for (int d = 0; d < dim; d++) {
                v[d] = rng.nextFloat();
            }
            con.executeUpdate(TableSpace.DEFAULT,
                    "INSERT INTO t1(id, vec) VALUES(?, ?)",
                    0, false, true, Arrays.asList(id, v));
        }
    }


    /**
     * Polls the ANN search until the expected id appears in the top-k
     * results, so we don't race with the indexing-service consumer.
     */
    private static void waitForIdInTopK(HDBConnection con, float[] query,
                                        int expectedId, int topK,
                                        long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        List<Integer> lastIds = Collections.emptyList();
        while (System.currentTimeMillis() < deadline) {
            try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                    "SELECT id FROM t1 ORDER BY ann_of(vec, "
                            + "CAST(? AS FLOAT ARRAY)) DESC LIMIT " + topK,
                    false, Arrays.asList((Object) query), 0, 10, 10, false)) {
                List<Map<String, Object>> rows = scan.consume();
                List<Integer> ids = new ArrayList<>();
                for (Map<String, Object> row : rows) {
                    ids.add(((Number) row.get("id")).intValue());
                }
                lastIds = ids;
                if (ids.contains(expectedId)) {
                    return;
                }
            }
            Thread.sleep(50);
        }
        throw new AssertionError("id=" + expectedId + " did not appear in top-"
                + topK + " within " + timeoutMs + " ms; last ids=" + lastIds);
    }

    @Test
    public void tmpDirStaysLocalNotUploadedToS3() throws Exception {
        // Guard that `indexing.data.dir` only holds transient work files
        // (no resident graph/map tmp files after a checkpoint). The watermark
        // now lives on S3, so watermark.dat does NOT exist locally.
        try (Topology t = new Topology();
             HDBClient hdbClient = newHDBClient(t.server);
             HDBConnection con = hdbClient.openConnection()) {
            createTableAndIndex(con);
            insertOrthogonalBasis(con);
            waitForIndexing(t.server, con, 5000);
            t.server.getManager().checkpoint();
            // Give the vector-store compaction loop a moment to settle.
            Thread.sleep(500);

            // No resident herddb-vector-*.tmp files after a successful
            // checkpoint: P1.4 moved graph reads to on-demand page-store
            // streaming, so no per-segment mmap file lingers.
            long lingering;
            try (java.util.stream.Stream<Path> s = Files.list(t.indexingData)) {
                lingering = s.filter(p -> p.getFileName().toString()
                                .startsWith("herddb-vector-"))
                        .count();
            }
            assertEquals("no resident graph/map tmp files should remain",
                    0L, lingering);

            // With S3WatermarkStore installed, the watermark is on S3, NOT
            // on the local disk. Publish one and verify it landed on S3.
            t.engine.forceCheckpointAndSaveWatermark();
            t.stopIndexing();
            assertFalse("watermark.dat must NOT live locally with remote storage",
                    Files.exists(t.indexingData.resolve("watermark.dat")));

            // Verify the watermark was published to S3.
            try (S3AsyncClient s3 = buildS3Client()) {
                software.amazon.awssdk.services.s3.model.ListObjectsV2Response resp =
                        s3.listObjectsV2(
                                software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder()
                                        .bucket(BUCKET).prefix(testPrefix).build()).get();
                long watermarkObjects = resp.contents().stream()
                        .filter(o -> o.key().endsWith("/watermark.lsn"))
                        .count();
                assertTrue("watermark.lsn must exist on S3 under the _indexing/ prefix",
                        watermarkObjects > 0);
            }
        }
    }
}
