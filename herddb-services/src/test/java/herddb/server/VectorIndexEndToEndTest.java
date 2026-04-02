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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.ScanResultSet;
import herddb.indexing.EmbeddedIndexingService;
import herddb.indexing.IndexingServiceClient;
import herddb.model.TableSpace;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end tests that boot a real HerdDB Server with a real IndexingService
 * (in-process gRPC) and verify the full vector index lifecycle through the
 * client API.
 *
 * @author enrico.olivelli
 */
public class VectorIndexEndToEndTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Boots a standalone HerdDB server and an embedded IndexingService sharing
     * the same txlog directory.  Creates a table with a vector column, creates
     * a vector index, inserts rows, and verifies the index is visible via
     * system tables.
     */
    @Test
    public void testCreateVectorIndexAndInsert() throws Exception {
        Path baseDir = folder.newFolder("herddb").toPath();
        Path indexingDataDir = folder.newFolder("indexing-data").toPath();

        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_STANDALONE);
        config.set(ServerConfiguration.PROPERTY_BASEDIR, baseDir.toAbsolutePath().toString());
        config.set(ServerConfiguration.PROPERTY_HOST, "localhost");
        config.set(ServerConfiguration.PROPERTY_PORT, 0);
        config.set("http.enable", false);

        Path logDir = baseDir.resolve(
                config.getString(ServerConfiguration.PROPERTY_LOGDIR, ServerConfiguration.PROPERTY_LOGDIR_DEFAULT));

        try (EmbeddedIndexingService indexingService = new EmbeddedIndexingService(logDir, indexingDataDir)) {
            indexingService.start();

            config.set(ServerConfiguration.PROPERTY_INDEXING_SERVICE_SERVERS,
                    indexingService.getAddress());

            try (Server server = new Server(config)) {
                IndexingServiceClient client = indexingService.createClient();
                server.getManager().setRemoteVectorIndexService(client);
                server.start();
                server.waitForStandaloneBoot();

                try (HDBClient hdbClient = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                    hdbClient.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                    try (HDBConnection con = hdbClient.openConnection()) {
                        con.executeUpdate(TableSpace.DEFAULT,
                                "CREATE TABLE t1 (id int primary key, vec floata not null)",
                                0, false, true, Collections.emptyList());

                        con.executeUpdate(TableSpace.DEFAULT,
                                "CREATE VECTOR INDEX vidx ON t1(vec)",
                                0, false, true, Collections.emptyList());

                        for (int i = 1; i <= 10; i++) {
                            con.executeUpdate(TableSpace.DEFAULT,
                                    "INSERT INTO t1(id, vec) VALUES(?, ?)",
                                    0, false, true,
                                    Arrays.asList(i, new float[]{i * 1.0f, 0.0f, 0.0f, 0.0f}));
                        }

                        // Verify the index is visible in system tables
                        try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                                "SELECT * FROM SYSINDEXES WHERE index_name='vidx'",
                                false, Collections.emptyList(), 0, 10, 10, false)) {
                            List<Map<String, Object>> rows = scan.consume();
                            assertEquals("vector index must appear in SYSINDEXES", 1, rows.size());
                        }
                    }
                } finally {
                    client.close();
                }
            }
        }
    }

    /**
     * Verifies that DML operations (insert, update, delete) succeed when a
     * real IndexingService is connected.
     */
    @Test
    public void testInsertUpdateDeleteWithRealIndexingService() throws Exception {
        Path baseDir = folder.newFolder("herddb").toPath();
        Path indexingDataDir = folder.newFolder("indexing-data").toPath();

        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_STANDALONE);
        config.set(ServerConfiguration.PROPERTY_BASEDIR, baseDir.toAbsolutePath().toString());
        config.set(ServerConfiguration.PROPERTY_HOST, "localhost");
        config.set(ServerConfiguration.PROPERTY_PORT, 0);
        config.set("http.enable", false);

        Path logDir = baseDir.resolve(
                config.getString(ServerConfiguration.PROPERTY_LOGDIR, ServerConfiguration.PROPERTY_LOGDIR_DEFAULT));

        try (EmbeddedIndexingService indexingService = new EmbeddedIndexingService(logDir, indexingDataDir)) {
            indexingService.start();

            try (Server server = new Server(config)) {
                IndexingServiceClient client = indexingService.createClient();
                server.getManager().setRemoteVectorIndexService(client);
                server.start();
                server.waitForStandaloneBoot();

                try (HDBClient hdbClient = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                    hdbClient.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                    try (HDBConnection con = hdbClient.openConnection()) {
                        con.executeUpdate(TableSpace.DEFAULT,
                                "CREATE TABLE t1 (id int primary key, vec floata not null)",
                                0, false, true, Collections.emptyList());

                        con.executeUpdate(TableSpace.DEFAULT,
                                "CREATE VECTOR INDEX vidx ON t1(vec)",
                                0, false, true, Collections.emptyList());

                        // Insert
                        for (int i = 1; i <= 5; i++) {
                            con.executeUpdate(TableSpace.DEFAULT,
                                    "INSERT INTO t1(id, vec) VALUES(?, ?)",
                                    0, false, true,
                                    Arrays.asList(i, new float[]{i * 1.0f, 0.0f, 0.0f}));
                        }

                        // Update
                        con.executeUpdate(TableSpace.DEFAULT,
                                "UPDATE t1 SET vec=? WHERE id=?",
                                0, false, true,
                                Arrays.asList(new float[]{0.0f, 1.0f, 0.0f}, 1));

                        // Delete
                        con.executeUpdate(TableSpace.DEFAULT,
                                "DELETE FROM t1 WHERE id=?",
                                0, false, true,
                                Arrays.asList(2));

                        // Verify remaining row count
                        try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                                "SELECT COUNT(*) AS cnt FROM t1",
                                false, Collections.emptyList(), 0, 10, 10, false)) {
                            List<Map<String, Object>> rows = scan.consume();
                            assertEquals(1, rows.size());
                            Number count = (Number) rows.get(0).get("cnt");
                            assertEquals(4, count.intValue());
                        }
                    }
                } finally {
                    client.close();
                }
            }
        }
    }

    /**
     * Boots a real HerdDB server with an embedded IndexingService, creates a
     * table with a vector column, inserts rows, and verifies that ANN search
     * via ORDER BY ann_of() DESC LIMIT k returns the correct results.
     */
    @Test
    public void testANNSearchReturnsResults() throws Exception {
        Path baseDir = folder.newFolder("herddb").toPath();
        Path indexingDataDir = folder.newFolder("indexing-data").toPath();

        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_STANDALONE);
        config.set(ServerConfiguration.PROPERTY_BASEDIR, baseDir.toAbsolutePath().toString());
        config.set(ServerConfiguration.PROPERTY_HOST, "localhost");
        config.set(ServerConfiguration.PROPERTY_PORT, 0);
        config.set("http.enable", false);

        Path logDir = baseDir.resolve(
                config.getString(ServerConfiguration.PROPERTY_LOGDIR, ServerConfiguration.PROPERTY_LOGDIR_DEFAULT));

        try (EmbeddedIndexingService indexingService = new EmbeddedIndexingService(logDir, indexingDataDir)) {
            indexingService.start();

            config.set(ServerConfiguration.PROPERTY_INDEXING_SERVICE_SERVERS,
                    indexingService.getAddress());

            try (Server server = new Server(config)) {
                IndexingServiceClient client = indexingService.createClient();
                server.getManager().setRemoteVectorIndexService(client);
                server.start();
                server.waitForStandaloneBoot();

                try (HDBClient hdbClient = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                    hdbClient.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                    try (HDBConnection con = hdbClient.openConnection()) {
                        con.executeUpdate(TableSpace.DEFAULT,
                                "CREATE TABLE t1 (id int primary key, vec floata not null)",
                                0, false, true, Collections.emptyList());

                        con.executeUpdate(TableSpace.DEFAULT,
                                "CREATE VECTOR INDEX vidx ON t1(vec)",
                                0, false, true, Collections.emptyList());

                        // Insert 3 vectors: along X, Y, Z axes
                        float[] vecX = {1.0f, 0.0f, 0.0f, 0.0f};
                        float[] vecY = {0.0f, 1.0f, 0.0f, 0.0f};
                        float[] vecZ = {0.0f, 0.0f, 1.0f, 0.0f};

                        con.executeUpdate(TableSpace.DEFAULT,
                                "INSERT INTO t1(id, vec) VALUES(?, ?)",
                                0, false, true, Arrays.asList(1, vecX));
                        con.executeUpdate(TableSpace.DEFAULT,
                                "INSERT INTO t1(id, vec) VALUES(?, ?)",
                                0, false, true, Arrays.asList(2, vecY));
                        con.executeUpdate(TableSpace.DEFAULT,
                                "INSERT INTO t1(id, vec) VALUES(?, ?)",
                                0, false, true, Arrays.asList(3, vecZ));

                        // Force a checkpoint to make sure the indexing service has caught up
                        server.getManager().checkpoint();

                        // Search for the vector closest to X axis
                        float[] query = {1.0f, 0.0f, 0.0f, 0.0f};

                        try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                                "SELECT id FROM t1 ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT 2",
                                false, Arrays.asList((Object) query), 0, 10, 10, false)) {
                            List<Map<String, Object>> rows = scan.consume();
                            assertTrue("ANN search must return results, got " + rows.size(),
                                    rows.size() > 0);
                            // The closest vector to [1,0,0,0] should be id=1
                            Number firstId = (Number) rows.get(0).get("id");
                            assertEquals("Closest vector to X-axis should be id=1",
                                    1, firstId.intValue());
                        }
                    }
                } finally {
                    client.close();
                }
            }
        }
    }

    /**
     * Tests that the ServerMain wiring correctly creates the IndexingServiceClient
     * when indexing.service.servers is configured.
     */
    @Test
    public void testServerMainWiresIndexingService() throws Exception {
        Path baseDir = folder.newFolder("herddb").toPath();
        Path indexingDataDir = folder.newFolder("indexing-data").toPath();

        Path logDir = baseDir.resolve(ServerConfiguration.PROPERTY_LOGDIR_DEFAULT);

        try (EmbeddedIndexingService indexingService = new EmbeddedIndexingService(logDir, indexingDataDir)) {
            indexingService.start();

            java.util.Properties props = new java.util.Properties();
            props.setProperty(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_STANDALONE);
            props.setProperty(ServerConfiguration.PROPERTY_BASEDIR, baseDir.toAbsolutePath().toString());
            props.setProperty(ServerConfiguration.PROPERTY_HOST, "localhost");
            props.setProperty(ServerConfiguration.PROPERTY_PORT, "0");
            props.setProperty("http.enable", "false");
            props.setProperty(ServerConfiguration.PROPERTY_INDEXING_SERVICE_SERVERS,
                    indexingService.getAddress());

            ServerMain serverMain = new ServerMain(props);
            try {
                serverMain.start();
                Server server = serverMain.getServer();
                assertNotNull("Server must be started", server);
                server.waitForStandaloneBoot();

                assertNotNull("RemoteVectorIndexService must be wired",
                        server.getManager().getRemoteVectorIndexService());

                try (HDBClient hdbClient = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                    hdbClient.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                    try (HDBConnection con = hdbClient.openConnection()) {
                        con.executeUpdate(TableSpace.DEFAULT,
                                "CREATE TABLE t1 (id int primary key, vec floata not null)",
                                0, false, true, Collections.emptyList());

                        // This would have thrown IllegalArgumentException before the fix
                        con.executeUpdate(TableSpace.DEFAULT,
                                "CREATE VECTOR INDEX vidx ON t1(vec)",
                                0, false, true, Collections.emptyList());
                    }
                }
            } finally {
                serverMain.close();
            }
        }
    }
}
