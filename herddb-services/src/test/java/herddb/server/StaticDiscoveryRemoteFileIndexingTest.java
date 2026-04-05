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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end test that boots a standalone HerdDB Server, a RemoteFileServer,
 * and an IndexingService configured with {@code indexing.storage.type=remote}
 * using static discovery ({@code remote.file.servers}).
 *
 * <p>All services share the same file-based metadata manager (via the Server's
 * MetadataStorageManager) so that the IndexingService resolves the correct
 * tablespace UUID and tails the right commit log directory.
 *
 * @author enrico.olivelli
 */
public class StaticDiscoveryRemoteFileIndexingTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testIndexingServiceWithRemoteStorageStaticDiscovery() throws Exception {
        Path baseDir = folder.newFolder("herddb").toPath();
        Path indexingDataDir = folder.newFolder("indexing-data").toPath();
        Path fileServerDataDir = folder.newFolder("fileserver").toPath();

        // Start RemoteFileServer on ephemeral port
        RemoteFileServer fileServer = new RemoteFileServer("localhost", 0, fileServerDataDir);
        fileServer.start();

        try {
            String fileServerAddr = "localhost:" + fileServer.getPort();

            // Configure HerdDB Server (standalone mode)
            ServerConfiguration config = new ServerConfiguration();
            config.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_STANDALONE);
            config.set(ServerConfiguration.PROPERTY_BASEDIR, baseDir.toAbsolutePath().toString());
            config.set(ServerConfiguration.PROPERTY_HOST, "localhost");
            config.set(ServerConfiguration.PROPERTY_PORT, 0);
            config.set("http.enable", false);

            // Start HerdDB Server first so the txlog directory and tablespace exist
            try (Server server = new Server(config)) {
                server.start();
                server.waitForStandaloneBoot();

                // The txlog directory is created by the server
                Path logDir = baseDir.resolve(
                        config.getString(ServerConfiguration.PROPERTY_LOGDIR,
                                ServerConfiguration.PROPERTY_LOGDIR_DEFAULT));

                // Configure IndexingService with remote storage and static file server discovery
                Properties indexingProps = new Properties();
                indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "remote");
                indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS, fileServerAddr);
                indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_GRPC_PORT, "0");
                indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_GRPC_HOST, "localhost");
                indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_INSTANCE_ID, "0");
                indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES, "1");

                IndexingServerConfiguration indexingConfig = new IndexingServerConfiguration(indexingProps);

                Path indexingData = indexingDataDir.resolve("data");
                Files.createDirectories(indexingData);

                try (IndexingServiceEngine engine =
                             new IndexingServiceEngine(logDir, indexingData, indexingConfig)) {

                    // Share the server's MetadataStorageManager so the engine resolves
                    // the correct tablespace UUID and tails the right txlog directory
                    engine.setMetadataStorageManager(server.getMetadataStorageManager());

                    IndexingServer indexingServer =
                            new IndexingServer("localhost", 0, engine, indexingConfig);
                    indexingServer.start();

                    try {
                        engine.start();

                        // Wire IndexingServiceClient to the server
                        IndexingServiceClient client = new IndexingServiceClient(
                                Arrays.asList(indexingServer.getAddress()));
                        server.getManager().setRemoteVectorIndexService(client);

                        try {
                            try (HDBClient hdbClient = new HDBClient(
                                    new ClientConfiguration(folder.newFolder().toPath()))) {
                                hdbClient.setClientSideMetadataProvider(
                                        new StaticClientSideMetadataProvider(server));
                                try (HDBConnection con = hdbClient.openConnection()) {
                                    // Create table with vector column
                                    con.executeUpdate(TableSpace.DEFAULT,
                                            "CREATE TABLE t1 (id int primary key, vec floata not null)",
                                            0, false, true, Collections.emptyList());

                                    // Create vector index
                                    con.executeUpdate(TableSpace.DEFAULT,
                                            "CREATE VECTOR INDEX vidx ON t1(vec)",
                                            0, false, true, Collections.emptyList());

                                    // Insert orthogonal vectors
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

                                    // Checkpoint to flush data to remote storage
                                    server.getManager().checkpoint();

                                    // ANN search for vector closest to X axis
                                    float[] query = {1.0f, 0.0f, 0.0f, 0.0f};

                                    try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                                            "SELECT id FROM t1 ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT 2",
                                            false, Arrays.asList((Object) query), 0, 10, 10, false)) {
                                        List<Map<String, Object>> rows = scan.consume();
                                        assertTrue("ANN search must return results, got " + rows.size(),
                                                rows.size() > 0);
                                        Number firstId = (Number) rows.get(0).get("id");
                                        assertEquals("Closest vector to X-axis should be id=1",
                                                1, firstId.intValue());
                                    }
                                }
                            }
                        } finally {
                            client.close();
                        }
                    } finally {
                        indexingServer.stop();
                    }
                }
            }
        } finally {
            fileServer.stop();
        }
    }
}
