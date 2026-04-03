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
import herddb.client.ZookeeperClientSideMetadataProvider;
import herddb.indexing.IndexingServer;
import herddb.indexing.IndexingServerConfiguration;
import herddb.indexing.IndexingServiceClient;
import herddb.indexing.IndexingServiceEngine;
import herddb.metadata.ServiceDiscoveryListener;
import herddb.model.TableSpace;
import herddb.remote.RemoteFileServer;
import java.io.InputStream;
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
 * End-to-end test that boots a HerdDB Server in cluster mode (with embedded
 * bookie), a RemoteFileServer registered in ZooKeeper, and an IndexingService
 * configured with {@code indexing.storage.type=remote} using ZK-based
 * discovery for the file server.
 *
 * <p>Verifies that the IndexingService discovers the file server via ZK,
 * stores its vector index data on the remote file server, and that ANN
 * search returns correct results.
 *
 * @author enrico.olivelli
 */
public class ZKDiscoveryRemoteFileIndexingTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testIndexingServiceWithRemoteStorageZKDiscovery() throws Exception {
        Path zkDir = folder.newFolder("zk").toPath();
        Path indexingDataDir = folder.newFolder("indexing-data").toPath();
        Path fileServerDataDir = folder.newFolder("fileserver").toPath();

        org.apache.curator.test.TestingServer zookeeperServer =
                new org.apache.curator.test.TestingServer(-1, zkDir.toFile(), true);
        try {
            String zkAddress = zookeeperServer.getConnectString();
            String zkPath = "/herddb";

            // Start RemoteFileServer on ephemeral port
            RemoteFileServer fileServer = new RemoteFileServer("localhost", 0, fileServerDataDir);
            fileServer.start();

            try {
                // Register file server in ZK
                herddb.cluster.ZookeeperMetadataStorageManager fileServerRegistration =
                        new herddb.cluster.ZookeeperMetadataStorageManager(zkAddress, 40000, zkPath);
                fileServerRegistration.start();

                try {
                    String fileServerAddr = "localhost:" + fileServer.getPort();
                    fileServerRegistration.registerFileServer("fs1", fileServerAddr);

                    // Verify registration
                    List<String> registeredServers = fileServerRegistration.listFileServers();
                    assertEquals(1, registeredServers.size());

                    // Prepare HerdDB server properties (cluster mode, embedded bookie)
                    Properties serverProps = new Properties();
                    try (InputStream in = SimpleClusterTest.class.getResourceAsStream(
                            "/conf/test.server_cluster.properties")) {
                        serverProps.load(in);
                    }
                    serverProps.put(ServerConfiguration.PROPERTY_BASEDIR,
                            folder.newFolder("herddb").getAbsolutePath());
                    serverProps.put(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkAddress);
                    serverProps.put(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, zkPath);
                    serverProps.put("http.enable", "false");

                    // Start HerdDB Server first (starts embedded bookie and creates tablespace)
                    ServerConfiguration serverConfig = new ServerConfiguration(serverProps);
                    try (Server server = new Server(serverConfig)) {
                        server.start();
                        server.waitForStandaloneBoot();

                        // Configure IndexingService in cluster mode with remote storage
                        // and NO static file server addresses (so it discovers via ZK)
                        Properties indexingProps = new Properties();
                        indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_MODE,
                                ServerConfiguration.PROPERTY_MODE_CLUSTER);
                        indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkAddress);
                        indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_PATH, zkPath);
                        indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT,
                                String.valueOf(40000));
                        indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_LOG_TYPE, "bookkeeper");
                        indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "remote");
                        // No PROPERTY_REMOTE_FILE_SERVERS set -- ZK discovery
                        indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_GRPC_PORT, "0");
                        indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_GRPC_HOST, "localhost");
                        indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_NAME,
                                TableSpace.DEFAULT);
                        indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_INSTANCE_ID, "0");
                        indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES, "1");
                        indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH,
                                ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT);

                        IndexingServerConfiguration indexingConfig = new IndexingServerConfiguration(indexingProps);

                        Path indexingLogDir = indexingDataDir.resolve("log");
                        Path indexingData = indexingDataDir.resolve("data");
                        Files.createDirectories(indexingLogDir);
                        Files.createDirectories(indexingData);

                        try (IndexingServiceEngine engine =
                                     new IndexingServiceEngine(indexingLogDir, indexingData, indexingConfig)) {

                            IndexingServer indexingServer =
                                    new IndexingServer("localhost", 0, engine, indexingConfig);
                            // Set metadata storage manager so the IndexingServer can:
                            // 1. Register itself in ZK
                            // 2. Discover file servers via ZK for remote storage
                            indexingServer.setMetadataStorageManager(server.getMetadataStorageManager());
                            indexingServer.start();

                            try {
                                engine.start();

                                // Verify indexing service is registered in ZK
                                List<String> indexingServices =
                                        server.getMetadataStorageManager().listIndexingServices();
                                assertFalse("Indexing service should be discoverable via ZK",
                                        indexingServices.isEmpty());

                                // Create IndexingServiceClient from ZK-discovered addresses
                                IndexingServiceClient client = new IndexingServiceClient(indexingServices, 30);
                                server.getManager().setRemoteVectorIndexService(client);

                                // Register listener for dynamic updates
                                server.getMetadataStorageManager().addServiceDiscoveryListener(
                                        new ServiceDiscoveryListener() {
                                            @Override
                                            public void onIndexingServicesChanged(List<String> currentAddresses) {
                                                client.updateServers(currentAddresses);
                                            }

                                            @Override
                                            public void onFileServersChanged(List<String> currentAddresses) {
                                                // handled by IndexingServer internally
                                            }
                                        });

                                try {
                                    try (HDBClient hdbClient = new HDBClient(
                                            new ClientConfiguration(folder.newFolder().toPath()))) {
                                        hdbClient.setClientSideMetadataProvider(
                                                new ZookeeperClientSideMetadataProvider(
                                                        zkAddress, 40000, zkPath));
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

                                            // Checkpoint to flush data to remote storage
                                            server.getManager().checkpoint();

                                            // ANN search for vector closest to Y axis
                                            float[] query = {0.0f, 1.0f, 0.0f, 0.0f};

                                            try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                                                    "SELECT id FROM t1 ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT 2",
                                                    false, Arrays.asList((Object) query), 0, 10, 10, false)) {
                                                List<Map<String, Object>> rows = scan.consume();
                                                assertTrue("ANN search must return results, got " + rows.size(),
                                                        rows.size() > 0);
                                                Number firstId = (Number) rows.get(0).get("id");
                                                assertEquals("Closest vector to Y-axis should be id=2",
                                                        2, firstId.intValue());
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
                    fileServerRegistration.close();
                }
            } finally {
                fileServer.stop();
            }
        } finally {
            zookeeperServer.close();
        }
    }
}
