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
import herddb.model.TableSpace;
import java.io.InputStream;
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
 * End-to-end test for follower reads with vector search.
 * Boots 1 leader, 1 follower, 1 indexing service, and verifies
 * vector search works via follower reads.
 */
public class FollowerReadsVectorIndexTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testVectorSearchFromFollower() throws Exception {
        Path zkDir = folder.newFolder("zk").toPath();

        org.apache.curator.test.TestingServer zookeeperServer =
                new org.apache.curator.test.TestingServer(-1, zkDir.toFile(), true);
        try {
            String zkAddress = zookeeperServer.getConnectString();
            String zkPath = "/herddb";

            // Server 1 (leader) - cluster mode with embedded bookie
            Properties serverProps1 = new Properties();
            try (InputStream in = SimpleClusterTest.class.getResourceAsStream(
                    "/conf/test.server_cluster.properties")) {
                serverProps1.load(in);
            }
            serverProps1.put(ServerConfiguration.PROPERTY_BASEDIR,
                    folder.newFolder("herddb1").getAbsolutePath());
            serverProps1.put(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkAddress);
            serverProps1.put(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, zkPath);
            serverProps1.put("http.enable", "false");
            serverProps1.put(ServerConfiguration.PROPERTY_NODEID, "server1");
            serverProps1.put(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, "false");

            // Server 2 (follower) - cluster mode, no embedded bookie
            Properties serverProps2 = new Properties();
            try (InputStream in = SimpleClusterTest.class.getResourceAsStream(
                    "/conf/test.server_cluster.properties")) {
                serverProps2.load(in);
            }
            serverProps2.put(ServerConfiguration.PROPERTY_BASEDIR,
                    folder.newFolder("herddb2").getAbsolutePath());
            serverProps2.put(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkAddress);
            serverProps2.put(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, zkPath);
            serverProps2.put("http.enable", "false");
            serverProps2.put(ServerConfiguration.PROPERTY_NODEID, "server2");
            serverProps2.put(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, "false");
            serverProps2.put("bookie.start", "false");
            serverProps2.put(ServerConfiguration.PROPERTY_PORT, "0");

            ServerConfiguration serverConfig1 = new ServerConfiguration(serverProps1);
            ServerConfiguration serverConfig2 = new ServerConfiguration(serverProps2);

            try (Server server1 = new Server(serverConfig1);
                 Server server2 = new Server(serverConfig2)) {
                server1.start();
                server1.waitForStandaloneBoot();
                server2.start();

                // Start IndexingServiceEngine in cluster mode with BK tailing
                Path indexingDataDir = folder.newFolder("indexing-data").toPath();
                Properties indexingProps = new Properties();
                indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_MODE,
                        ServerConfiguration.PROPERTY_MODE_CLUSTER);
                indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkAddress);
                indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_PATH, zkPath);
                indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT,
                        String.valueOf(40000));
                indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_LOG_TYPE, "bookkeeper");
                indexingProps.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
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
                java.nio.file.Files.createDirectories(indexingLogDir);
                java.nio.file.Files.createDirectories(indexingData);

                try (IndexingServiceEngine engine =
                             new IndexingServiceEngine(indexingLogDir, indexingData, indexingConfig)) {

                    IndexingServer indexingServer =
                            new IndexingServer("localhost", 0, engine, indexingConfig);
                    indexingServer.setMetadataStorageManager(server1.getMetadataStorageManager());
                    indexingServer.start();

                    try {
                        engine.start();

                        // Verify indexing service is registered
                        List<String> services = server1.getMetadataStorageManager().listIndexingServices();
                        assertFalse("Indexing service should be discoverable via ZK", services.isEmpty());

                        // Wire indexing client to both servers
                        IndexingServiceClient client1 = new IndexingServiceClient(services, 30);
                        server1.getManager().setRemoteVectorIndexService(client1);

                        IndexingServiceClient client2 = new IndexingServiceClient(services, 30);
                        server2.getManager().setRemoteVectorIndexService(client2);

                        try {
                            // Use the leader to create table and insert data
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

                                    // Add server2 as follower
                                    con.executeUpdate(TableSpace.DEFAULT,
                                            "ALTER TABLESPACE 'herd','expectedreplicacount:1','leader:"
                                                    + server1.getNodeId() + "','replica:" + server1.getNodeId()
                                                    + "," + server2.getNodeId() + "'",
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

                                    // Force checkpoint
                                    server1.getManager().checkpoint();
                                }
                            }

                            // Wait for follower to boot
                            server2.waitForTableSpaceBoot(TableSpace.DEFAULT, 60000, false);

                            // Wait for data to replicate to follower
                            Thread.sleep(5000);

                            // Now test with follower reads enabled
                            ClientConfiguration clientConfig = new ClientConfiguration(folder.newFolder().toPath());
                            clientConfig.set(ClientConfiguration.PROPERTY_MODE,
                                    ClientConfiguration.PROPERTY_MODE_CLUSTER);
                            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkAddress);
                            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, zkPath);
                            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, 40000);
                            clientConfig.set(ClientConfiguration.PROPERTY_ALLOW_READS_FROM_FOLLOWERS, true);
                            clientConfig.set(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, 1);
                            clientConfig.set(ClientConfiguration.PROPERTY_OPERATION_RETRY_DELAY, 200);
                            clientConfig.set(ClientConfiguration.PROPERTY_MAX_OPERATION_RETRY_COUNT, 30);

                            try (HDBClient hdbClient = new HDBClient(clientConfig);
                                 HDBConnection con = hdbClient.openConnection()) {

                                // ANN search with follower reads enabled
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
                        } finally {
                            client1.close();
                            client2.close();
                        }
                    } finally {
                        indexingServer.stop();
                    }
                }
            }
        } finally {
            zookeeperServer.close();
        }
    }
}
