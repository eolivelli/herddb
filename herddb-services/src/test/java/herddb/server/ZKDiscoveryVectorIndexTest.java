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
 * End-to-end tests that verify the IndexingService is discovered via
 * ZooKeeper and vector search works correctly through the full stack.
 *
 * <p>Unlike {@link VectorIndexEndToEndTest} which statically configures
 * {@code indexing.service.servers}, these tests start the IndexingService
 * in cluster mode so it registers itself in ZooKeeper, and the HerdDB
 * server discovers it dynamically.
 *
 * @author enrico.olivelli
 */
public class ZKDiscoveryVectorIndexTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Boots an IndexingServiceEngine in cluster mode (with BookKeeper WAL
     * tailing), registers it in ZooKeeper, starts a HerdDB server in
     * cluster mode with an embedded bookie, discovers the indexing service
     * via ZK, and verifies that ANN search returns correct results.
     */
    @Test
    public void testSingleIndexingServiceDiscoveredViaZK() throws Exception {
        Path zkDir = folder.newFolder("zk").toPath();
        Path indexingDataDir = folder.newFolder("indexing-data").toPath();

        org.apache.curator.test.TestingServer zookeeperServer =
                new org.apache.curator.test.TestingServer(-1, zkDir.toFile(), true);
        try {
            String zkAddress = zookeeperServer.getConnectString();
            String zkPath = "/herddb";

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

            // Start HerdDB Server first (it starts the embedded bookie and creates the tablespace)
            ServerConfiguration serverConfig = new ServerConfiguration(serverProps);
            try (Server server = new Server(serverConfig)) {
                server.start();
                server.waitForStandaloneBoot();

                // Now start IndexingServiceEngine in cluster mode with BK tailing
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
                    // Set metadata storage manager so the server registers in ZK
                    indexingServer.setMetadataStorageManager(server.getMetadataStorageManager());
                    indexingServer.start();

                    try {
                        engine.start();

                        // 1. Verify indexing service is registered in ZK
                        List<String> services = server.getMetadataStorageManager().listIndexingServices();
                        assertFalse("Indexing service should be discoverable via ZK", services.isEmpty());

                        // 2. Create IndexingServiceClient from ZK-discovered addresses
                        IndexingServiceClient client = new IndexingServiceClient(services, 30);

                        // 3. Wire the client to the server's DBManager
                        server.getManager().setRemoteVectorIndexService(client);

                        // 4. Register a ServiceDiscoveryListener for dynamic updates
                        server.getMetadataStorageManager().addServiceDiscoveryListener(
                                new ServiceDiscoveryListener() {
                                    @Override
                                    public void onIndexingServicesChanged(List<String> currentAddresses) {
                                        client.updateServers(currentAddresses);
                                    }

                                    @Override
                                    public void onFileServersChanged(List<String> currentAddresses) {
                                        // not used in this test
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

                                    // Insert 4 orthogonal vectors
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

                                    // Force checkpoint so the indexing service catches up
                                    server.getManager().checkpoint();

                                    // ANN search for vector closest to X axis
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
            zookeeperServer.close();
        }
    }

    /**
     * Starts two IndexingServiceEngine instances in cluster mode, both tailing
     * the same BookKeeper WAL with different instance IDs (sharding).
     * Both register in ZooKeeper. The client discovers both and fans out
     * searches across them.
     */
    @Test
    public void testTwoIndexingServicesDiscoveredViaZK() throws Exception {
        Path zkDir = folder.newFolder("zk").toPath();

        org.apache.curator.test.TestingServer zookeeperServer =
                new org.apache.curator.test.TestingServer(-1, zkDir.toFile(), true);
        try {
            String zkAddress = zookeeperServer.getConnectString();
            String zkPath = "/herddb";

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

            // Start HerdDB Server first
            ServerConfiguration serverConfig = new ServerConfiguration(serverProps);
            try (Server server = new Server(serverConfig)) {
                server.start();
                server.waitForStandaloneBoot();

                // Start two indexing service instances
                IndexingServiceEngine engine1 = null;
                IndexingServiceEngine engine2 = null;
                IndexingServer indexingServer1 = null;
                IndexingServer indexingServer2 = null;

                try {
                    // Instance 1: instanceId=0, numInstances=2
                    Properties indexingProps1 = buildIndexingProperties(zkAddress, zkPath, 0, 2);
                    IndexingServerConfiguration indexingConfig1 = new IndexingServerConfiguration(indexingProps1);
                    Path indexingDir1 = folder.newFolder("indexing-1").toPath();
                    Path logDir1 = indexingDir1.resolve("log");
                    Path dataDir1 = indexingDir1.resolve("data");
                    java.nio.file.Files.createDirectories(logDir1);
                    java.nio.file.Files.createDirectories(dataDir1);

                    engine1 = new IndexingServiceEngine(logDir1, dataDir1, indexingConfig1);
                    indexingServer1 = new IndexingServer("localhost", 0, engine1, indexingConfig1);
                    indexingServer1.setMetadataStorageManager(server.getMetadataStorageManager());
                    indexingServer1.start();
                    engine1.start();

                    // Instance 2: instanceId=1, numInstances=2
                    Properties indexingProps2 = buildIndexingProperties(zkAddress, zkPath, 1, 2);
                    IndexingServerConfiguration indexingConfig2 = new IndexingServerConfiguration(indexingProps2);
                    Path indexingDir2 = folder.newFolder("indexing-2").toPath();
                    Path logDir2 = indexingDir2.resolve("log");
                    Path dataDir2 = indexingDir2.resolve("data");
                    java.nio.file.Files.createDirectories(logDir2);
                    java.nio.file.Files.createDirectories(dataDir2);

                    engine2 = new IndexingServiceEngine(logDir2, dataDir2, indexingConfig2);
                    indexingServer2 = new IndexingServer("localhost", 0, engine2, indexingConfig2);
                    indexingServer2.setMetadataStorageManager(server.getMetadataStorageManager());
                    indexingServer2.start();
                    engine2.start();

                    // Verify both indexing services are registered in ZK
                    List<String> services = server.getMetadataStorageManager().listIndexingServices();
                    assertEquals("Both indexing services should be discoverable via ZK",
                            2, services.size());

                    // Create client with both discovered services
                    IndexingServiceClient client = new IndexingServiceClient(services, 30);
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

                                // Insert 4 orthogonal vectors
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

                                // Force checkpoint so both indexing services catch up
                                server.getManager().checkpoint();

                                // ANN search for vector closest to Y axis
                                float[] query = {0.0f, 1.0f, 0.0f, 0.0f};

                                try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                                        "SELECT id FROM t1 ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT 2",
                                        false, Arrays.asList((Object) query), 0, 10, 10, false)) {
                                    List<Map<String, Object>> rows = scan.consume();
                                    assertTrue("ANN search must return results, got " + rows.size(),
                                            rows.size() > 0);
                                    // The closest vector to [0,1,0,0] should be id=2
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
                    if (indexingServer2 != null) {
                        indexingServer2.stop();
                    }
                    if (engine2 != null) {
                        engine2.close();
                    }
                    if (indexingServer1 != null) {
                        indexingServer1.stop();
                    }
                    if (engine1 != null) {
                        engine1.close();
                    }
                }
            }
        } finally {
            zookeeperServer.close();
        }
    }

    private Properties buildIndexingProperties(String zkAddress, String zkPath,
                                                int instanceId, int numInstances) {
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_MODE,
                ServerConfiguration.PROPERTY_MODE_CLUSTER);
        props.setProperty(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkAddress);
        props.setProperty(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_PATH, zkPath);
        props.setProperty(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT,
                String.valueOf(40000));
        props.setProperty(IndexingServerConfiguration.PROPERTY_LOG_TYPE, "bookkeeper");
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_GRPC_PORT, "0");
        props.setProperty(IndexingServerConfiguration.PROPERTY_GRPC_HOST, "localhost");
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_NAME,
                TableSpace.DEFAULT);
        props.setProperty(IndexingServerConfiguration.PROPERTY_INSTANCE_ID,
                String.valueOf(instanceId));
        props.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES,
                String.valueOf(numInstances));
        props.setProperty(IndexingServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH,
                ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT);
        return props;
    }
}
