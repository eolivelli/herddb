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
import herddb.metadata.ServiceDiscoveryListener;
import herddb.model.TableSpace;
import herddb.remote.RemoteFileServer;
import herddb.remote.RemoteFileServiceClient;
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
 * End-to-end tests that verify the RemoteFileServer is discovered via
 * ZooKeeper and that a HerdDB server in cluster mode with
 * {@code server.storage.mode=remote} can store table data on the
 * remote file server.
 *
 * <p>These tests start a RemoteFileServer, register it in ZooKeeper,
 * then start a HerdDB server in cluster mode with an embedded bookie
 * that discovers the file server via ZK and uses it for page storage.
 *
 * @author enrico.olivelli
 */
public class ZKDiscoveryRemoteFileServerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Starts a single RemoteFileServer, registers it in ZK, starts a
     * HerdDB server in cluster mode with remote storage, and verifies
     * that basic CRUD operations work with data stored on the remote
     * file server.
     */
    @Test
    public void testSingleFileServerDiscoveredViaZK() throws Exception {
        Path zkDir = folder.newFolder("zk").toPath();

        org.apache.curator.test.TestingServer zookeeperServer =
                new org.apache.curator.test.TestingServer(-1, zkDir.toFile(), true);
        try {
            String zkAddress = zookeeperServer.getConnectString();
            String zkPath = "/herddb";

            // Start RemoteFileServer on ephemeral port
            Path fileServerDataDir = folder.newFolder("fileserver1").toPath();
            RemoteFileServer fileServer1 = new RemoteFileServer("localhost", 0, fileServerDataDir);
            fileServer1.start();

            try {
                // Register file server in ZK using its own metadata manager
                herddb.cluster.ZookeeperMetadataStorageManager registrationManager =
                        new herddb.cluster.ZookeeperMetadataStorageManager(zkAddress, 40000, zkPath);
                registrationManager.start();

                try {
                    String addr1 = "localhost:" + fileServer1.getPort();
                    registrationManager.registerFileServer("fs1", addr1);

                    // Verify registration
                    List<String> servers = registrationManager.listFileServers();
                    assertEquals(1, servers.size());
                    String fileServerAddr = servers.get(0);

                    // Prepare HerdDB server properties (cluster mode, embedded bookie, remote storage)
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
                    // Enable remote storage mode
                    serverProps.put(ServerConfiguration.PROPERTY_STORAGE_MODE,
                            ServerConfiguration.PROPERTY_STORAGE_MODE_REMOTE);
                    serverProps.put(ServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS, fileServerAddr);

                    ServerConfiguration serverConfig = new ServerConfiguration(serverProps);
                    try (Server server = new Server(serverConfig)) {
                        server.start();
                        server.waitForStandaloneBoot();

                        try (HDBClient hdbClient = new HDBClient(
                                new ClientConfiguration(folder.newFolder().toPath()))) {
                            hdbClient.setClientSideMetadataProvider(
                                    new ZookeeperClientSideMetadataProvider(
                                            zkAddress, 40000, zkPath));
                            try (HDBConnection con = hdbClient.openConnection()) {
                                // Create table
                                con.executeUpdate(TableSpace.DEFAULT,
                                        "CREATE TABLE tt (n1 string primary key, n2 int)",
                                        0, false, true, Collections.emptyList());

                                // Insert data
                                con.executeUpdate(TableSpace.DEFAULT,
                                        "INSERT INTO tt(n1, n2) VALUES(?, ?)",
                                        0, false, true, Arrays.asList("a", 1));
                                con.executeUpdate(TableSpace.DEFAULT,
                                        "INSERT INTO tt(n1, n2) VALUES(?, ?)",
                                        0, false, true, Arrays.asList("b", 2));
                                con.executeUpdate(TableSpace.DEFAULT,
                                        "INSERT INTO tt(n1, n2) VALUES(?, ?)",
                                        0, false, true, Arrays.asList("c", 3));

                                // Checkpoint to flush pages to remote storage
                                server.getManager().checkpoint();

                                // Select and verify all data
                                try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                                        "SELECT n1, n2 FROM tt ORDER BY n1",
                                        false, Collections.emptyList(), 0, 10, 10, false)) {
                                    List<Map<String, Object>> rows = scan.consume();
                                    assertEquals(3, rows.size());
                                    assertEquals("a", rows.get(0).get("n1").toString());
                                    assertEquals(1, ((Number) rows.get(0).get("n2")).intValue());
                                    assertEquals("b", rows.get(1).get("n1").toString());
                                    assertEquals(2, ((Number) rows.get(1).get("n2")).intValue());
                                    assertEquals("c", rows.get(2).get("n1").toString());
                                    assertEquals(3, ((Number) rows.get(2).get("n2")).intValue());
                                }

                                // Verify count
                                try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                                        "SELECT count(*) as cnt FROM tt",
                                        false, Collections.emptyList(), 0, 10, 10, false)) {
                                    List<Map<String, Object>> rows = scan.consume();
                                    assertEquals(1, rows.size());
                                    assertEquals(3, ((Number) rows.get(0).get("cnt")).intValue());
                                }
                            }
                        }
                    }
                } finally {
                    registrationManager.close();
                }
            } finally {
                fileServer1.stop();
            }
        } finally {
            zookeeperServer.close();
        }
    }

    /**
     * Starts two RemoteFileServers, registers both in ZK, starts a
     * HerdDB server in cluster mode with remote storage pointing to
     * both servers, inserts enough data to create pages, and verifies
     * all data is readable.
     */
    @Test
    public void testTwoFileServersDiscoveredViaZK() throws Exception {
        Path zkDir = folder.newFolder("zk").toPath();

        org.apache.curator.test.TestingServer zookeeperServer =
                new org.apache.curator.test.TestingServer(-1, zkDir.toFile(), true);
        try {
            String zkAddress = zookeeperServer.getConnectString();
            String zkPath = "/herddb";

            // Start two RemoteFileServers on ephemeral ports
            Path fileServerDataDir1 = folder.newFolder("fileserver1").toPath();
            RemoteFileServer fileServer1 = new RemoteFileServer("localhost", 0, fileServerDataDir1);
            fileServer1.start();

            Path fileServerDataDir2 = folder.newFolder("fileserver2").toPath();
            RemoteFileServer fileServer2 = new RemoteFileServer("localhost", 0, fileServerDataDir2);
            fileServer2.start();

            try {
                // Register both file servers in ZK
                herddb.cluster.ZookeeperMetadataStorageManager registrationManager =
                        new herddb.cluster.ZookeeperMetadataStorageManager(zkAddress, 40000, zkPath);
                registrationManager.start();

                try {
                    String addr1 = "localhost:" + fileServer1.getPort();
                    String addr2 = "localhost:" + fileServer2.getPort();
                    registrationManager.registerFileServer("fs1", addr1);
                    // Need a second manager to register the second server (each manager tracks one)
                    herddb.cluster.ZookeeperMetadataStorageManager registrationManager2 =
                            new herddb.cluster.ZookeeperMetadataStorageManager(zkAddress, 40000, zkPath);
                    registrationManager2.start();

                    try {
                        registrationManager2.registerFileServer("fs2", addr2);

                        // Verify both are registered
                        List<String> servers = registrationManager.listFileServers();
                        assertEquals(2, servers.size());

                        // Configure HerdDB Server with both file servers discovered from ZK
                        String allServers = String.join(",", servers);
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
                        // Enable remote storage mode with both servers
                        serverProps.put(ServerConfiguration.PROPERTY_STORAGE_MODE,
                                ServerConfiguration.PROPERTY_STORAGE_MODE_REMOTE);
                        serverProps.put(ServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS, allServers);

                        ServerConfiguration serverConfig = new ServerConfiguration(serverProps);
                        try (Server server = new Server(serverConfig)) {
                            server.start();
                            server.waitForStandaloneBoot();

                            try (HDBClient hdbClient = new HDBClient(
                                    new ClientConfiguration(folder.newFolder().toPath()))) {
                                hdbClient.setClientSideMetadataProvider(
                                        new ZookeeperClientSideMetadataProvider(
                                                zkAddress, 40000, zkPath));
                                try (HDBConnection con = hdbClient.openConnection()) {
                                    // Create table and insert enough data to create pages
                                    con.executeUpdate(TableSpace.DEFAULT,
                                            "CREATE TABLE tt (n1 string primary key, n2 int)",
                                            0, false, true, Collections.emptyList());

                                    for (int i = 0; i < 100; i++) {
                                        con.executeUpdate(TableSpace.DEFAULT,
                                                "INSERT INTO tt(n1, n2) VALUES(?, ?)",
                                                0, false, true,
                                                Arrays.asList("key" + i, i));
                                    }

                                    // Checkpoint to flush pages to remote storage
                                    server.getManager().checkpoint();

                                    // Verify all data is readable
                                    try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                                            "SELECT count(*) as cnt FROM tt",
                                            false, Collections.emptyList(), 0, 10, 10, false)) {
                                        List<Map<String, Object>> rows = scan.consume();
                                        assertEquals(1, rows.size());
                                        assertEquals(100, ((Number) rows.get(0).get("cnt")).intValue());
                                    }

                                    // Also verify individual rows
                                    try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                                            "SELECT n1, n2 FROM tt WHERE n1 = ?",
                                            false, Arrays.asList((Object) "key50"), 0, 10, 10, false)) {
                                        List<Map<String, Object>> rows = scan.consume();
                                        assertEquals(1, rows.size());
                                        assertEquals(50, ((Number) rows.get(0).get("n2")).intValue());
                                    }
                                }
                            }
                        }
                    } finally {
                        registrationManager2.close();
                    }
                } finally {
                    registrationManager.close();
                }
            } finally {
                fileServer2.stop();
                fileServer1.stop();
            }
        } finally {
            zookeeperServer.close();
        }
    }
}
