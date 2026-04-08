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
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.ScanResultSet;
import herddb.client.ZookeeperClientSideMetadataProvider;
import herddb.model.TableSpace;
import herddb.remote.RemoteFileServer;
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

    /**
     * Tests ZK-only discovery (no static {@code remote.file.servers} config).
     * The file server is registered in ZK before the HerdDB server starts.
     * The server must discover the file server via ZK and checkpoint
     * must succeed.
     *
     * <p>This is a regression test for a race condition where the one-shot
     * ZK watcher set by {@code listFileServers()} could fire before the
     * service discovery listener was registered, causing the file server
     * address to be lost and the hash ring to remain empty.
     */
    @Test
    public void testZKOnlyDiscovery_fileServerAlreadyRegistered() throws Exception {
        Path zkDir = folder.newFolder("zk").toPath();

        org.apache.curator.test.TestingServer zookeeperServer =
                new org.apache.curator.test.TestingServer(-1, zkDir.toFile(), true);
        try {
            String zkAddress = zookeeperServer.getConnectString();
            String zkPath = "/herddb";

            // Start RemoteFileServer
            Path fileServerDataDir = folder.newFolder("fileserver").toPath();
            RemoteFileServer fileServer = new RemoteFileServer("localhost", 0, fileServerDataDir);
            fileServer.start();

            try {
                // Register file server in ZK BEFORE starting the HerdDB server
                herddb.cluster.ZookeeperMetadataStorageManager registrationManager =
                        new herddb.cluster.ZookeeperMetadataStorageManager(zkAddress, 40000, zkPath);
                registrationManager.start();

                try {
                    String addr = "localhost:" + fileServer.getPort();
                    registrationManager.registerFileServer("fs1", addr);

                    // Prepare HerdDB server: ZK discovery only (no remote.file.servers)
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
                    serverProps.put(ServerConfiguration.PROPERTY_STORAGE_MODE,
                            ServerConfiguration.PROPERTY_STORAGE_MODE_REMOTE);
                    // NOTE: not setting PROPERTY_REMOTE_FILE_SERVERS — ZK discovery only

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
                                con.executeUpdate(TableSpace.DEFAULT,
                                        "CREATE TABLE tt (n1 string primary key, n2 int)",
                                        0, false, true, Collections.emptyList());

                                for (int i = 0; i < 10; i++) {
                                    con.executeUpdate(TableSpace.DEFAULT,
                                            "INSERT INTO tt(n1, n2) VALUES(?, ?)",
                                            0, false, true,
                                            Arrays.asList("key" + i, i));
                                }

                                // Checkpoint must succeed — this would fail with
                                // "Hash ring is empty" before the fix
                                server.getManager().checkpoint();

                                try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                                        "SELECT count(*) as cnt FROM tt",
                                        false, Collections.emptyList(), 0, 10, 10, false)) {
                                    List<Map<String, Object>> rows = scan.consume();
                                    assertEquals(1, rows.size());
                                    assertEquals(10, ((Number) rows.get(0).get("cnt")).intValue());
                                }
                            }
                        }
                    }
                } finally {
                    registrationManager.close();
                }
            } finally {
                fileServer.stop();
            }
        } finally {
            zookeeperServer.close();
        }
    }

    /**
     * Tests that ZK discovery works after a server restart (when the node ID
     * file already exists on disk). Before the fix, metadataStorageManager was
     * not started before buildDataStorageManager() on the second boot, so ZK
     * discovery silently failed and the hash ring stayed empty.
     */
    @Test
    public void testZKOnlyDiscovery_serverRestart() throws Exception {
        Path zkDir = folder.newFolder("zk").toPath();

        org.apache.curator.test.TestingServer zookeeperServer =
                new org.apache.curator.test.TestingServer(-1, zkDir.toFile(), true);
        try {
            String zkAddress = zookeeperServer.getConnectString();
            String zkPath = "/herddb";

            // Start RemoteFileServer
            Path fileServerDataDir = folder.newFolder("fileserver").toPath();
            RemoteFileServer fileServer = new RemoteFileServer("localhost", 0, fileServerDataDir);
            fileServer.start();

            try {
                // Register file server in ZK
                herddb.cluster.ZookeeperMetadataStorageManager registrationManager =
                        new herddb.cluster.ZookeeperMetadataStorageManager(zkAddress, 40000, zkPath);
                registrationManager.start();

                try {
                    String addr = "localhost:" + fileServer.getPort();
                    registrationManager.registerFileServer("fs1", addr);

                    // Use a fixed base directory so that the node ID persists across restarts
                    String baseDir = folder.newFolder("herddb").getAbsolutePath();

                    Properties serverProps = new Properties();
                    try (InputStream in = SimpleClusterTest.class.getResourceAsStream(
                            "/conf/test.server_cluster.properties")) {
                        serverProps.load(in);
                    }
                    serverProps.put(ServerConfiguration.PROPERTY_BASEDIR, baseDir);
                    serverProps.put(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkAddress);
                    serverProps.put(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, zkPath);
                    serverProps.put("http.enable", "false");
                    serverProps.put(ServerConfiguration.PROPERTY_STORAGE_MODE,
                            ServerConfiguration.PROPERTY_STORAGE_MODE_REMOTE);

                    // --- First boot: generates and persists node ID ---
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
                                con.executeUpdate(TableSpace.DEFAULT,
                                        "CREATE TABLE tt (n1 string primary key, n2 int)",
                                        0, false, true, Collections.emptyList());

                                for (int i = 0; i < 10; i++) {
                                    con.executeUpdate(TableSpace.DEFAULT,
                                            "INSERT INTO tt(n1, n2) VALUES(?, ?)",
                                            0, false, true,
                                            Arrays.asList("key" + i, i));
                                }

                                server.getManager().checkpoint();
                            }
                        }
                    }

                    // --- Second boot: node ID file already exists ---
                    // Before the fix, metadataStorageManager was not started
                    // before buildDataStorageManager(), so ZK discovery failed
                    // and the hash ring stayed empty.
                    ServerConfiguration serverConfig2 = new ServerConfiguration(serverProps);
                    try (Server server = new Server(serverConfig2)) {
                        server.start();
                        server.waitForStandaloneBoot();

                        try (HDBClient hdbClient = new HDBClient(
                                new ClientConfiguration(folder.newFolder().toPath()))) {
                            hdbClient.setClientSideMetadataProvider(
                                    new ZookeeperClientSideMetadataProvider(
                                            zkAddress, 40000, zkPath));
                            try (HDBConnection con = hdbClient.openConnection()) {
                                // Checkpoint must succeed on second boot too
                                server.getManager().checkpoint();

                                try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                                        "SELECT count(*) as cnt FROM tt",
                                        false, Collections.emptyList(), 0, 10, 10, false)) {
                                    List<Map<String, Object>> rows = scan.consume();
                                    assertEquals(1, rows.size());
                                    assertEquals(10, ((Number) rows.get(0).get("cnt")).intValue());
                                }
                            }
                        }
                    }
                } finally {
                    registrationManager.close();
                }
            } finally {
                fileServer.stop();
            }
        } finally {
            zookeeperServer.close();
        }
    }

    /**
     * Tests ZK-only discovery when the file server registers AFTER the
     * HerdDB server has started. The ZK watcher notification must reach
     * the client so that checkpoint succeeds.
     */
    @Test
    public void testZKOnlyDiscovery_fileServerRegistersLate() throws Exception {
        Path zkDir = folder.newFolder("zk").toPath();

        org.apache.curator.test.TestingServer zookeeperServer =
                new org.apache.curator.test.TestingServer(-1, zkDir.toFile(), true);
        try {
            String zkAddress = zookeeperServer.getConnectString();
            String zkPath = "/herddb";

            // Start RemoteFileServer (but don't register in ZK yet)
            Path fileServerDataDir = folder.newFolder("fileserver").toPath();
            RemoteFileServer fileServer = new RemoteFileServer("localhost", 0, fileServerDataDir);
            fileServer.start();

            try {
                // Prepare HerdDB server: ZK discovery only (no remote.file.servers)
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
                serverProps.put(ServerConfiguration.PROPERTY_STORAGE_MODE,
                        ServerConfiguration.PROPERTY_STORAGE_MODE_REMOTE);
                // NOTE: not setting PROPERTY_REMOTE_FILE_SERVERS — ZK discovery only

                ServerConfiguration serverConfig = new ServerConfiguration(serverProps);
                try (Server server = new Server(serverConfig)) {
                    server.start();
                    server.waitForStandaloneBoot();

                    // Now register the file server in ZK — the watcher should notify the client
                    herddb.cluster.ZookeeperMetadataStorageManager registrationManager =
                            new herddb.cluster.ZookeeperMetadataStorageManager(zkAddress, 40000, zkPath);
                    registrationManager.start();

                    try {
                        String addr = "localhost:" + fileServer.getPort();
                        registrationManager.registerFileServer("fs1", addr);

                        // Give ZK watcher time to propagate
                        Thread.sleep(2000);

                        try (HDBClient hdbClient = new HDBClient(
                                new ClientConfiguration(folder.newFolder().toPath()))) {
                            hdbClient.setClientSideMetadataProvider(
                                    new ZookeeperClientSideMetadataProvider(
                                            zkAddress, 40000, zkPath));
                            try (HDBConnection con = hdbClient.openConnection()) {
                                con.executeUpdate(TableSpace.DEFAULT,
                                        "CREATE TABLE tt (n1 string primary key, n2 int)",
                                        0, false, true, Collections.emptyList());

                                for (int i = 0; i < 10; i++) {
                                    con.executeUpdate(TableSpace.DEFAULT,
                                            "INSERT INTO tt(n1, n2) VALUES(?, ?)",
                                            0, false, true,
                                            Arrays.asList("key" + i, i));
                                }

                                // Checkpoint must succeed after late discovery
                                server.getManager().checkpoint();

                                try (ScanResultSet scan = con.executeScan(TableSpace.DEFAULT,
                                        "SELECT count(*) as cnt FROM tt",
                                        false, Collections.emptyList(), 0, 10, 10, false)) {
                                    List<Map<String, Object>> rows = scan.consume();
                                    assertEquals(1, rows.size());
                                    assertEquals(10, ((Number) rows.get(0).get("cnt")).intValue());
                                }
                            }
                        }
                    } finally {
                        registrationManager.close();
                    }
                }
            } finally {
                fileServer.stop();
            }
        } finally {
            zookeeperServer.close();
        }
    }
}
