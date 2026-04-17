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
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.core.AbstractTableManager;
import herddb.core.TableManager;
import herddb.model.TableSpace;
import herddb.remote.RemoteFileServer;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Cluster + remote-storage regression test for issue #145. Exercises the same
 * Phase-B COMMIT race as
 * {@link herddb.core.CheckpointStaleLsnRecoveryTest} but with a full HerdDB
 * {@link Server} in cluster mode ({@link herddb.cluster.BookkeeperCommitLog})
 * backed by a {@link RemoteFileServer} registered via ZooKeeper.
 */
public class CheckpointStaleLsnRemoteStorageTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testRecoveryAfterCommitDuringPhaseB_RemoteStorage() throws Exception {
        Path zkDir = folder.newFolder("zk").toPath();
        try (TestingServer zookeeperServer = new TestingServer(-1, zkDir.toFile(), true)) {
            String zkAddress = zookeeperServer.getConnectString();
            String zkPath = "/herddb";

            Path fileServerDataDir = folder.newFolder("fileserver").toPath();
            RemoteFileServer fileServer = new RemoteFileServer("localhost", 0, fileServerDataDir);
            fileServer.start();
            try {
                ZookeeperMetadataStorageManager registrationManager =
                        new ZookeeperMetadataStorageManager(zkAddress, 40000, zkPath);
                registrationManager.start();
                try {
                    String addr = "localhost:" + fileServer.getPort();
                    registrationManager.registerFileServer("fs1", addr);

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
                    serverProps.put(ServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS, addr);

                    // ---- First boot: load data and run the checkpoint with the Phase-B hook ----
                    try (Server server = new Server(new ServerConfiguration(serverProps))) {
                        server.start();
                        server.waitForStandaloneBoot();

                        try (HDBClient hdbClient = new HDBClient(
                                new ClientConfiguration(folder.newFolder().toPath()))) {
                            hdbClient.setClientSideMetadataProvider(
                                    new ZookeeperClientSideMetadataProvider(
                                            zkAddress, 40000, zkPath));
                            try (HDBConnection con = hdbClient.openConnection()) {
                                con.executeUpdate(TableSpace.DEFAULT,
                                        "CREATE TABLE t1 (k string primary key, n int)",
                                        0, false, true, Collections.emptyList());
                                for (int i = 0; i < 30; i++) {
                                    con.executeUpdate(TableSpace.DEFAULT,
                                            "INSERT INTO t1(k,n) VALUES(?, ?)",
                                            0, false, true, Arrays.asList("pre" + i, i));
                                }
                                server.getManager().checkpoint();

                                long tx = con.beginTransaction(TableSpace.DEFAULT);
                                for (int i = 0; i < 5; i++) {
                                    con.executeUpdate(TableSpace.DEFAULT,
                                            "INSERT INTO t1(k,n) VALUES(?, ?)",
                                            tx, false, true, Arrays.asList("tx" + i, 100 + i));
                                }

                                AtomicReference<Throwable> hookError = new AtomicReference<>();
                                AbstractTableManager t1Manager = server.getManager()
                                        .getTableSpaceManager(TableSpace.DEFAULT)
                                        .getTableManager("t1");
                                ((TableManager) t1Manager).setDuringPhaseBAction(() -> {
                                    try {
                                        con.commitTransaction(TableSpace.DEFAULT, tx);
                                    } catch (Throwable t) {
                                        hookError.set(t);
                                    }
                                });

                                server.getManager().checkpoint();
                                if (hookError.get() != null) {
                                    throw new AssertionError("commit-during-Phase-B hook failed",
                                            hookError.get());
                                }
                            }
                        }
                    }

                    // ---- Second boot: recovery must succeed and all rows must be visible ----
                    try (Server server = new Server(new ServerConfiguration(serverProps))) {
                        server.start();
                        server.waitForStandaloneBoot();

                        try (HDBClient hdbClient = new HDBClient(
                                new ClientConfiguration(folder.newFolder().toPath()))) {
                            hdbClient.setClientSideMetadataProvider(
                                    new ZookeeperClientSideMetadataProvider(
                                            zkAddress, 40000, zkPath));
                            try (HDBConnection con = hdbClient.openConnection()) {
                                try (ScanResultSet s = con.executeScan(TableSpace.DEFAULT,
                                        "SELECT count(*) as cnt FROM t1",
                                        false, Collections.emptyList(), 0, 10, 10, false)) {
                                    List<Map<String, Object>> rows = s.consume();
                                    assertEquals(1, rows.size());
                                    assertEquals(35, ((Number) rows.get(0).get("cnt")).intValue());
                                }

                                try (ScanResultSet s = con.executeScan(TableSpace.DEFAULT,
                                        "SELECT count(*) as cnt FROM t1 WHERE n>=100",
                                        false, Collections.emptyList(), 0, 10, 10, false)) {
                                    List<Map<String, Object>> rows = s.consume();
                                    assertEquals(5, ((Number) rows.get(0).get("cnt")).intValue());
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
        }
    }
}
