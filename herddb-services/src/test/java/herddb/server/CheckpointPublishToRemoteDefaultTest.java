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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.ZookeeperClientSideMetadataProvider;
import herddb.model.TableSpace;
import herddb.remote.RemoteFileServer;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.curator.test.TestingServer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #522.
 *
 * <p>Verifies that when {@code server.storage.mode=remote} the server enables
 * checkpoint metadata publication ({@code server.checkpoint.publish.to.remote})
 * by default, without requiring the operator to set the flag explicitly. Also
 * verifies that an explicit {@code false} override is honoured.
 *
 * <p>The check looks for {@code _metadata/latest.checkpoint} under the remote
 * file server's data directory. That file is written by
 * {@link herddb.remote.SharedCheckpointMetadataManager#writeCheckpointLsn} and
 * is the atomic commit marker that the Index Optimizer and read replicas use to
 * discover a consistent checkpoint snapshot.
 */
public class CheckpointPublishToRemoteDefaultTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * With {@code server.storage.mode=remote} and the property NOT set,
     * the server must enable checkpoint metadata publication by default.
     * After a manual checkpoint the {@code _metadata/latest.checkpoint} file
     * must exist somewhere under the file server's data directory.
     */
    @Test
    public void testDefaultPublishToRemoteIsEnabledInRemoteMode() throws Exception {
        Path zkDir = folder.newFolder("zk").toPath();
        try (TestingServer zookeeperServer = new TestingServer(-1, zkDir.toFile(), true)) {
            String zkAddress = zookeeperServer.getConnectString();
            String zkPath = "/herddb";

            Path fileServerDataDir = folder.newFolder("fileserver").toPath();
            RemoteFileServer fileServer = new RemoteFileServer("localhost", 0, fileServerDataDir);
            fileServer.start();
            try {
                String addr = "localhost:" + fileServer.getPort();
                Properties serverProps = loadClusterProps(zkAddress, zkPath);
                serverProps.put(ServerConfiguration.PROPERTY_STORAGE_MODE,
                        ServerConfiguration.PROPERTY_STORAGE_MODE_REMOTE);
                serverProps.put(ServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS, addr);
                // Deliberately NOT setting PROPERTY_CHECKPOINT_PUBLISH_TO_REMOTE —
                // the server must enable it automatically in remote mode.

                try (Server server = new Server(new ServerConfiguration(serverProps))) {
                    server.start();
                    server.waitForStandaloneBoot();

                    try (HDBClient hdbClient = new HDBClient(
                            new ClientConfiguration(folder.newFolder().toPath()))) {
                        hdbClient.setClientSideMetadataProvider(
                                new ZookeeperClientSideMetadataProvider(zkAddress, 40000, zkPath));
                        try (HDBConnection con = hdbClient.openConnection()) {
                            con.executeUpdate(TableSpace.DEFAULT,
                                    "CREATE TABLE t1 (k string primary key, n int)",
                                    0, false, true, Collections.emptyList());
                            for (int i = 0; i < 10; i++) {
                                con.executeUpdate(TableSpace.DEFAULT,
                                        "INSERT INTO t1(k,n) VALUES(?,?)",
                                        0, false, true, Arrays.asList("row" + i, i));
                            }
                        }
                    }

                    // Trigger a checkpoint — this is when latest.checkpoint is written.
                    server.getManager().checkpoint();
                }

                assertTrue(
                        "latest.checkpoint must exist on the remote file server after a checkpoint "
                                + "when storage.mode=remote and publishToRemote is not explicitly set",
                        latestCheckpointExists(fileServerDataDir));
            } finally {
                fileServer.stop();
            }
        }
    }

    /**
     * With {@code server.checkpoint.publish.to.remote=false} the server must
     * suppress checkpoint metadata publication even in remote storage mode.
     * After a manual checkpoint the {@code _metadata/latest.checkpoint} file
     * must NOT exist under the file server's data directory.
     */
    @Test
    public void testExplicitFalseDisablesPublishToRemote() throws Exception {
        Path zkDir = folder.newFolder("zk").toPath();
        try (TestingServer zookeeperServer = new TestingServer(-1, zkDir.toFile(), true)) {
            String zkAddress = zookeeperServer.getConnectString();
            String zkPath = "/herddb";

            Path fileServerDataDir = folder.newFolder("fileserver").toPath();
            RemoteFileServer fileServer = new RemoteFileServer("localhost", 0, fileServerDataDir);
            fileServer.start();
            try {
                String addr = "localhost:" + fileServer.getPort();
                Properties serverProps = loadClusterProps(zkAddress, zkPath);
                serverProps.put(ServerConfiguration.PROPERTY_STORAGE_MODE,
                        ServerConfiguration.PROPERTY_STORAGE_MODE_REMOTE);
                serverProps.put(ServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS, addr);
                // Explicit override: operator disables publication.
                serverProps.put(ServerConfiguration.PROPERTY_CHECKPOINT_PUBLISH_TO_REMOTE, "false");

                try (Server server = new Server(new ServerConfiguration(serverProps))) {
                    server.start();
                    server.waitForStandaloneBoot();

                    try (HDBClient hdbClient = new HDBClient(
                            new ClientConfiguration(folder.newFolder().toPath()))) {
                        hdbClient.setClientSideMetadataProvider(
                                new ZookeeperClientSideMetadataProvider(zkAddress, 40000, zkPath));
                        try (HDBConnection con = hdbClient.openConnection()) {
                            con.executeUpdate(TableSpace.DEFAULT,
                                    "CREATE TABLE t1 (k string primary key, n int)",
                                    0, false, true, Collections.emptyList());
                            for (int i = 0; i < 10; i++) {
                                con.executeUpdate(TableSpace.DEFAULT,
                                        "INSERT INTO t1(k,n) VALUES(?,?)",
                                        0, false, true, Arrays.asList("row" + i, i));
                            }
                        }
                    }

                    server.getManager().checkpoint();
                }

                assertFalse(
                        "latest.checkpoint must NOT exist on the remote file server when "
                                + ServerConfiguration.PROPERTY_CHECKPOINT_PUBLISH_TO_REMOTE + "=false",
                        latestCheckpointExists(fileServerDataDir));
            } finally {
                fileServer.stop();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} if any {@code _metadata/latest.checkpoint} file exists
     * anywhere under {@code rootDir} (the tablespace UUID is not known at test-write time,
     * so we scan recursively).
     */
    private static boolean latestCheckpointExists(Path rootDir) throws IOException {
        try (Stream<Path> stream = Files.walk(rootDir)) {
            return stream.anyMatch(p -> p.getFileName() != null
                    && "latest.checkpoint".equals(p.getFileName().toString())
                    && p.getParent() != null
                    && "_metadata".equals(p.getParent().getFileName().toString()));
        }
    }

    private Properties loadClusterProps(String zkAddress, String zkPath) throws Exception {
        Properties props = new Properties();
        try (InputStream in = CheckpointPublishToRemoteDefaultTest.class.getResourceAsStream(
                "/conf/test.server_cluster.properties")) {
            props.load(in);
        }
        props.put(ServerConfiguration.PROPERTY_BASEDIR,
                folder.newFolder("herddb").getAbsolutePath());
        props.put(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkAddress);
        props.put(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, zkPath);
        props.put("http.enable", "false");
        return props;
    }
}
