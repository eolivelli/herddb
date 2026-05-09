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
package herddb.indexing.optimizer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.model.TableSpace;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end test verifying the ZK-session-expiry → {@code /health} 503
 * wiring on a real {@link IndexOptimizerMain} instance (issue #484, round-2
 * review item B.6).
 *
 * <p>The test does not rely on a synthetic predicate; it triggers an actual
 * {@code KeeperState.Expired} event by hijacking the optimizer's session id
 * and closing it from a second client (the canonical "kill ZK session"
 * pattern). The test then asserts {@link IndexOptimizerMain#isSessionExpired}
 * flips within a deadline.
 */
public class IndexOptimizerSessionExpiredTest {

    private static final String BASE_PATH = "/herd-test-session-expired";
    private static final String TS_NAME = "herd";
    private static final String TS_UUID = "ts-expired";

    private TestingServer zkServer;
    private ZooKeeper bootstrapZk;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        CountDownLatch connected = new CountDownLatch(1);
        bootstrapZk = new ZooKeeper(zkServer.getConnectString(), 30000, ev -> {
            if (ev.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue(connected.await(30, TimeUnit.SECONDS));
        bootstrapZk.create(BASE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        registerTablespace(TS_NAME, TS_UUID);
    }

    @After
    public void tearDown() throws Exception {
        if (bootstrapZk != null) {
            bootstrapZk.close();
        }
        if (zkServer != null) {
            zkServer.close();
        }
    }

    private void registerTablespace(String name, String uuid) throws Exception {
        Set<String> replicas = new HashSet<>();
        replicas.add("test-instance");
        TableSpace ts = TableSpace.builder()
                .name(name)
                .uuid(uuid)
                .leader("test-instance")
                .replicas(replicas)
                .expectedReplicaCount(1)
                .build();
        try (ZookeeperMetadataStorageManager zkmeta =
                new ZookeeperMetadataStorageManager(zkServer.getConnectString(), 30000, BASE_PATH)) {
            zkmeta.start(true);
            zkmeta.registerTableSpace(ts);
        }
    }

    @Test
    public void zkSessionExpiryTrippedAndDetected() throws Exception {
        Properties props = new Properties();
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        // Short ZK session timeout so we can force an expiry quickly.
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT, "4000");
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH, BASE_PATH);
        props.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, TS_NAME);
        props.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "60000");
        props.setProperty(OptimizerConfiguration.PROPERTY_RETENTION_MS, "60000");
        props.setProperty(OptimizerConfiguration.PROPERTY_HTTP_PORT, "0");

        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        IndexOptimizerMain main = new IndexOptimizerMain(
                new OptimizerConfiguration(props), merger);
        try {
            main.start();
            assertNotNull(main.getEngine());
            assertTrue("session must NOT be expired immediately after start",
                    !main.isSessionExpired());

            // Hijack the optimizer's ZK session id + password and close that
            // session from a second client. ZooKeeper considers this "session
            // moved" — the original client's next operation gets
            // SessionExpired, which fires the watcher with KeeperState.Expired.
            ZooKeeper victimSession = extractZkClient(main);
            long sessionId = victimSession.getSessionId();
            byte[] passwd = victimSession.getSessionPasswd();

            CountDownLatch kicked = new CountDownLatch(1);
            ZooKeeper kicker = new ZooKeeper(zkServer.getConnectString(), 4000,
                    (WatchedEvent ev) -> {
                        if (ev.getState() == Watcher.Event.KeeperState.SyncConnected) {
                            kicked.countDown();
                        }
                    },
                    sessionId, passwd);
            assertTrue("kicker session must establish",
                    kicked.await(10, TimeUnit.SECONDS));
            // Closing the kicker session also kills the optimizer's session
            // (same session id) — the original watcher fires Expired.
            kicker.close();

            // Wait up to 10 s for the optimizer's watcher to observe Expired
            // and flip the flag. (The timeout is generous because session
            // expiry detection waits for the next ZK keepalive round-trip.)
            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline && !main.isSessionExpired()) {
                Thread.sleep(50);
            }
            assertTrue("ZK session expiry must trip IndexOptimizerMain.isSessionExpired",
                    main.isSessionExpired());
        } finally {
            main.shutdown();
        }
    }

    /**
     * Reflectively pulls the {@link ZooKeeper} field out of {@link IndexOptimizerMain}.
     * The field is package-private; we use reflection to keep the test file in
     * a different package without leaking it onto the public API.
     */
    private static ZooKeeper extractZkClient(IndexOptimizerMain main) throws Exception {
        java.lang.reflect.Field f = IndexOptimizerMain.class.getDeclaredField("zooKeeper");
        f.setAccessible(true);
        return (ZooKeeper) f.get(main);
    }
}
