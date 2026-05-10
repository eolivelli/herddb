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

import static org.junit.Assert.assertNotEquals;
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
 * End-to-end test for the optimizer's in-process ZooKeeper session-recovery
 * path (issue #504). Forces a real {@code KeeperState.Expired} event by
 * hijacking the optimizer's session id and closing it from a second client
 * (the canonical "kill ZK session" pattern), then asserts:
 * <ul>
 *   <li>{@link IndexOptimizerMain#getSessionReconnects()} crosses zero,</li>
 *   <li>the optimizer's internal {@link ZooKeeper} reference is replaced with
 *       a fresh client (different session id),</li>
 *   <li>the engine can complete a tick after the reconnect.</li>
 * </ul>
 *
 * <p>The fix replaces the old "trip /health to 503 so Helm restarts us"
 * behavior with in-process recovery — long merges no longer race the
 * liveness probe, and a transient ZK outage does not require a pod restart.
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
    public void zkSessionExpiryTriggersInProcessReconnect() throws Exception {
        Properties props = new Properties();
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        // Short ZK session timeout so we can force an expiry quickly.
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT, "4000");
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH, BASE_PATH);
        props.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, TS_NAME);
        // Long periodic interval — we drive ticks via the event-driven path that
        // the reconnect schedules so the test does not depend on timing.
        props.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "60000");
        props.setProperty(OptimizerConfiguration.PROPERTY_RETENTION_MS, "60000");
        props.setProperty(OptimizerConfiguration.PROPERTY_HTTP_PORT, "0");

        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        IndexOptimizerMain main = new IndexOptimizerMain(
                new OptimizerConfiguration(props), merger);
        try {
            main.start();
            assertNotNull(main.getEngine());
            assertTrue("baseline reconnect counter must be 0",
                    main.getSessionReconnects() == 0L);

            ZooKeeper firstZk = extractZkClient(main);
            long firstSessionId = firstZk.getSessionId();
            assertTrue("first session must have a non-zero id", firstSessionId != 0L);

            // Hijack the optimizer's ZK session id + password and close that
            // session from a second client. ZooKeeper considers this "session
            // moved" — the original client's next operation gets
            // SessionExpired, which fires the bootstrap watcher with
            // KeeperState.Expired and triggers an in-process reconnect.
            byte[] passwd = firstZk.getSessionPasswd();
            CountDownLatch kicked = new CountDownLatch(1);
            ZooKeeper kicker = new ZooKeeper(zkServer.getConnectString(), 4000,
                    (WatchedEvent ev) -> {
                        if (ev.getState() == Watcher.Event.KeeperState.SyncConnected) {
                            kicked.countDown();
                        }
                    },
                    firstSessionId, passwd);
            assertTrue("kicker session must establish",
                    kicked.await(10, TimeUnit.SECONDS));
            kicker.close();

            // Wait up to 30 s for the reconnect to land. The bootstrap watcher
            // first sees Expired (after the next ZK keepalive round-trip),
            // schedules the reconnect on the engine scheduler, then opens a
            // fresh session and increments getSessionReconnects().
            long deadline = System.currentTimeMillis() + 30_000L;
            while (System.currentTimeMillis() < deadline && main.getSessionReconnects() == 0L) {
                Thread.sleep(50);
            }
            assertTrue("ZK session expiry must trigger an in-process reconnect; reconnects="
                            + main.getSessionReconnects(),
                    main.getSessionReconnects() >= 1L);

            // The second ZK client must be a different session.
            ZooKeeper secondZk = extractZkClient(main);
            assertNotNull("post-reconnect ZK reference must be non-null", secondZk);
            assertNotEquals("post-reconnect session id must differ from the expired one",
                    firstSessionId, secondZk.getSessionId());

            // Verify the engine is functional after the reconnect by running a tick
            // directly. If the registry / leader-lock are wired to the new ZK,
            // this should succeed without throwing.
            main.getEngine().runOnce();
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
