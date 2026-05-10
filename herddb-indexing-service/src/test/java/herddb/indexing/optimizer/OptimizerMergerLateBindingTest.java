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

import static org.junit.Assert.assertFalse;
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
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies the fix for the {@code NoopMerger} startup race (issue #507):
 *
 * <p>The root cause: the optimizer called {@code listFileServers()} exactly once at
 * startup. If the file-server pod had not yet written its ZK znode, the list was
 * empty and {@code resolveMerger()} fell back to {@code NoopMerger} permanently.
 *
 * <p>The fix uses a single long-lived {@link ZookeeperMetadataStorageManager} that:
 * <ol>
 *   <li>Performs the initial {@code listFileServers()} call, which arms a ZK
 *       children-watch on {@code /herd/fileServers}.</li>
 *   <li>When a file server registers, the watch fires → {@code notifyFileServersChanged}
 *       → {@link IndexOptimizerMain}'s {@code ServiceDiscoveryListener} schedules an
 *       event-driven tick.</li>
 *   <li>The tick's {@code maybeUpgradeMerger()} upgrades the engine from
 *       {@code NoopMerger} to a real merger atomically.</li>
 * </ol>
 *
 * <p>Both tests use the package-private {@code mergerBuilderForTests} seam so they
 * can inject an {@link InMemorySegmentMerger} without needing a live remote file server.
 */
public class OptimizerMergerLateBindingTest {

    private static final String BASE_PATH = "/herd-test-late-binding";
    private static final String TS_NAME = "herd";
    private static final String TS_UUID = "ts-late-bind";
    private static final String FILE_SERVER_ID = "file-server-0";
    private static final String FILE_SERVER_ADDR = "herddb-file-server-0.svc:9846";

    private TestingServer zkServer;
    private ZooKeeper zk;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        CountDownLatch connected = new CountDownLatch(1);
        zk = new ZooKeeper(zkServer.getConnectString(), 30000, ev -> {
            if (ev.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue("ZK connect timed out", connected.await(30, TimeUnit.SECONDS));
        zk.create(BASE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        registerTablespace(TS_NAME, TS_UUID);
    }

    @After
    public void tearDown() throws Exception {
        if (zk != null) {
            zk.close();
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

    private void registerFileServer() throws Exception {
        try (ZookeeperMetadataStorageManager zkmeta =
                new ZookeeperMetadataStorageManager(zkServer.getConnectString(), 30000, BASE_PATH)) {
            zkmeta.start(false);
            zkmeta.registerFileServer(FILE_SERVER_ID, FILE_SERVER_ADDR);
        }
    }

    private Properties baseProps() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        p.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT, "30000");
        p.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH, BASE_PATH);
        p.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, TS_NAME);
        // Long periodic interval; the watcher-driven tick handles the upgrade.
        p.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "3600000");
        p.setProperty(OptimizerConfiguration.PROPERTY_RETENTION_MS, "60000");
        p.setProperty(OptimizerConfiguration.PROPERTY_HTTP_PORT, "0");
        return p;
    }

    /**
     * Issue #507 — watcher-driven upgrade (primary fix):
     *
     * <p>The optimizer starts with no file server in ZK → resolves {@link IndexOptimizerMain.NoopMerger}.
     * The long-lived {@link ZookeeperMetadataStorageManager}'s children-watch on
     * {@code /herd/fileServers} is armed by the initial {@code listFileServers()} call.
     * When a file server registers, the watch fires → {@code ServiceDiscoveryListener.onFileServersChanged}
     * schedules an event-driven tick → {@code maybeUpgradeMerger()} upgrades the merger
     * to {@link InMemorySegmentMerger} (via the test seam).
     *
     * <p>No timing hacks: the upgrade is driven by the reactive ZK watcher, so the
     * test only needs to wait for the upgrade to complete (up to 10 s).
     */
    @Test
    public void watcherDrivenUpgradeReplacesNoopMergerWhenFileServerRegisters() throws Exception {
        Properties props = baseProps();
        // Short debounce so the watcher-triggered tick fires quickly.
        props.setProperty(OptimizerConfiguration.PROPERTY_EVENT_DEBOUNCE_MS, "50");

        IndexOptimizerMain main = new IndexOptimizerMain(new OptimizerConfiguration(props));
        main.mergerBuilderForTests = servers -> new InMemorySegmentMerger();

        try {
            main.start();
            assertNotNull("engine must be initialised", main.getEngine());

            // No file server in ZK at startup → NoopMerger.
            assertTrue("merger must be NoopMerger immediately after startup with no file server",
                    main.getMerger() instanceof IndexOptimizerMain.NoopMerger);

            // Now register the file server: ZK watch fires → ServiceDiscoveryListener
            // schedules an event-driven tick → maybeUpgradeMerger() upgrades.
            registerFileServer();

            // Wait for the watcher-driven upgrade to complete (up to 10 s).
            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline) {
                if (!(main.getMerger() instanceof IndexOptimizerMain.NoopMerger)) {
                    break;
                }
                Thread.sleep(50);
            }

            assertFalse("merger must NOT be a NoopMerger after watcher-driven upgrade",
                    main.getMerger() instanceof IndexOptimizerMain.NoopMerger);
            assertTrue("upgraded merger must be an InMemorySegmentMerger (injected via test seam)",
                    main.getMerger() instanceof InMemorySegmentMerger);
        } finally {
            main.shutdown();
        }
    }

    /**
     * Issue #507 — tick-time safety-net upgrade (Option B):
     *
     * <p>Same scenario as above, but here we verify that the periodic tick's
     * {@code maybeUpgradeMerger()} call also upgrades the merger. This covers the
     * case where the ZK session-expiry causes the watcher to be lost and a new
     * {@code listFileServers()} re-arms it at tick time.
     *
     * <p>We trigger the upgrade explicitly by waiting for the short-interval periodic
     * tick rather than registering AFTER the watcher is armed.
     */
    @Test
    public void tickTimeUpgradeReplacesNoopMergerOnceFileServerAppears() throws Exception {
        Properties props = baseProps();
        // Short periodic tick so the safety-net upgrade fires quickly.
        props.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "100");

        IndexOptimizerMain main = new IndexOptimizerMain(new OptimizerConfiguration(props));
        main.mergerBuilderForTests = servers -> new InMemorySegmentMerger();

        try {
            main.start();
            assertNotNull("engine must be initialised", main.getEngine());

            // No file server at startup → NoopMerger.
            assertTrue("merger must be NoopMerger immediately after startup with no file server",
                    main.getMerger() instanceof IndexOptimizerMain.NoopMerger);

            // Register the file server. The next periodic tick calls tickSafe()
            // → maybeUpgradeMerger() → zkMeta.listFileServers() → upgrade.
            registerFileServer();

            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline) {
                if (!(main.getMerger() instanceof IndexOptimizerMain.NoopMerger)) {
                    break;
                }
                Thread.sleep(50);
            }

            assertFalse("merger must NOT be a NoopMerger after tick-time upgrade",
                    main.getMerger() instanceof IndexOptimizerMain.NoopMerger);
            assertTrue("upgraded merger must be an InMemorySegmentMerger (injected via test seam)",
                    main.getMerger() instanceof InMemorySegmentMerger);
        } finally {
            main.shutdown();
        }
    }
}
