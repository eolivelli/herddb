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
 * startup via a throw-away {@link ZookeeperMetadataStorageManager}. If the file-server
 * pod had not yet written its ZK znode, the list was empty and {@code resolveMerger()}
 * fell back to {@code NoopMerger} permanently.
 *
 * <p>The fix keeps a single long-lived {@link ZookeeperMetadataStorageManager} open for
 * the pod's lifetime. {@code listFileServers()} arms a ZK children-watch on
 * {@code /herd/fileServers}. When a file server registers, the watch fires →
 * {@code notifyFileServersChanged} → {@link IndexOptimizerMain}'s
 * {@code ServiceDiscoveryListener} schedules an event-driven tick →
 * {@code maybeUpgradeMerger()} replaces the {@link IndexOptimizerMain.NoopMerger}.
 * The periodic tick also calls {@code maybeUpgradeMerger()} as a safety net.
 *
 * <p>All tests use the package-private {@code mergerBuilderForTests} seam to inject an
 * {@link InMemorySegmentMerger} without needing a live remote file server.
 */
public class OptimizerMergerLateBindingTest {

    private static final String BASE_PATH = "/herd-test-late-binding";
    private static final String TS_NAME = "herd";
    private static final String TS_UUID = "ts-late-bind";
    private static final String FILE_SERVER_ID = "file-server-0";
    private static final String FILE_SERVER_ADDR = "herddb-file-server-0.svc:9846";
    private static final String FILE_SERVER_ID_2 = "file-server-1";
    private static final String FILE_SERVER_ADDR_2 = "herddb-file-server-1.svc:9846";

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

    private void registerFileServer(String id, String addr) throws Exception {
        try (ZookeeperMetadataStorageManager zkmeta =
                new ZookeeperMetadataStorageManager(zkServer.getConnectString(), 30000, BASE_PATH)) {
            zkmeta.start(false);
            zkmeta.registerFileServer(id, addr);
        }
    }

    private Properties baseProps() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        p.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT, "30000");
        p.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH, BASE_PATH);
        p.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, TS_NAME);
        // Long periodic interval; the watcher-driven tick handles the upgrade in test 1.
        p.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "3600000");
        p.setProperty(OptimizerConfiguration.PROPERTY_RETENTION_MS, "60000");
        p.setProperty(OptimizerConfiguration.PROPERTY_HTTP_PORT, "0");
        return p;
    }

    /**
     * Issue #507 — watcher-driven upgrade (primary fix path):
     *
     * <p>The optimizer starts with no file server in ZK → resolves
     * {@link IndexOptimizerMain.NoopMerger}. The long-lived
     * {@link ZookeeperMetadataStorageManager}'s children-watch on
     * {@code /herd/fileServers} is armed by the initial {@code listFileServers()} call.
     * When a file server registers, the watch fires →
     * {@code ServiceDiscoveryListener.onFileServersChanged} schedules an event-driven
     * tick → {@code maybeUpgradeMerger()} upgrades the merger to
     * {@link InMemorySegmentMerger} (via the test seam).
     *
     * <p>No timing hacks: the upgrade is driven by the reactive ZK watcher, so the test
     * only needs to wait for the upgrade to complete (up to 10 s).
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
            registerFileServer(FILE_SERVER_ID, FILE_SERVER_ADDR);

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
            // Also verify the engine itself received the new merger so that merge()
            // calls during runOnce() will actually use it (that's the whole point of
            // issue #507 — the engine's call to merger.merge() must not hit NoopMerger).
            assertTrue("engine.getMerger() must also be an InMemorySegmentMerger after upgrade",
                    main.getEngine().getMerger() instanceof InMemorySegmentMerger);
        } finally {
            main.shutdown();
        }
    }

    /**
     * Issue #507 — periodic-tick safety-net upgrade path:
     *
     * <p>Same scenario (no file server at startup → {@link IndexOptimizerMain.NoopMerger}),
     * but here we verify that the periodic tick's {@code maybeUpgradeMerger()} call upgrades
     * the merger independently of the ZK watcher.
     *
     * <p>The watcher path is deliberately disabled by setting a very large
     * {@code EVENT_DEBOUNCE_MS} (one hour), so the watcher-triggered scheduled tick
     * cannot fire within the 10 s test window. The periodic tick (every 100 ms)
     * fires {@code tickSafe()} → {@code maybeUpgradeMerger()} → upgrade.
     *
     * <p>This covers the recovery scenario where the ZK session expires and the
     * watcher is lost: the periodic tick re-arms the watch via
     * {@code zkMeta.listFileServers()} and picks up any newly registered file servers.
     */
    @Test
    public void tickTimeUpgradeReplacesNoopMergerOnceFileServerAppears() throws Exception {
        Properties props = baseProps();
        // Short periodic tick so the safety-net upgrade fires quickly.
        props.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "100");
        // Very large debounce prevents the watcher-triggered (onFileServersChanged)
        // tick from completing within the 10 s test window. This forces the test
        // to exercise the periodic-tick path exclusively.
        props.setProperty(OptimizerConfiguration.PROPERTY_EVENT_DEBOUNCE_MS, "3600000");

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
            // The watcher-triggered tick is suppressed by the huge debounce above.
            registerFileServer(FILE_SERVER_ID, FILE_SERVER_ADDR);

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
            assertTrue("engine.getMerger() must also be an InMemorySegmentMerger after upgrade",
                    main.getEngine().getMerger() instanceof InMemorySegmentMerger);
        } finally {
            main.shutdown();
        }
    }

    /**
     * Issue #542 — upgrade must reach the task consumer.
     *
     * <p>Before the fix, {@link OptimizerTaskConsumer#merger} was {@code private final}
     * and captured the startup-time {@link IndexOptimizerMain.NoopMerger}. Every merge
     * task was then declined by the stale reference even though the engine's own field
     * was correctly upgraded to a real merger. This test verifies that after a
     * watcher-driven upgrade (the same upgrade path tested in
     * {@link #watcherDrivenUpgradeReplacesNoopMergerWhenFileServerRegisters}),
     * {@code main.getTaskConsumer().getMerger()} is no longer a {@link IndexOptimizerMain.NoopMerger}.
     */
    @Test
    public void mergerUpgradePropagatesFromEngineToTaskConsumer() throws Exception {
        Properties props = baseProps();
        // Short debounce so the watcher-triggered tick fires quickly.
        props.setProperty(OptimizerConfiguration.PROPERTY_EVENT_DEBOUNCE_MS, "50");

        IndexOptimizerMain main = new IndexOptimizerMain(new OptimizerConfiguration(props));
        main.mergerBuilderForTests = servers -> new InMemorySegmentMerger();

        try {
            main.start();
            assertNotNull("engine must be initialised", main.getEngine());
            assertNotNull("taskConsumer must be initialised", main.getTaskConsumer());

            // No file server at startup → consumer's merger is also NoopMerger.
            assertTrue("taskConsumer.getMerger() must be NoopMerger before upgrade",
                    main.getTaskConsumer().getMerger() instanceof IndexOptimizerMain.NoopMerger);

            // Register a file server: ZK watch fires → maybeUpgradeMerger() upgrades
            // the engine AND — since issue #542 is fixed — the task consumer.
            registerFileServer(FILE_SERVER_ID, FILE_SERVER_ADDR);

            // Wait for the upgrade to complete (up to 10 s).
            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline) {
                if (!(main.getTaskConsumer().getMerger() instanceof IndexOptimizerMain.NoopMerger)) {
                    break;
                }
                Thread.sleep(50);
            }

            // Core assertion for issue #542: the consumer must see the new merger.
            assertFalse("taskConsumer.getMerger() must NOT be NoopMerger after upgrade (issue #542)",
                    main.getTaskConsumer().getMerger() instanceof IndexOptimizerMain.NoopMerger);
            assertTrue("taskConsumer.getMerger() must be InMemorySegmentMerger after upgrade",
                    main.getTaskConsumer().getMerger() instanceof InMemorySegmentMerger);

            // Sanity: engine and main-level fields are also upgraded (pre-existing behaviour).
            assertFalse("engine.getMerger() must not be NoopMerger",
                    main.getEngine().getMerger() instanceof IndexOptimizerMain.NoopMerger);
            assertFalse("main.getMerger() must not be NoopMerger",
                    main.getMerger() instanceof IndexOptimizerMain.NoopMerger);
        } finally {
            main.shutdown();
        }
    }

    /**
     * Issue #507 — churn resilience: the merger stays real after file-server churn.
     *
     * <p>Once the optimizer upgrades from {@link IndexOptimizerMain.NoopMerger} to a real
     * merger it must not downgrade even when additional {@code onFileServersChanged}
     * callbacks arrive. This test registers a second file server after the initial upgrade
     * and verifies the merger remains an {@link InMemorySegmentMerger} (not re-created or
     * reset to {@link IndexOptimizerMain.NoopMerger}).
     */
    @Test
    public void mergerStaysRealAfterFileServerChurn() throws Exception {
        Properties props = baseProps();
        props.setProperty(OptimizerConfiguration.PROPERTY_EVENT_DEBOUNCE_MS, "50");

        IndexOptimizerMain main = new IndexOptimizerMain(new OptimizerConfiguration(props));
        main.mergerBuilderForTests = servers -> new InMemorySegmentMerger();

        try {
            main.start();
            assertNotNull("engine must be initialised", main.getEngine());
            assertTrue("should start as NoopMerger",
                    main.getMerger() instanceof IndexOptimizerMain.NoopMerger);

            // Register first file server → watcher fires → upgrade.
            registerFileServer(FILE_SERVER_ID, FILE_SERVER_ADDR);

            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline) {
                if (!(main.getMerger() instanceof IndexOptimizerMain.NoopMerger)) {
                    break;
                }
                Thread.sleep(50);
            }
            assertFalse("merger should be real after first registration",
                    main.getMerger() instanceof IndexOptimizerMain.NoopMerger);
            SegmentMerger mergerAfterFirstUpgrade = main.getMerger();

            // Register a second file server (simulates churn / scale-out). The
            // onFileServersChanged callback fires again. Because the merger is already
            // a real merger (not NoopMerger), the listener short-circuits and no
            // upgrade attempt is made. The existing merger must remain unchanged.
            registerFileServer(FILE_SERVER_ID_2, FILE_SERVER_ADDR_2);
            // Give the watcher event a moment to propagate.
            Thread.sleep(500);

            assertFalse("merger must still be real after second file-server registration",
                    main.getMerger() instanceof IndexOptimizerMain.NoopMerger);
            assertTrue("merger must be the same InMemorySegmentMerger instance (not re-created)",
                    main.getMerger() == mergerAfterFirstUpgrade);
        } finally {
            main.shutdown();
        }
    }
}
