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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies the two-layer fix for the NoopMerger startup-race (issue #507):
 *
 * <ul>
 *   <li><b>Option A</b> — startup retry: if the initial ZK discovery returns
 *       empty and no static servers are configured, the optimizer retries
 *       {@code listFileServers()} up to {@code PROPERTY_ZK_DISCOVERY_RETRIES}
 *       times. A file server that registers in ZK during the retry window is
 *       picked up and the optimizer resolves a non-NoopMerger at startup.</li>
 *   <li><b>Option B</b> — tick-time upgrade: if all startup retries are
 *       exhausted and the optimizer starts with {@link IndexOptimizerMain.NoopMerger},
 *       each subsequent tick calls {@link IndexOptimizerMain#maybeUpgradeMerger()}.
 *       Once a file server appears in ZK, the engine's merger is replaced
 *       atomically with the upgraded merger.</li>
 * </ul>
 *
 * <p>Both tests use the package-private {@code mergerBuilderForTests} seam
 * (a {@link java.util.function.Function}) so they can inject an
 * {@link InMemorySegmentMerger} without needing a live remote file server.
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
        // Long periodic interval; we drive the engine manually via the tick or with a short interval.
        p.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "3600000");
        p.setProperty(OptimizerConfiguration.PROPERTY_RETENTION_MS, "60000");
        p.setProperty(OptimizerConfiguration.PROPERTY_HTTP_PORT, "0");
        return p;
    }

    /**
     * Option A: the file server registers in ZK while {@code start()} is
     * blocked in its retry loop. The retry picks it up and the optimizer
     * resolves a real (non-NoopMerger) merger at startup.
     *
     * <p>The test sets {@code PROPERTY_ZK_DISCOVERY_RETRIES=5} and
     * {@code PROPERTY_ZK_DISCOVERY_RETRY_INTERVAL_MS=100}. A background
     * thread registers the file server 120 ms after the retry loop starts
     * (so the first retry misses it, the second or later retries catch it).
     * {@code start()} is run in the foreground — the test asserts on the
     * merger that was resolved before any tick runs.
     */
    @Test
    public void startupRetryPicksUpFileServerRegisteredDuringRetryWindow() throws Exception {
        Properties props = baseProps();
        props.setProperty(OptimizerConfiguration.PROPERTY_ZK_DISCOVERY_RETRIES, "5");
        props.setProperty(OptimizerConfiguration.PROPERTY_ZK_DISCOVERY_RETRY_INTERVAL_MS, "100");

        // The merger builder test seam: returns an InMemorySegmentMerger when servers are discovered.
        AtomicBoolean builderCalled = new AtomicBoolean(false);
        IndexOptimizerMain main = new IndexOptimizerMain(new OptimizerConfiguration(props));
        main.mergerBuilderForTests = servers -> {
            builderCalled.set(true);
            return new InMemorySegmentMerger();
        };

        // Register the file server 120 ms from now — the first retry (at 100 ms) will miss
        // it, subsequent retries will find it.
        Thread registrar = new Thread(() -> {
            try {
                Thread.sleep(120);
                registerFileServer();
            } catch (Exception e) {
                throw new RuntimeException("file server registration failed", e);
            }
        }, "test-file-server-registrar");
        registrar.setDaemon(true);
        registrar.start();

        try {
            main.start();
            assertNotNull("engine must be initialised", main.getEngine());
            assertTrue("merger builder test seam must have been called: file server was found by retry",
                    builderCalled.get());
            assertFalse("merger must NOT be a NoopMerger after startup retry picked up the file server",
                    main.getMerger() instanceof IndexOptimizerMain.NoopMerger);
        } finally {
            main.shutdown();
        }
    }

    /**
     * Option B: the optimizer starts with {@link IndexOptimizerMain.NoopMerger}
     * (no file server in ZK at startup, retries exhausted). After startup, a
     * file server registers in ZK. The next tick call upgrades the merger from
     * NoopMerger to the real merger via {@link IndexOptimizerMain#maybeUpgradeMerger()}.
     *
     * <p>The test disables the startup retry loop ({@code retries=0}) so the
     * optimizer always starts with NoopMerger. It then registers the file server
     * and triggers a short-interval tick to observe the upgrade.
     */
    @Test
    public void tickTimeUpgradeReplacesNoopMergerOnceFileServerAppears() throws Exception {
        Properties props = baseProps();
        // Disable startup retries so the optimizer always starts with NoopMerger.
        props.setProperty(OptimizerConfiguration.PROPERTY_ZK_DISCOVERY_RETRIES, "0");
        // Short tick so the upgrade fires quickly.
        props.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "100");

        AtomicBoolean builderCalled = new AtomicBoolean(false);
        IndexOptimizerMain main = new IndexOptimizerMain(new OptimizerConfiguration(props));
        main.mergerBuilderForTests = servers -> {
            builderCalled.set(true);
            return new InMemorySegmentMerger();
        };

        try {
            main.start();
            assertNotNull("engine must be initialised", main.getEngine());
            // At this point, no file server is in ZK and retries are disabled:
            // the optimizer must have fallen back to NoopMerger.
            assertTrue("merger must be NoopMerger immediately after startup with no file server",
                    main.getMerger() instanceof IndexOptimizerMain.NoopMerger);

            // Now register the file server in ZK.
            registerFileServer();

            // Wait for the tick-time upgrade to fire (up to 10 s).
            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline) {
                if (!(main.getMerger() instanceof IndexOptimizerMain.NoopMerger)) {
                    break;
                }
                Thread.sleep(50);
            }

            assertTrue("merger builder test seam must have been called by maybeUpgradeMerger()",
                    builderCalled.get());
            assertFalse("merger must NOT be a NoopMerger after tick-time upgrade",
                    main.getMerger() instanceof IndexOptimizerMain.NoopMerger);
            assertTrue("upgraded merger must be an InMemorySegmentMerger (injected via test seam)",
                    main.getMerger() instanceof InMemorySegmentMerger);
        } finally {
            main.shutdown();
        }
    }
}
