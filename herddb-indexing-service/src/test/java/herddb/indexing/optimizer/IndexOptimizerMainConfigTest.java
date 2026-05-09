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

import static org.junit.Assert.assertEquals;
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
 * Verifies the merger-resolution chain in {@link IndexOptimizerMain} (issue #484):
 * <ul>
 *   <li>When the constructor is given a pre-configured merger, that merger is
 *       used verbatim regardless of configuration (test override path).</li>
 *   <li>When no merger is preconfigured AND no remote-file servers are available
 *       (neither static nor via ZK discovery), the optimizer falls back to a
 *       {@link IndexOptimizerMain.NoopMerger}. This is the unit-test convenience
 *       path that lets the registry plumbing exercise without provisioning a
 *       remote file service.</li>
 *   <li>{@link IndexOptimizerMain#loadMergerSpi()} continues to return the
 *       NoopMerger fallback when no SPI provider is registered (preserved
 *       for backward compat with existing tests).</li>
 * </ul>
 */
public class IndexOptimizerMainConfigTest {

    private static final String BASE_PATH = "/herd-test-cfg";
    private static final String TS_NAME = "herd";
    private static final String TS_UUID = "ts-cfg";

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
        assertTrue(connected.await(30, TimeUnit.SECONDS));
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

    private Properties baseProps() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        p.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT, "30000");
        p.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH, BASE_PATH);
        p.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, TS_NAME);
        p.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "3600000");
        p.setProperty(OptimizerConfiguration.PROPERTY_RETENTION_MS, "60000");
        p.setProperty(OptimizerConfiguration.PROPERTY_HTTP_PORT, "0");
        return p;
    }

    @Test
    public void preConfiguredMergerIsUsedVerbatim() throws Exception {
        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        IndexOptimizerMain main = new IndexOptimizerMain(
                new OptimizerConfiguration(baseProps()), merger);
        try {
            main.start();
            assertNotNull(main.getMerger());
            assertEquals("preconfigured merger must be used as-is",
                    merger, main.getMerger());
        } finally {
            main.shutdown();
        }
    }

    @Test
    public void noServersConfiguredFallsBackToNoopMerger() throws Exception {
        // No PROPERTY_REMOTE_FILE_SERVERS, and the test ZK has no /herd/file-servers
        // znode (we never registered one), so the optimizer must fall back to
        // NoopMerger rather than failing startup.
        IndexOptimizerMain main = new IndexOptimizerMain(
                new OptimizerConfiguration(baseProps()));
        try {
            main.start();
            assertNotNull(main.getMerger());
            assertTrue("expected NoopMerger fallback, got " + main.getMerger().getClass().getName(),
                    main.getMerger() instanceof IndexOptimizerMain.NoopMerger);
        } finally {
            main.shutdown();
        }
    }

    @Test
    public void loadMergerSpiReturnsNoopFallback() {
        SegmentMerger m = IndexOptimizerMain.loadMergerSpi();
        assertNotNull(m);
        assertTrue("backward-compat helper must still return NoopMerger fallback",
                m instanceof IndexOptimizerMain.NoopMerger);
    }

    /**
     * Issue #485 (PR #494 final-review item B.1): the optimizer pod runs in
     * a separate process and does not see the IS-side
     * {@code vector.index.compaction.streaming.enabled} key. Operators must
     * set {@code indexoptimizer.merge.streaming.enabled} on the optimizer's
     * own configuration to flip the streaming engine off there.
     *
     * <p>Verifies that {@link IndexOptimizerMain#start()} reads the key and
     * propagates the value to the process-wide
     * {@code VectorIndexCompactor.streamingCompactionEnabled} static.
     */
    @Test
    public void optimizerHonorsStreamingCompactionConfigKey() throws Exception {
        boolean originalFlag =
                herddb.index.vector.PersistentVectorStore.isStreamingCompactionEnabled();
        try {
            // Set the key to false and assert the static was flipped after start().
            Properties propsOff = baseProps();
            propsOff.setProperty(
                    OptimizerConfiguration.PROPERTY_MERGE_STREAMING_ENABLED, "false");
            IndexOptimizerMain mainOff = new IndexOptimizerMain(
                    new OptimizerConfiguration(propsOff));
            try {
                mainOff.start();
                assertEquals("streaming flag must be false after start() with config key=false",
                        false,
                        herddb.index.vector.PersistentVectorStore.isStreamingCompactionEnabled());
            } finally {
                mainOff.shutdown();
            }

            // Set the key to true and assert the static was flipped back.
            Properties propsOn = baseProps();
            propsOn.setProperty(
                    OptimizerConfiguration.PROPERTY_MERGE_STREAMING_ENABLED, "true");
            IndexOptimizerMain mainOn = new IndexOptimizerMain(
                    new OptimizerConfiguration(propsOn));
            try {
                mainOn.start();
                assertEquals("streaming flag must be true after start() with config key=true",
                        true,
                        herddb.index.vector.PersistentVectorStore.isStreamingCompactionEnabled());
            } finally {
                mainOn.shutdown();
            }

            // Unset the key entirely → default = true.
            Properties propsDefault = baseProps();
            // (no streaming key set)
            IndexOptimizerMain mainDefault = new IndexOptimizerMain(
                    new OptimizerConfiguration(propsDefault));
            try {
                mainDefault.start();
                assertEquals("streaming flag must default to true when key is absent",
                        true,
                        herddb.index.vector.PersistentVectorStore.isStreamingCompactionEnabled());
            } finally {
                mainDefault.shutdown();
            }
        } finally {
            herddb.index.vector.PersistentVectorStore.setStreamingCompactionEnabled(originalFlag);
        }
    }
}
