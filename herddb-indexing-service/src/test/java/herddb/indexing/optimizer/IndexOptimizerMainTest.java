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
import static org.junit.Assert.fail;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogSequenceNumber;
import herddb.model.TableSpace;
import java.util.List;
import java.util.Properties;
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
 * Bootstraps the full {@link IndexOptimizerMain} against a real curator-test
 * ZK and verifies that the scheduler ticks the engine and applies the merge
 * policy end-to-end.
 *
 * <p>Issue #481: tests also verify that the optimizer resolves the tablespace
 * UUID from the human-readable name via
 * {@link ZookeeperMetadataStorageManager#describeTableSpace(String)}.
 */
public class IndexOptimizerMainTest {

    private static final String BASE_PATH = "/herd-test-step5-main";
    private static final String TS_NAME = "mytablespace";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";

    private TestingServer zkServer;
    private ZooKeeper provisioningZk;
    private SegmentRegistryClient registry;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        CountDownLatch connected = new CountDownLatch(1);
        provisioningZk = new ZooKeeper(zkServer.getConnectString(), 30000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue("ZK connect timed out", connected.await(30, TimeUnit.SECONDS));
        provisioningZk.create(BASE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        registry = new SegmentRegistryClient(() -> provisioningZk, BASE_PATH);
        registry.ensureRoot();
    }

    @After
    public void tearDown() throws Exception {
        if (provisioningZk != null) {
            provisioningZk.close();
        }
        if (zkServer != null) {
            zkServer.close();
        }
    }

    /**
     * Registers a {@link TableSpace} with the given name and UUID into the
     * ZookeeperMetadataStorageManager so the optimizer can resolve it by name.
     */
    private void registerTablespace(String name, String uuid) throws Exception {
        try (ZookeeperMetadataStorageManager zkmeta =
                new ZookeeperMetadataStorageManager(
                        zkServer.getConnectString(), 30000, BASE_PATH)) {
            // start(true) so it creates the cluster metadata paths (tableSpaces, replicas, etc.)
            zkmeta.start(true);
            TableSpace ts = TableSpace.builder()
                    .name(name)
                    .uuid(uuid)
                    .leader("node1")
                    .replica("node1")
                    .expectedReplicaCount(1)
                    .build();
            zkmeta.registerTableSpace(ts);
        }
    }

    private SegmentMetadata sampleSegment(String segUuid, long sizeBytes) {
        return SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .graphPath("g/" + segUuid)
                .mapPath("m/" + segUuid)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(sizeBytes)
                .vectorCount(10L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
    }

    @Test
    public void mainBootstrapTicksEngineAndCompletesMerge() throws Exception {
        // Register the tablespace so the optimizer can resolve the UUID by name.
        registerTablespace(TS_NAME, TS_UUID);

        // Pre-seed three small segments. The AggressivePolicy fires whenever ≥2
        // sub-target segments exist; with maxCount=100 all three are merged
        // into a single output in one cycle.
        for (int i = 0; i < 3; i++) {
            registry.createSegment(sampleSegment("seg-" + i, 100L));
        }

        Properties props = new Properties();
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT, "30000");
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH, BASE_PATH);
        props.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, TS_NAME);
        props.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "100");
        props.setProperty(OptimizerConfiguration.PROPERTY_MAX_COUNT, "100");
        props.setProperty(OptimizerConfiguration.PROPERTY_MAX_BYTES, "9999999999");
        props.setProperty(OptimizerConfiguration.PROPERTY_TARGET_MAX_BYTES, "9999999999");
        props.setProperty(OptimizerConfiguration.PROPERTY_RETENTION_MS, "60000");
        // Disable event-driven debounce so the test's deterministic single-tick
        // assertion (1 merge invocation) doesn't race with watcher-driven
        // wakeups from the createSegment / deprecate side-effects.
        props.setProperty(OptimizerConfiguration.PROPERTY_EVENT_DEBOUNCE_MS, "60000");
        // Step 2 default is LEAST_LOADED which requires live IS instance ephemerals
        // in ZK — this single-pod test doesn't register any, so pin to FIXED_ZERO
        // to preserve the pre-step-2 "owner = 0" semantics for the merge output.
        props.setProperty(OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_POLICY, "FIXED_ZERO");
        // Step 7: pin role to LEADER so the single-pod test reliably drives
        // the producer + consumer through tickSafe (the default "auto" detection
        // falls back to WORKER when HOSTNAME doesn't match the StatefulSet regex).
        props.setProperty(OptimizerConfiguration.PROPERTY_ROLE_IS_LEADER, "true");
        // orphan.reset.ms must be >= 2 × session timeout (startup validation).
        props.setProperty(OptimizerConfiguration.PROPERTY_TASKS_ORPHAN_RESET_MS, "120000");

        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        IndexOptimizerMain main = new IndexOptimizerMain(new OptimizerConfiguration(props), merger);
        try {
            main.start();
            assertNotNull(main.getEngine());

            // The scheduler ticks every 100ms; wait up to 10s for the merge to land.
            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline) {
                if (merger.getInvocationCount() > 0
                        && main.getEngine().getSegmentsMerged() > 0) {
                    break;
                }
                Thread.sleep(50);
            }

            List<VersionedSegmentMetadata> all = registry.listSegments(TS_UUID, IDX_UUID);
            assertEquals(4, all.size()); // 3 deprecated + 1 active output
            long active = all.stream()
                    .filter(v -> v.metadata().getState() == SegmentState.ACTIVE).count();
            long deprecated = all.stream()
                    .filter(v -> v.metadata().getState() == SegmentState.DEPRECATED).count();
            assertEquals(1, active);
            assertEquals(3, deprecated);
        } finally {
            main.shutdown();
        }
    }

    /**
     * Verifies that the optimizer resolves the tablespace UUID from the human-readable
     * name via ZookeeperMetadataStorageManager when only the name is configured.
     */
    @Test
    public void resolveTablespaceUuidByName() throws Exception {
        registerTablespace("herd", "resolved-uuid-abc");

        Properties props = new Properties();
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT, "30000");
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH, BASE_PATH);
        // Only name — no UUID.
        props.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, "herd");
        props.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "60000"); // long interval — we don't need ticks
        props.setProperty(OptimizerConfiguration.PROPERTY_RETENTION_MS, "60000");

        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        IndexOptimizerMain main = new IndexOptimizerMain(new OptimizerConfiguration(props), merger);
        try {
            // start() must succeed and resolve the UUID without error
            main.start();
            assertNotNull("engine must be initialised after start()", main.getEngine());
        } finally {
            main.shutdown();
        }
    }

    /**
     * When neither tablespace name nor UUID is configured, start() must throw
     * an {@link IllegalStateException} before touching ZooKeeper.
     */
    @Test
    public void missingTablespaceNameFailsFast() {
        Properties props = new Properties();
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        // No tablespace name set.
        IndexOptimizerMain main = new IndexOptimizerMain(new OptimizerConfiguration(props),
                new InMemorySegmentMerger());
        try {
            main.start();
            fail("expected IllegalStateException");
        } catch (IllegalStateException ok) {
            assertTrue("message must mention the property name, got: " + ok.getMessage(),
                    ok.getMessage().contains("tablespace.name"));
        } catch (Exception other) {
            fail("expected IllegalStateException but got " + other);
        } finally {
            main.shutdown();
        }
    }

    /**
     * When the configured tablespace name does not exist in ZooKeeper, start()
     * must throw an {@link IllegalStateException} with a clear message.
     */
    @Test
    public void unknownTablespaceNameFailsFast() throws Exception {
        // No tablespace registered — ZK path does not exist.
        Properties props = new Properties();
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT, "30000");
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH, BASE_PATH);
        props.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, "nonexistent");
        props.setProperty(OptimizerConfiguration.PROPERTY_RETENTION_MS, "60000");

        IndexOptimizerMain main = new IndexOptimizerMain(new OptimizerConfiguration(props),
                new InMemorySegmentMerger());
        try {
            main.start();
            fail("expected IllegalStateException for unknown tablespace name");
        } catch (IllegalStateException ok) {
            assertTrue("message must mention the tablespace name, got: " + ok.getMessage(),
                    ok.getMessage().contains("nonexistent"));
        } catch (Exception other) {
            fail("expected IllegalStateException but got " + other);
        } finally {
            main.shutdown();
        }
    }

    @Test
    public void noopMergerLoadedWhenNoSpiProviderRegistered() {
        // No META-INF/services/herddb.indexing.optimizer.SegmentMerger file is shipped, so the
        // SPI loader returns the NoopMerger fallback.
        SegmentMerger m = IndexOptimizerMain.loadMergerSpi();
        assertNotNull(m);
        assertTrue("default fallback must be NoopMerger but was " + m.getClass().getName(),
                m instanceof IndexOptimizerMain.NoopMerger);
    }
}
