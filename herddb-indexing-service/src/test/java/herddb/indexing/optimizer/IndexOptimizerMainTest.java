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
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogSequenceNumber;
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
 */
public class IndexOptimizerMainTest {

    private static final String BASE_PATH = "/herd-test-step5-main";
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
        // Pre-seed enough segments to force-fire (maxCount=2 < 3 segments).
        for (int i = 0; i < 3; i++) {
            registry.createSegment(sampleSegment("seg-" + i, 100L));
        }

        Properties props = new Properties();
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT, "30000");
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH, BASE_PATH);
        props.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_UUID, TS_UUID);
        props.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "100");
        props.setProperty(OptimizerConfiguration.PROPERTY_MIN_COUNT, "4");
        props.setProperty(OptimizerConfiguration.PROPERTY_MAX_COUNT, "2");
        props.setProperty(OptimizerConfiguration.PROPERTY_MIN_BYTES, "9999999999");
        props.setProperty(OptimizerConfiguration.PROPERTY_MAX_BYTES, "9999999999");
        props.setProperty(OptimizerConfiguration.PROPERTY_RETENTION_MS, "60000");

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

    @Test
    public void missingTablespaceUuidFailsFast() {
        Properties props = new Properties();
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        // No tablespace UUID set.
        IndexOptimizerMain main = new IndexOptimizerMain(new OptimizerConfiguration(props),
                new InMemorySegmentMerger());
        try {
            main.start();
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            assertTrue(ok.getMessage().contains("tablespace.uuid"));
        } catch (Exception other) {
            fail("expected IllegalArgumentException but got " + other);
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
