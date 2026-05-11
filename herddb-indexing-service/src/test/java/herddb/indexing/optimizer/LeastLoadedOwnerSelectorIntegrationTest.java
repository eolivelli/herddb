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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.log.LogSequenceNumber;
import herddb.metadata.IndexingServiceInstanceDescriptor;
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
 * Integration tests for {@link LeastLoadedOwnerSelector} wired through
 * {@link ZkIndexingServiceInstanceDirectory} + {@link RegistryBackedInstanceLoadProbe}
 * against a real (test-server) ZooKeeper. Exercises the step-2 default for
 * production optimizer pods.
 */
public class LeastLoadedOwnerSelectorIntegrationTest {

    private static final String BASE_PATH = "/herd-test-step2";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;
    private ZookeeperMetadataStorageManager zkMeta;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        CountDownLatch connected = new CountDownLatch(1);
        zk = new ZooKeeper(zkServer.getConnectString(), 30000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue(connected.await(30, TimeUnit.SECONDS));
        zk.create(BASE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        registry = new SegmentRegistryClient(() -> zk, BASE_PATH);
        registry.ensureRoot();
        zkMeta = new ZookeeperMetadataStorageManager(zkServer.getConnectString(), 30000, BASE_PATH);
        zkMeta.start(true);
    }

    @After
    public void tearDown() throws Exception {
        if (zkMeta != null) {
            zkMeta.close();
        }
        if (zk != null) {
            zk.close();
        }
        if (zkServer != null) {
            zkServer.close();
        }
    }

    private void registerPrimary(int ordinal) throws Exception {
        zkMeta.registerIndexingServiceInstance(
                IndexingServiceInstanceDescriptor.primary("svc-" + ordinal, "addr:" + ordinal, ordinal));
    }

    private void registerShadow(String serviceId, int shadowOf) throws Exception {
        zkMeta.registerIndexingServiceInstance(
                IndexingServiceInstanceDescriptor.shadow(serviceId, "addr:" + serviceId, shadowOf));
    }

    private SegmentMetadata activeSegment(String segUuid, int owner, long sizeBytes) {
        return SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(owner)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(sizeBytes)
                .vectorCount(10L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
    }

    private LeastLoadedOwnerSelector selector(int fallbackNumInstances) {
        ZkIndexingServiceInstanceDirectory directory =
                new ZkIndexingServiceInstanceDirectory(zkMeta, () -> fallbackNumInstances);
        RegistryBackedInstanceLoadProbe probe =
                new RegistryBackedInstanceLoadProbe(registry, directory);
        return new LeastLoadedOwnerSelector(probe);
    }

    @Test
    public void singleLiveInstanceAlwaysPickedRegardlessOfBytes() throws Exception {
        registerPrimary(0);
        registry.createSegment(activeSegment("seg-a", 0, 5_000_000L));
        assertEquals(0, selector(0).selectOwner(TS_UUID, IDX_UUID));
    }

    @Test
    public void picksLightestOfTwoLiveInstances() throws Exception {
        registerPrimary(0);
        registerPrimary(1);
        registry.createSegment(activeSegment("seg-a", 0, 1000L));
        registry.createSegment(activeSegment("seg-b", 0, 1000L));
        registry.createSegment(activeSegment("seg-c", 1, 100L));
        assertEquals(1, selector(0).selectOwner(TS_UUID, IDX_UUID));
    }

    @Test
    public void tieBreaksToLowestOrdinal() throws Exception {
        registerPrimary(0);
        registerPrimary(1);
        // both have 0 bytes -> ordinal 0 wins
        assertEquals(0, selector(0).selectOwner(TS_UUID, IDX_UUID));
    }

    @Test
    public void deprecatedSegmentsAreIgnored() throws Exception {
        registerPrimary(0);
        registerPrimary(1);
        registry.createSegment(activeSegment("seg-a", 0, 100L));
        SegmentMetadata depr = SegmentMetadata.builder()
                .segmentUuid("seg-d")
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.DEPRECATED)
                .ownerInstanceId(1)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(9_999_999_999L)
                .vectorCount(10L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .retentionUntilEpochMillis(Long.MAX_VALUE)
                .build();
        registry.createSegment(depr);
        // instance 1 has only DEPRECATED bytes (ignored), so it is lighter.
        assertEquals(1, selector(0).selectOwner(TS_UUID, IDX_UUID));
    }

    @Test
    public void segmentsOwnedByDeadOrdinalAreNotCountedAgainstLive() throws Exception {
        registerPrimary(0);
        registerPrimary(2); // gap: ordinal 1 is dead
        registry.createSegment(activeSegment("seg-a", 1, 9_999_999L)); // owned by dead ordinal
        registry.createSegment(activeSegment("seg-b", 0, 100L));
        // ordinal 2 has 0 bytes; ordinal 0 has 100; ordinal 1 is dead.
        assertEquals(2, selector(0).selectOwner(TS_UUID, IDX_UUID));
    }

    @Test
    public void shadowsAreIgnoredByDirectory() throws Exception {
        registerPrimary(0);
        registerShadow("shadow-1", 0);
        ZkIndexingServiceInstanceDirectory directory =
                new ZkIndexingServiceInstanceDirectory(zkMeta, () -> 0);
        assertArrayEquals(new int[]{0}, directory.liveInstanceOrdinals());
    }

    @Test
    public void fallbackNumInstancesUsedWhenZkListEmpty() throws Exception {
        // No primaries registered; fallback says 3 instances → [0, 1, 2].
        ZkIndexingServiceInstanceDirectory directory =
                new ZkIndexingServiceInstanceDirectory(zkMeta, () -> 3);
        assertArrayEquals(new int[]{0, 1, 2}, directory.liveInstanceOrdinals());
    }

    @Test
    public void emptyDirectoryCausesSelectorToThrow() {
        // No primaries + fallbackNumInstances=0 → selector cannot decide.
        LeastLoadedOwnerSelector sel = selector(0);
        try {
            sel.selectOwner(TS_UUID, IDX_UUID);
            fail("expected IllegalStateException");
        } catch (IllegalStateException ok) {
            assertTrue(ok.getMessage().contains("no live instances"));
        }
    }

    @Test
    public void zkPrimariesTakePrecedenceOverFallback() throws Exception {
        registerPrimary(0);
        registerPrimary(1);
        ZkIndexingServiceInstanceDirectory directory =
                new ZkIndexingServiceInstanceDirectory(zkMeta, () -> 5);
        // Fallback is ignored when ZK reports live primaries.
        assertArrayEquals(new int[]{0, 1}, directory.liveInstanceOrdinals());
    }
}
