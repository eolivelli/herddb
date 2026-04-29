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
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogSequenceNumber;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end tests for the optimizer engine driving the segment lifecycle
 * (ACTIVE → DEPRECATED → DELETED) on a real ZK registry, using an in-memory
 * {@link InMemorySegmentMerger} so the test doesn't need real graph code.
 */
public class IndexOptimizerEngineTest {

    private static final String BASE_PATH = "/herd-test-step5";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;
    private InMemorySegmentMerger merger;
    private AtomicLong fakeClock;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        CountDownLatch connected = new CountDownLatch(1);
        zk = new ZooKeeper(zkServer.getConnectString(), 30000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue("ZK connect timed out", connected.await(30, TimeUnit.SECONDS));
        zk.create(BASE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        registry = new SegmentRegistryClient(() -> zk, BASE_PATH);
        registry.ensureRoot();

        merger = new InMemorySegmentMerger();
        fakeClock = new AtomicLong(0L);
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

    private SegmentMetadata sampleSegment(String segUuid, int owner, long sizeBytes, long vectors) {
        return SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(owner)
                .graphPath("g/" + segUuid)
                .mapPath("m/" + segUuid)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(sizeBytes)
                .vectorCount(vectors)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
    }

    private IndexOptimizerEngine newEngine(MergePolicy policy, long retentionMs) {
        return new IndexOptimizerEngine(registry, merger, TS_UUID, policy, retentionMs,
                () -> 0, fakeClock::get);
    }

    @Test
    public void doesNothingBelowMergeThreshold() throws Exception {
        registry.createSegment(sampleSegment("seg-a", 0, 100L, 10));
        registry.createSegment(sampleSegment("seg-b", 0, 200L, 20));

        IndexOptimizerEngine engine = newEngine(
                new MergePolicy.SmallestFirstPolicy(/* minCount= */ 4, /* maxCount= */ 1000,
                        /* minBytes= */ 10_000_000L, /* maxBytes= */ 1_000_000_000L),
                /* retentionMs */ 60_000L);
        engine.runOnce();

        assertEquals(0, merger.getInvocationCount());
        assertEquals(2, registry.listSegments(TS_UUID, IDX_UUID).size());
    }

    @Test
    public void mergesAndDeprecatesInputsWhenCountCeilingHit() throws Exception {
        // Force-fire via maxCount = 2.
        for (int i = 0; i < 3; i++) {
            registry.createSegment(sampleSegment("seg-" + i, 0, 100L, 10L));
        }
        IndexOptimizerEngine engine = newEngine(
                new MergePolicy.SmallestFirstPolicy(4, /* maxCount= */ 2,
                        Long.MAX_VALUE, Long.MAX_VALUE),
                /* retentionMs */ 60_000L);

        engine.runOnce();

        assertEquals(1, merger.getInvocationCount());
        List<VersionedSegmentMetadata> all = registry.listSegments(TS_UUID, IDX_UUID);
        // 3 deprecated inputs + 1 new ACTIVE output = 4 total.
        assertEquals(4, all.size());
        long active = all.stream().filter(v -> v.metadata().getState() == SegmentState.ACTIVE).count();
        long deprecated = all.stream()
                .filter(v -> v.metadata().getState() == SegmentState.DEPRECATED).count();
        assertEquals(1, active);
        assertEquals(3, deprecated);

        // Output should reference all three input UUIDs as replacedBy.
        VersionedSegmentMetadata outputV = all.stream()
                .filter(v -> v.metadata().getState() == SegmentState.ACTIVE).findFirst().orElseThrow();
        for (VersionedSegmentMetadata depr : all) {
            if (depr.metadata().getState() == SegmentState.DEPRECATED) {
                assertEquals(1, depr.metadata().getReplacedBy().size());
                assertEquals(outputV.metadata().getSegmentUuid(),
                        depr.metadata().getReplacedBy().get(0));
                assertTrue(depr.metadata().getRetentionUntilEpochMillis() > 0L);
            }
        }
        assertEquals(1, engine.getSegmentsMerged());
        assertEquals(3, engine.getSegmentsDeprecated());
    }

    @Test
    public void reapsDeprecatedSegmentsAfterRetentionElapses() throws Exception {
        // Build a state with one DEPRECATED segment whose retentionUntil is in the past.
        SegmentMetadata seg = sampleSegment("seg-old", 0, 100L, 10L);
        registry.createSegment(seg);
        VersionedSegmentMetadata v0 = registry.getSegment(TS_UUID, IDX_UUID, "seg-old").orElseThrow();
        SegmentMetadata deprecated = v0.metadata().toBuilder()
                .state(SegmentState.DEPRECATED)
                .retentionUntilEpochMillis(/* in the past */ 1L)
                .build();
        registry.casUpdateSegment(v0, deprecated);

        // Advance the fake clock past retention.
        fakeClock.set(1000L);

        IndexOptimizerEngine engine = newEngine(
                new MergePolicy.SmallestFirstPolicy(99, 99, Long.MAX_VALUE, Long.MAX_VALUE),
                /* retentionMs */ 60_000L);
        engine.runOnce();

        // The reaper should have transitioned DEPRECATED → DELETED → znode removed.
        assertEquals(0, registry.listSegments(TS_UUID, IDX_UUID).size());
        assertEquals(1, engine.getSegmentsDeleted());
    }

    @Test
    public void reapDoesNotFireBeforeRetention() throws Exception {
        SegmentMetadata seg = sampleSegment("seg-young", 0, 100L, 10L);
        registry.createSegment(seg);
        VersionedSegmentMetadata v0 = registry.getSegment(TS_UUID, IDX_UUID, "seg-young").orElseThrow();
        registry.casUpdateSegment(v0, v0.metadata().toBuilder()
                .state(SegmentState.DEPRECATED)
                .retentionUntilEpochMillis(10_000L) // far in the future
                .build());

        fakeClock.set(0L);
        IndexOptimizerEngine engine = newEngine(
                new MergePolicy.SmallestFirstPolicy(99, 99, Long.MAX_VALUE, Long.MAX_VALUE),
                /* retentionMs */ 60_000L);
        engine.runOnce();

        assertEquals(1, registry.listSegments(TS_UUID, IDX_UUID).size());
        assertEquals(0, engine.getSegmentsDeleted());
    }

    @Test
    public void mergerFailureLeavesInputsUntouched() throws Exception {
        for (int i = 0; i < 3; i++) {
            registry.createSegment(sampleSegment("seg-" + i, 0, 100L, 10L));
        }
        SegmentMerger faulty = (inputs, owner) -> {
            throw new RuntimeException("simulated merger failure");
        };
        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, faulty,
                TS_UUID,
                new MergePolicy.SmallestFirstPolicy(4, 2, Long.MAX_VALUE, Long.MAX_VALUE),
                60_000L,
                () -> 0,
                fakeClock::get);

        engine.runOnce();

        // Inputs unchanged; no new ACTIVE; no DEPRECATED.
        List<VersionedSegmentMetadata> all = registry.listSegments(TS_UUID, IDX_UUID);
        assertEquals(3, all.size());
        for (VersionedSegmentMetadata v : all) {
            assertEquals(SegmentState.ACTIVE, v.metadata().getState());
        }
        assertEquals(0, engine.getSegmentsMerged());
    }

    @Test
    public void multipleIndexesProcessedIndependently() throws Exception {
        String idxA = "idx-a-" + UUID.randomUUID();
        String idxB = "idx-b-" + UUID.randomUUID();
        for (int i = 0; i < 3; i++) {
            registry.createSegment(sampleSegment("a-" + i, 0, 100L, 10L)
                    .toBuilder().indexUuid(idxA).build());
            registry.createSegment(sampleSegment("b-" + i, 0, 100L, 10L)
                    .toBuilder().indexUuid(idxB).build());
        }
        IndexOptimizerEngine engine = newEngine(
                new MergePolicy.SmallestFirstPolicy(4, 2, Long.MAX_VALUE, Long.MAX_VALUE),
                60_000L);
        engine.runOnce();
        assertEquals(2, merger.getInvocationCount());

        for (String idx : new String[]{idxA, idxB}) {
            List<VersionedSegmentMetadata> all = registry.listSegments(TS_UUID, idx);
            assertEquals(4, all.size()); // 3 deprecated + 1 active output
            long activeCount = all.stream().filter(v -> v.metadata().getState() == SegmentState.ACTIVE).count();
            assertEquals(1, activeCount);
        }
    }

    @Test
    public void smallestFirstPolicyPicksByteCappedSubset() {
        java.util.List<VersionedSegmentMetadata> input = new java.util.ArrayList<>();
        // Sizes: 50, 50, 50, 1000 — minBytes=100, minCount=2, maxBytes=200 → picks the
        // three smallest (50+50+50=150 which is >= minBytes), excludes the big one.
        long[] sizes = {1000L, 50L, 50L, 50L};
        for (int i = 0; i < sizes.length; i++) {
            SegmentMetadata m = SegmentMetadata.builder()
                    .segmentUuid("s" + i).tablespaceUuid("t").tableName("x")
                    .indexUuid("i").indexName("xi").state(SegmentState.ACTIVE)
                    .ownerInstanceId(0).sizeBytes(sizes[i]).build();
            input.add(new VersionedSegmentMetadata(m, 0));
        }
        MergePolicy policy = new MergePolicy.SmallestFirstPolicy(2, 1000, 100L, 200L);
        java.util.List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(input);
        assertEquals(3, picked.size());
        for (VersionedSegmentMetadata v : picked) {
            assertEquals(50L, v.metadata().getSizeBytes());
        }
    }

    @Test
    public void smallestFirstPolicyForceFiresOnCountCeilingEvenBelowMinBytes() {
        java.util.List<VersionedSegmentMetadata> input = new java.util.ArrayList<>();
        for (int i = 0; i < 10; i++) {
            SegmentMetadata m = SegmentMetadata.builder()
                    .segmentUuid("s" + i).tablespaceUuid("t").tableName("x")
                    .indexUuid("i").indexName("xi").state(SegmentState.ACTIVE)
                    .ownerInstanceId(0).sizeBytes(1L).build();
            input.add(new VersionedSegmentMetadata(m, 0));
        }
        MergePolicy policy = new MergePolicy.SmallestFirstPolicy(20, 5, /* minBytes */ 1_000_000L,
                Long.MAX_VALUE);
        java.util.List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(input);
        assertNotNull(picked);
        assertTrue("count ceiling must force-fire the merge", picked.size() >= 2);
    }
}
