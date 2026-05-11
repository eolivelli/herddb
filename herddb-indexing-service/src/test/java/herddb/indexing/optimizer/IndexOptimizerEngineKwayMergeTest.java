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
import static org.junit.Assert.assertTrue;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogSequenceNumber;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
 * End-to-end engine tests for the k-way single-pass merge feature (issue #524).
 *
 * <p>Uses a real ZooKeeper registry (via Curator {@code TestingServer}) and a
 * {@link SegmentMerger} delegate backed by {@link InMemorySegmentMerger} that
 * also records the input count per call. Verifies:
 * <ul>
 *   <li>8 ACTIVE sub-target segments are collapsed to a single output in ONE
 *       tick with {@code kwayMax=8}, and the merger is called exactly once
 *       with all 8 inputs. Without k-way, the same scenario requires 7
 *       rounds of 2-way merges (cumulative O(N²) work).</li>
 *   <li>The output segment is ACTIVE; all 8 inputs are DEPRECATED.</li>
 *   <li>With {@code kwayMax=4} and 8 candidates, exactly 4 are merged per
 *       tick (the smallest 4), leaving 4 ACTIVE.</li>
 *   <li>With {@code kwayMax=0} (legacy mode) and a tight byte cap, the engine
 *       falls back to picking only 2 inputs per tick.</li>
 * </ul>
 */
public class IndexOptimizerEngineKwayMergeTest {

    private static final String BASE_PATH = "/herd-test-kway";
    private static final String TS_UUID = "ts-kway";
    private static final String IDX_UUID = "idx-kway";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;
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

    private SegmentMetadata seg(String uuid, long sizeBytes) {
        return SegmentMetadata.builder()
                .segmentUuid(uuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .graphPath("g/" + uuid)
                .mapPath("m/" + uuid)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(sizeBytes)
                .vectorCount(sizeBytes / 100L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
    }

    /**
     * Thin wrapper that delegates to {@link InMemorySegmentMerger} and records
     * the input count of each {@link #merge} call. We use delegation instead of
     * subclassing because {@link InMemorySegmentMerger} is {@code final}.
     */
    private static final class TrackingMerger implements SegmentMerger {
        private final InMemorySegmentMerger delegate = new InMemorySegmentMerger();
        private final AtomicInteger invocations = new AtomicInteger(0);
        private final AtomicInteger lastInputCount = new AtomicInteger(0);

        @Override
        public SegmentMetadata merge(List<SegmentMetadata> inputs, int newOwnerInstance) {
            invocations.incrementAndGet();
            lastInputCount.set(inputs.size());
            return delegate.merge(inputs, newOwnerInstance);
        }

        int getInvocations() {
            return invocations.get();
        }

        int getLastInputCount() {
            return lastInputCount.get();
        }
    }

    /**
     * The core k-way test: 8 sub-target segments are merged into 1 in a single
     * optimizer tick. The merger is called exactly once with all 8 inputs, not
     * 7 times with 2 inputs each.
     *
     * <p>This reproduces the gist1m / 8-initial-segments scenario from the issue.
     * With the legacy byte-cap policy (perCycleMaxBytes=1 GiB, segments ~400 MiB
     * each), only 2-3 segments were picked per cycle, forcing 7 sequential rounds.
     * K-way picks all 8 in one shot.
     */
    @Test
    public void eightSegmentsMergedInOneTickWithKway8() throws Exception {
        long segBytes = 400L * 1024L * 1024L; // 400 MiB each — realistic for gist1m shards
        for (int i = 0; i < 8; i++) {
            registry.createSegment(seg("seg-" + i, segBytes + i /* distinct sizes */));
        }

        MergePolicy policy = new MergePolicy.AggressivePolicy(
                /* targetMaxBytes   */ 8L * 1024L * 1024L * 1024L,  // 8 GiB — nothing graduated
                /* perCycleMaxBytes */ 1L * 1024L * 1024L * 1024L,  // 1 GiB — would cap to 2 in legacy
                /* maxCount         */ 200,
                /* kwayMax          */ 8);

        TrackingMerger trackingMerger = new TrackingMerger();
        IndexOptimizerEngine engine = new IndexOptimizerEngine(
                registry, trackingMerger, TS_UUID, policy,
                /* retentionMs */ 60_000L, () -> 0, fakeClock::get);

        engine.runOnce();

        assertEquals("merger must be invoked exactly once (k-way single-pass)",
                1, trackingMerger.getInvocations());
        assertEquals("all 8 segments must be merged in one call",
                8, trackingMerger.getLastInputCount());

        // Registry state: 1 ACTIVE output + 8 DEPRECATED inputs.
        List<VersionedSegmentMetadata> all = registry.listSegments(TS_UUID, IDX_UUID);
        assertEquals("registry must contain 9 znodes after merge (8 deprecated + 1 active)",
                9, all.size());
        long active = all.stream()
                .filter(v -> v.metadata().getState() == SegmentState.ACTIVE).count();
        long deprecated = all.stream()
                .filter(v -> v.metadata().getState() == SegmentState.DEPRECATED).count();
        assertEquals("exactly 1 ACTIVE output", 1, active);
        assertEquals("all 8 inputs deprecated", 8, deprecated);

        assertEquals(1L, engine.getSegmentsMerged());
        assertEquals(8L, engine.getSegmentsDeprecated());
    }

    /**
     * With {@code kwayMax=4} and 8 available candidates, the policy picks the 4
     * smallest. After one tick, 4 are merged into 1 and 4 remain ACTIVE.
     */
    @Test
    public void kwayMax4MergesSmallest4Of8() throws Exception {
        for (int i = 1; i <= 8; i++) {
            registry.createSegment(seg("seg-" + i, 100L * i)); // 100, 200, …, 800 bytes
        }

        MergePolicy policy = new MergePolicy.AggressivePolicy(
                /* targetMaxBytes   */ 10_000L,
                /* perCycleMaxBytes */ 1L,       // irrelevant in k-way mode
                /* maxCount         */ 200,
                /* kwayMax          */ 4);

        TrackingMerger trackingMerger = new TrackingMerger();
        IndexOptimizerEngine engine = new IndexOptimizerEngine(
                registry, trackingMerger, TS_UUID, policy,
                60_000L, () -> 0, fakeClock::get);

        engine.runOnce();

        assertEquals("merger called once", 1, trackingMerger.getInvocations());
        assertEquals("exactly 4 inputs merged (kwayMax=4)", 4, trackingMerger.getLastInputCount());

        List<VersionedSegmentMetadata> all = registry.listSegments(TS_UUID, IDX_UUID);
        long active = all.stream()
                .filter(v -> v.metadata().getState() == SegmentState.ACTIVE).count();
        long deprecated = all.stream()
                .filter(v -> v.metadata().getState() == SegmentState.DEPRECATED).count();
        // 1 merged output + 4 remaining un-merged ACTIVE = 5 ACTIVE; 4 deprecated
        assertEquals("1 merged output + 4 un-merged = 5 ACTIVE", 5, active);
        assertEquals("4 smallest inputs deprecated", 4, deprecated);
    }

    /**
     * With {@code kwayMax=0} (legacy mode) and a tight byte cap, the engine
     * falls back to the old behaviour: only 2 segments per tick because the
     * byte budget is exhausted after the mandatory first pair.
     */
    @Test
    public void kwayMax0FallsBackToLegacyByteCap() throws Exception {
        // 4 segments of 100 bytes each; perCycleMaxBytes=250 → only 2 fit
        // (100+100=200 < 250, but 100+100+100=300 > 250).
        for (int i = 0; i < 4; i++) {
            registry.createSegment(seg("seg-" + i, 100L));
        }

        MergePolicy policy = new MergePolicy.AggressivePolicy(
                /* targetMaxBytes   */ 10_000L,
                /* perCycleMaxBytes */ 250L,
                /* maxCount         */ 200,
                /* kwayMax          */ 0);   // legacy mode

        TrackingMerger trackingMerger = new TrackingMerger();
        IndexOptimizerEngine engine = new IndexOptimizerEngine(
                registry, trackingMerger, TS_UUID, policy,
                60_000L, () -> 0, fakeClock::get);

        engine.runOnce();

        assertEquals("merger called once", 1, trackingMerger.getInvocations());
        assertEquals("legacy mode: perCycleMaxBytes caps at 2 inputs",
                2, trackingMerger.getLastInputCount());
    }
}
