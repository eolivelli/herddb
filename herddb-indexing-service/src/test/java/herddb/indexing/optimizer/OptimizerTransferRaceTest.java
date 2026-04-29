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
import herddb.indexing.segment.OwnershipTransfer;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogSequenceNumber;
import java.util.List;
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
 * Validates fix A9: the optimizer must NOT publish a merged output if any of its
 * input segments has drifted from ACTIVE between the merge-pick step and the
 * deprecate-inputs step (e.g. an ownership transfer started concurrently). The
 * pre-publish revalidation must catch the drift and abort the run, leaving all
 * inputs untouched and the registry consistent.
 */
public class OptimizerTransferRaceTest {

    private static final String BASE_PATH = "/herd-test-A9";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;
    private AtomicLong fakeClock;
    private InMemorySegmentMerger merger;

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

    private SegmentMetadata sampleSegment(String segUuid, long sizeBytes) {
        return SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(sizeBytes)
                .vectorCount(10L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
    }

    /**
     * Drives the engine via a {@link MergePolicy} that, between pick and the engine's
     * subsequent revalidation, mutates one of the candidates' znodes. This simulates
     * a concurrent ownership transfer (state ACTIVE → TRANSFERRING).
     */
    @Test
    public void mergeAbortedWhenInputDriftsToTransferring() throws Exception {
        registry.createSegment(sampleSegment("seg-a", 100L));
        registry.createSegment(sampleSegment("seg-b", 100L));
        registry.createSegment(sampleSegment("seg-c", 100L));

        // Custom policy: returns the candidates as picked, then mutates seg-a's znode
        // (transition to TRANSFERRING) BEFORE the engine's revalidation phase.
        MergePolicy raceTrigger = new MergePolicy() {
            @Override
            public List<VersionedSegmentMetadata> pickMergeCandidates(
                    List<VersionedSegmentMetadata> activeSegments) {
                List<VersionedSegmentMetadata> picked =
                        new MergePolicy.SmallestFirstPolicy(2, 2, Long.MAX_VALUE, Long.MAX_VALUE)
                                .pickMergeCandidates(activeSegments);
                // Race: flip seg-a to TRANSFERRING BEFORE the engine revalidates.
                try {
                    VersionedSegmentMetadata segA = registry.getSegment(TS_UUID, IDX_UUID, "seg-a")
                            .orElseThrow();
                    OwnershipTransfer.initiate(registry, segA, /* newOwner */ 1);
                } catch (Exception e) {
                    throw new RuntimeException("race-trigger failed: " + e, e);
                }
                return picked;
            }
        };

        IndexOptimizerEngine engine = new IndexOptimizerEngine(
                registry, merger, TS_UUID, raceTrigger, /* retentionMs */ 60_000L,
                () -> 0, fakeClock::get);
        engine.runOnce();

        // The engine must NOT have published a new output and must NOT have deprecated
        // any input — the revalidation aborted the run after the merger finished.
        // The merger DID run (its output was discarded) — that's an accepted cost; the
        // alternative (revalidate before merge) would race against ALSO-after-merge
        // drifts, so we picked the safer "merge then revalidate then publish" order.
        assertEquals("merger ran exactly once before the revalidation abort",
                1, merger.getInvocationCount());
        assertEquals("no segment merged into the registry", 0, engine.getSegmentsMerged());
        assertEquals("no input deprecated", 0, engine.getSegmentsDeprecated());

        // Final registry state: seg-a is TRANSFERRING (per our race), seg-b/seg-c are
        // still ACTIVE, no output segment exists.
        List<VersionedSegmentMetadata> all = registry.listSegments(TS_UUID, IDX_UUID);
        assertEquals(3, all.size());
        for (VersionedSegmentMetadata v : all) {
            switch (v.metadata().getSegmentUuid()) {
                case "seg-a":
                    assertEquals(SegmentState.TRANSFERRING, v.metadata().getState());
                    break;
                case "seg-b":
                case "seg-c":
                    assertEquals(SegmentState.ACTIVE, v.metadata().getState());
                    break;
                default:
                    org.junit.Assert.fail("unexpected segment in registry: "
                            + v.metadata().getSegmentUuid());
            }
        }
    }

    @Test
    public void mergeProceedsWhenAllInputsRemainActive() throws Exception {
        // Sanity check: revalidation does NOT abort when nothing drifted.
        for (int i = 0; i < 3; i++) {
            registry.createSegment(sampleSegment("seg-" + i, 100L));
        }
        IndexOptimizerEngine engine = new IndexOptimizerEngine(
                registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(2, 2, Long.MAX_VALUE, Long.MAX_VALUE),
                60_000L, () -> 0, fakeClock::get);
        engine.runOnce();
        assertEquals(1, engine.getSegmentsMerged());
        assertEquals(3, engine.getSegmentsDeprecated());
    }

    @Test
    public void mergeAbortedWhenInputDeletedConcurrently() throws Exception {
        registry.createSegment(sampleSegment("seg-x", 100L));
        registry.createSegment(sampleSegment("seg-y", 100L));

        MergePolicy deleteTrigger = new MergePolicy() {
            @Override
            public List<VersionedSegmentMetadata> pickMergeCandidates(
                    List<VersionedSegmentMetadata> activeSegments) {
                List<VersionedSegmentMetadata> picked =
                        new MergePolicy.SmallestFirstPolicy(2, 2, Long.MAX_VALUE, Long.MAX_VALUE)
                                .pickMergeCandidates(activeSegments);
                try {
                    VersionedSegmentMetadata segX = registry.getSegment(TS_UUID, IDX_UUID, "seg-x")
                            .orElseThrow();
                    registry.casDeleteSegment(segX);
                } catch (Exception e) {
                    throw new RuntimeException("race-trigger failed", e);
                }
                return picked;
            }
        };
        IndexOptimizerEngine engine = new IndexOptimizerEngine(
                registry, merger, TS_UUID, deleteTrigger, 60_000L, () -> 0, fakeClock::get);
        engine.runOnce();
        assertEquals(0, engine.getSegmentsMerged());
        assertEquals(0, engine.getSegmentsDeprecated());
    }
}
