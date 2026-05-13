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
package herddb.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.index.vector.NewSegmentInfo;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentRegistryPublisher;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogSequenceNumber;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
 * Two-actor scenario for the IS-local compaction fallback: the IS-local
 * compactor and a fake optimizer race to merge the same input set. The
 * optimizer wins by deprecating an input between the IS's stage and revalidate
 * steps. The IS must:
 *
 * <ol>
 *   <li>Detect the drift via the registry revalidate.</li>
 *   <li>Roll back its staged PROVISIONAL znode (so we don't leak orphan ZK
 *       state).</li>
 *   <li>Skip the in-memory swap (so we don't corrupt IndexStatus).</li>
 * </ol>
 *
 * <p>The test exercises {@link SegmentRegistryPublisher} directly (unit-style
 * around the interaction with {@code SegmentRegistryClient}) — the full
 * end-to-end test through {@link herddb.index.vector.PersistentVectorStore}
 * lives in the broader integration suite. Keeping this test ZK-only lets us
 * inject the optimizer's win in the precise window between stage and
 * revalidate.
 */
public class LocalCompactionRegistryRaceTest {

    private static final String BASE_PATH = "/herd-test-local-race";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";
    private static final String IDX_NAME = "idx";
    private static final String TBL_NAME = "tbl";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;

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

    private NewSegmentInfo info(String segmentUuid, int segmentId, long generation) {
        return new NewSegmentInfo(segmentId, segmentUuid,
                "graph-" + segmentId, 1024L,
                "map-" + segmentId, 256L,
                4096L, 100L, generation, LogSequenceNumber.START_OF_TIME, null);
    }

    private SegmentMetadata buildActive(NewSegmentInfo i, long now) {
        return SegmentMetadata.builder()
                .segmentUuid(i.getSegmentUuid())
                .tablespaceUuid(TS_UUID)
                .tableName(TBL_NAME)
                .indexUuid(IDX_UUID)
                .indexName(IDX_NAME)
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .pendingOwnerInstanceId(SegmentMetadata.NO_INSTANCE)
                .segmentId(i.getSegmentId())
                .graphPath(i.getGraphFilePath())
                .mapPath(i.getMapFilePath())
                .baseLsn(i.getBaseLsn())
                .sizeBytes(i.getEstimatedSizeBytes())
                .vectorCount(i.getVectorCount())
                .generation(i.getGeneration())
                .createdAtEpochMillis(now)
                .build();
    }

    @Test
    public void revalidateAbortsAndUnstagesWhenOptimizerDeprecatesInputUnderUs() throws Exception {
        long now = System.currentTimeMillis();

        // Two ACTIVE inputs (the optimizer and the IS pick the same set).
        NewSegmentInfo inputA = info("inputA", 1, 5L);
        NewSegmentInfo inputB = info("inputB", 2, 5L);
        registry.createSegment(buildActive(inputA, now));
        registry.createSegment(buildActive(inputB, now));

        // The IS-local merged output (about to be staged).
        NewSegmentInfo merged = info("isMerged", 100, 6L);
        List<NewSegmentInfo> stagedInfo = new ArrayList<>(Arrays.asList(merged));
        List<NewSegmentInfo> inputInfos = Arrays.asList(inputA, inputB);

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TS_UUID, TBL_NAME, IDX_UUID, IDX_NAME, /* instanceId= */ 0);

        // Phase 1: IS stages the merged output (creates PROVISIONAL znode).
        publisher.stageNewSegments(stagedInfo);
        assertTrue("stage must produce a PROVISIONAL znode for the merged output",
                registry.getSegment(TS_UUID, IDX_UUID, "isMerged").isPresent());

        // Phase 2: optimizer wins the race — it CAS-deprecates inputA between
        // IS's stage and revalidate. Simulate by directly CAS-deprecating it.
        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, "inputA")
                .orElseThrow();
        SegmentMetadata next = current.metadata().toBuilder()
                .state(SegmentState.DEPRECATED)
                .replacedBy(Collections.singletonList("optimizerMerged"))
                .retentionUntilEpochMillis(now + 600_000L)
                .build();
        registry.casUpdateSegment(current, next);

        // Phase 3: IS revalidates — must return false (drift detected).
        boolean ok = publisher.revalidateInputsActive(inputInfos);
        assertFalse("revalidate must detect the optimizer's win and return false", ok);

        // Phase 4: IS unstages — must remove the PROVISIONAL znode (no orphan).
        publisher.unstage(stagedInfo);
        assertFalse("unstage must remove the merged PROVISIONAL znode",
                registry.getSegment(TS_UUID, IDX_UUID, "isMerged").isPresent());

        // The optimizer's deprecate took effect (inputA is DEPRECATED), and
        // the registry has exactly the entries we expect: deprecated inputA,
        // active inputB, no merged.
        SegmentState aState = registry.getSegment(TS_UUID, IDX_UUID, "inputA")
                .orElseThrow().metadata().getState();
        SegmentState bState = registry.getSegment(TS_UUID, IDX_UUID, "inputB")
                .orElseThrow().metadata().getState();
        assertEquals(SegmentState.DEPRECATED, aState);
        assertEquals(SegmentState.ACTIVE, bState);
    }

    @Test
    public void revalidateSucceedsWhenAllInputsAreStillActive() throws Exception {
        long now = System.currentTimeMillis();
        NewSegmentInfo inputA = info("a2", 11, 1L);
        NewSegmentInfo inputB = info("b2", 12, 1L);
        registry.createSegment(buildActive(inputA, now));
        registry.createSegment(buildActive(inputB, now));

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TS_UUID, TBL_NAME, IDX_UUID, IDX_NAME, 0);

        assertTrue(publisher.revalidateInputsActive(Arrays.asList(inputA, inputB)));
    }

    @Test
    public void deprecateInputsContinuesPastIndividualVersionMismatch() throws Exception {
        // If the optimizer raced us on ONE specific input between revalidate
        // and deprecate, we must skip that input but still deprecate the rest.
        long now = System.currentTimeMillis();
        NewSegmentInfo inputA = info("aDep", 21, 1L);
        NewSegmentInfo inputB = info("bDep", 22, 1L);
        NewSegmentInfo inputC = info("cDep", 23, 1L);
        registry.createSegment(buildActive(inputA, now));
        registry.createSegment(buildActive(inputB, now));
        registry.createSegment(buildActive(inputC, now));

        // After revalidate but before our deprecateInputs call, the optimizer
        // bumps inputB by deprecating it itself.
        VersionedSegmentMetadata bv = registry.getSegment(TS_UUID, IDX_UUID, "bDep").orElseThrow();
        registry.casUpdateSegment(bv, bv.metadata().toBuilder()
                .state(SegmentState.DEPRECATED)
                .replacedBy(Collections.singletonList("optimizerSneaky"))
                .retentionUntilEpochMillis(now + 600_000L)
                .build());

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TS_UUID, TBL_NAME, IDX_UUID, IDX_NAME, 0);

        publisher.deprecateInputs(Arrays.asList(inputA, inputB, inputC),
                "isMergedX", now + 600_000L);

        // a and c must now be DEPRECATED with replacedBy = isMergedX.
        // b must remain DEPRECATED with replacedBy = optimizerSneaky (we
        // didn't clobber it).
        VersionedSegmentMetadata afterA = registry.getSegment(TS_UUID, IDX_UUID, "aDep").orElseThrow();
        VersionedSegmentMetadata afterB = registry.getSegment(TS_UUID, IDX_UUID, "bDep").orElseThrow();
        VersionedSegmentMetadata afterC = registry.getSegment(TS_UUID, IDX_UUID, "cDep").orElseThrow();
        assertEquals(SegmentState.DEPRECATED, afterA.metadata().getState());
        assertEquals("isMergedX", afterA.metadata().getReplacedBy().get(0));
        assertEquals(SegmentState.DEPRECATED, afterB.metadata().getState());
        assertEquals("optimizerSneaky", afterB.metadata().getReplacedBy().get(0));
        assertEquals(SegmentState.DEPRECATED, afterC.metadata().getState());
        assertEquals("isMergedX", afterC.metadata().getReplacedBy().get(0));
    }

    @Test
    public void unstageLeavesNonProvisionalEntriesAlone() throws Exception {
        // Defense-in-depth: if some other actor already promoted the staged
        // znode to ACTIVE (an unusual but possible state in a multi-IS
        // deployment with concurrent reconciliation), unstage MUST leave it
        // alone — clobbering an ACTIVE znode would lose data.
        long now = System.currentTimeMillis();
        NewSegmentInfo merged = info("active-already", 200, 7L);
        // Create as ACTIVE directly (skipping PROVISIONAL).
        registry.createSegment(buildActive(merged, now));

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TS_UUID, TBL_NAME, IDX_UUID, IDX_NAME, 0);

        publisher.unstage(Collections.singletonList(merged));

        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID,
                "active-already").orElseThrow();
        assertEquals("unstage must NOT touch an ACTIVE znode",
                SegmentState.ACTIVE, current.metadata().getState());
    }

    @Test
    public void revalidateReturnsFalseWhenInputDisappeared() throws Exception {
        // No znode for the input at all (e.g. the optimizer reaped a
        // previously-deprecated entry). revalidate must abort.
        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TS_UUID, TBL_NAME, IDX_UUID, IDX_NAME, 0);
        NewSegmentInfo missing = info("never-existed", 999, 1L);
        assertFalse("a single missing input must short-circuit revalidate to false",
                publisher.revalidateInputsActive(Collections.singletonList(missing)));
    }

    @Test
    public void revalidateScansAllInputsAndCatchesDriftAfterFirstActive() throws Exception {
        // Defense-in-depth: revalidate must walk EVERY input — not just probe
        // the first and shortcut. If a future refactor lazily probed only the
        // first input then returned true on success, drift on the second
        // would slip through, producing two ACTIVE segments covering the same
        // data after the local compactor commits its merged output. This
        // test pins that contract by registering input #1 as ACTIVE and
        // input #2 as DEPRECATED, then asserting revalidate returns false.
        long now = System.currentTimeMillis();
        NewSegmentInfo good = info("scanGood", 31, 1L);
        NewSegmentInfo drifted = info("scanDrifted", 32, 1L);
        registry.createSegment(buildActive(good, now));
        registry.createSegment(buildActive(drifted, now));
        // Move the SECOND input to DEPRECATED so the publisher must scan past
        // the first good one to detect drift.
        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID,
                "scanDrifted").orElseThrow();
        registry.casUpdateSegment(current, current.metadata().toBuilder()
                .state(SegmentState.DEPRECATED)
                .replacedBy(Collections.singletonList("optimizerWon"))
                .retentionUntilEpochMillis(now + 600_000L)
                .build());

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TS_UUID, TBL_NAME, IDX_UUID, IDX_NAME, 0);

        assertFalse("revalidate must walk every input — drift on the second must"
                        + " return false even though the first is still ACTIVE",
                publisher.revalidateInputsActive(Arrays.asList(good, drifted)));
    }
}
