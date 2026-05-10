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
package herddb.indexing.segment;

import static org.junit.Assert.assertEquals;
import herddb.log.LogSequenceNumber;
import org.junit.Test;

/**
 * Unit-level coverage for the IS-side ownership observer. Drives
 * {@link SegmentAssignmentMetrics} through the {@link SegmentAssignmentListener}
 * interface and asserts each counter / gauge moves correctly under the
 * realistic event sequences produced by {@code SegmentAssignmentWatcher}.
 */
public class SegmentAssignmentMetricsTest {

    private static VersionedSegmentMetadata sample(String segmentUuid, SegmentState state,
                                                   int owner, int pending) {
        SegmentMetadata m = SegmentMetadata.builder()
                .segmentUuid(segmentUuid)
                .tablespaceUuid("tsuid").tableName("docs")
                .indexUuid("idxuid").indexName("docs_v1")
                .state(state).ownerInstanceId(owner).pendingOwnerInstanceId(pending)
                .baseLsn(new LogSequenceNumber(1L, 1L))
                .sizeBytes(0L).vectorCount(0L).generation(1L).createdAtEpochMillis(0L)
                .build();
        return new VersionedSegmentMetadata(m, /* zkVersion */ 0);
    }

    @Test
    public void initialStateIsZero() {
        SegmentAssignmentMetrics m = new SegmentAssignmentMetrics();
        assertEquals(0L, m.getOwnedSegmentsCount());
        assertEquals(0L, m.getSegmentLoadsTotal());
        assertEquals(0L, m.getSegmentReleasesTotal());
        assertEquals(0L, m.getPendingAssignmentsObservedTotal());
    }

    @Test
    public void singleAssignmentLoadsOnce() {
        SegmentAssignmentMetrics m = new SegmentAssignmentMetrics();
        m.onSegmentAssigned(sample("seg-A", SegmentState.ACTIVE, 0, SegmentMetadata.NO_INSTANCE));
        assertEquals(1L, m.getOwnedSegmentsCount());
        assertEquals(1L, m.getSegmentLoadsTotal());
    }

    @Test
    public void duplicateAssignmentDoesNotInflateLoads() {
        // The watcher's refresh tick can re-emit ASSIGNED for a segment we
        // already own (no state change observed). The observer must NOT
        // count that as a load — otherwise the loads-rate panel would show
        // phantom traffic at refresh cadence.
        SegmentAssignmentMetrics m = new SegmentAssignmentMetrics();
        VersionedSegmentMetadata seg = sample("seg-A", SegmentState.ACTIVE,
                0, SegmentMetadata.NO_INSTANCE);
        m.onSegmentAssigned(seg);
        m.onSegmentAssigned(seg);
        m.onSegmentAssigned(seg);
        assertEquals(1L, m.getOwnedSegmentsCount());
        assertEquals("duplicate ASSIGNED events for the same segment must NOT"
                + " inflate the loads counter", 1L, m.getSegmentLoadsTotal());
    }

    @Test
    public void releasedAfterAssignDecrementsAndIncrementsReleases() {
        SegmentAssignmentMetrics m = new SegmentAssignmentMetrics();
        m.onSegmentAssigned(sample("seg-A", SegmentState.ACTIVE, 0, SegmentMetadata.NO_INSTANCE));
        m.onSegmentReleased(sample("seg-A", SegmentState.ACTIVE, 1, SegmentMetadata.NO_INSTANCE)
                .metadata());
        assertEquals(0L, m.getOwnedSegmentsCount());
        assertEquals(1L, m.getSegmentLoadsTotal());
        assertEquals(1L, m.getSegmentReleasesTotal());
    }

    @Test
    public void releasedWithNullPreviousStillBumpsReleasesCounter() {
        // Defence-in-depth: the watcher contract (since issue #514) no longer
        // delivers null on deletion — the cached metadata is always passed so
        // listeners can identify the segment by UUID (see SegmentAssignmentListener
        // javadoc). The metrics observer must remain defensive against a
        // hypothetical future violation of that contract (e.g. a direct call).
        SegmentAssignmentMetrics m = new SegmentAssignmentMetrics();
        m.onSegmentReleased(null);
        assertEquals(0L, m.getOwnedSegmentsCount());
        assertEquals(1L, m.getSegmentReleasesTotal());
    }

    @Test
    public void releasedForUnknownSegmentDoesNotMoveReleasesCounter() {
        // Adversarial: a stray RELEASED for a segment we never observed
        // ASSIGNED for must not inflate the counter. (This shouldn't
        // happen in practice — the watcher only fires RELEASED for
        // segments it previously surfaced as ours — but the observer
        // must be defensive.)
        SegmentAssignmentMetrics m = new SegmentAssignmentMetrics();
        m.onSegmentReleased(sample("never-loaded", SegmentState.DEPRECATED,
                1, SegmentMetadata.NO_INSTANCE).metadata());
        assertEquals(0L, m.getSegmentReleasesTotal());
    }

    @Test
    public void pendingAssignmentsCountIndependentlyOfLoadsAndReleases() {
        // A flood of PENDING events that never converge into ASSIGNED
        // (e.g. transfers being aborted) must light up the
        // pending-assignments counter without touching loads / releases.
        // High pending / low loads is the diagnostic shape for
        // "transfers are failing".
        SegmentAssignmentMetrics m = new SegmentAssignmentMetrics();
        for (int i = 0; i < 7; i++) {
            m.onPendingAssignment(sample("pending-" + i, SegmentState.TRANSFERRING, 0, 1));
        }
        assertEquals(7L, m.getPendingAssignmentsObservedTotal());
        assertEquals(0L, m.getSegmentLoadsTotal());
        assertEquals(0L, m.getSegmentReleasesTotal());
        assertEquals(0L, m.getOwnedSegmentsCount());
    }

    @Test
    public void multipleSegmentsTrackedCorrectly() {
        SegmentAssignmentMetrics m = new SegmentAssignmentMetrics();
        m.onSegmentAssigned(sample("A", SegmentState.ACTIVE, 0, SegmentMetadata.NO_INSTANCE));
        m.onSegmentAssigned(sample("B", SegmentState.ACTIVE, 0, SegmentMetadata.NO_INSTANCE));
        m.onSegmentAssigned(sample("C", SegmentState.ACTIVE, 0, SegmentMetadata.NO_INSTANCE));
        assertEquals(3L, m.getOwnedSegmentsCount());
        assertEquals(3L, m.getSegmentLoadsTotal());

        // Release B; A and C remain.
        m.onSegmentReleased(sample("B", SegmentState.ACTIVE, 1, SegmentMetadata.NO_INSTANCE)
                .metadata());
        assertEquals(2L, m.getOwnedSegmentsCount());
        assertEquals(1L, m.getSegmentReleasesTotal());

        // Re-acquire B (transfer-back). Loads counter advances again
        // because B re-crosses the not-owned -> owned boundary.
        m.onSegmentAssigned(sample("B", SegmentState.ACTIVE, 0, SegmentMetadata.NO_INSTANCE));
        assertEquals(3L, m.getOwnedSegmentsCount());
        assertEquals(4L, m.getSegmentLoadsTotal());
    }

    @Test
    public void compositionWithExistingListenerWorks() {
        // The metrics observer is a SegmentAssignmentListener so it can be
        // chained onto an existing engine listener via a thin delegate.
        // This test pins that the composition produces correct counters
        // regardless of which side fires first.
        SegmentAssignmentMetrics metrics = new SegmentAssignmentMetrics();
        java.util.concurrent.atomic.AtomicInteger engineLoads =
                new java.util.concurrent.atomic.AtomicInteger();
        SegmentAssignmentListener composite = new SegmentAssignmentListener() {
            @Override
            public void onSegmentAssigned(VersionedSegmentMetadata segment) {
                metrics.onSegmentAssigned(segment);
                engineLoads.incrementAndGet();
            }
            @Override
            public void onSegmentReleased(SegmentMetadata previous) {
                metrics.onSegmentReleased(previous);
            }
            @Override
            public void onPendingAssignment(VersionedSegmentMetadata segment) {
                metrics.onPendingAssignment(segment);
            }
        };

        composite.onSegmentAssigned(sample("X", SegmentState.ACTIVE, 0, SegmentMetadata.NO_INSTANCE));
        assertEquals(1L, metrics.getOwnedSegmentsCount());
        assertEquals(1, engineLoads.get());
    }
}
