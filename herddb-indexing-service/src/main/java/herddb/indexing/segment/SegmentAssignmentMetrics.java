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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Side-effect-free observer of segment ownership changes for the local IS
 * instance. Implements {@link SegmentAssignmentListener} so it can be chained
 * onto an existing listener via a thin delegate (production wiring composes
 * the metrics observer with the engine's own load/release handler).
 *
 * <p>Tracks four numbers:
 *
 * <ul>
 *   <li>{@code ownedSegmentsCount} — current count of segments where this
 *       instance is the canonical owner. Maintained as a set under the
 *       observer's monitor so the gauge value is always exactly the size of
 *       a single coherent observation.</li>
 *   <li>{@code segmentLoadsTotal} — number of times a segment crossed
 *       {@code not-owned -> owned} via either initial assignment or
 *       takeover-completion. Equivalent to "the local IS just took over a
 *       new segment and had to load it".</li>
 *   <li>{@code segmentReleasesTotal} — number of times a segment crossed
 *       {@code owned -> not-owned}. Either it was transferred away or
 *       deleted; in both cases the local IS unloads it.</li>
 *   <li>{@code pendingAssignmentsObservedTotal} — number of times the local
 *       instance observed a {@code TRANSFERRING} znode with our id as
 *       pending owner. Useful for diagnosing "why is the IS spending CPU
 *       downloading segment artefacts?": every pending observation
 *       eventually leads to either a load (if we complete the transfer) or
 *       no event (if the optimizer aborts/redirects). High pending /
 *       low loads ⇒ transfers are failing or being redirected.</li>
 * </ul>
 *
 * <p>Thread-safety: all four counters are atomic. The {@code ownedSegmentUuids}
 * set is a {@link ConcurrentHashMap}-backed concurrent set so reads and
 * writes are safely interleaved across the watcher's dispatch thread and any
 * Prometheus scrape thread reading {@link #getOwnedSegmentsCount()}.
 */
public final class SegmentAssignmentMetrics implements SegmentAssignmentListener {

    private final AtomicLong segmentLoadsTotal = new AtomicLong();
    private final AtomicLong segmentReleasesTotal = new AtomicLong();
    private final AtomicLong pendingAssignmentsObservedTotal = new AtomicLong();
    private final Set<String> ownedSegmentUuids = ConcurrentHashMap.newKeySet();

    @Override
    public void onPendingAssignment(VersionedSegmentMetadata segment) {
        pendingAssignmentsObservedTotal.incrementAndGet();
    }

    @Override
    public void onSegmentAssigned(VersionedSegmentMetadata segment) {
        // {@link Set#add} returns true iff the element was newly added —
        // we use that as the LOAD signal so a duplicate event (e.g. a
        // refresh tick re-emitting ASSIGNED for a segment we already own)
        // does NOT inflate the loads counter.
        if (ownedSegmentUuids.add(segment.metadata().getSegmentUuid())) {
            segmentLoadsTotal.incrementAndGet();
        }
    }

    @Override
    public void onSegmentReleased(SegmentMetadata previous) {
        // Defence-in-depth: the watcher contract (since issue #514) guarantees
        // `previous` is non-null even on znode-gone events; this branch is
        // unreachable in production. We keep it so the metrics observer never
        // NPEs if a future code path violates the contract — just bump the
        // counter and leave the owned-set self-consistent.
        if (previous == null) {
            segmentReleasesTotal.incrementAndGet();
            return;
        }
        if (ownedSegmentUuids.remove(previous.getSegmentUuid())) {
            segmentReleasesTotal.incrementAndGet();
        }
    }

    /**
     * Returns the current count of segments owned by this instance. A
     * Prometheus scrape that reads this value gets a strictly-monotone-
     * over-set-membership view: between two scrapes the value can move up
     * (a load won the race) or down (a release won) but never inconsistent
     * with the set size at the read instant.
     */
    public long getOwnedSegmentsCount() {
        return ownedSegmentUuids.size();
    }

    public long getSegmentLoadsTotal() {
        return segmentLoadsTotal.get();
    }

    public long getSegmentReleasesTotal() {
        return segmentReleasesTotal.get();
    }

    public long getPendingAssignmentsObservedTotal() {
        return pendingAssignmentsObservedTotal.get();
    }
}
