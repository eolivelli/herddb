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

/**
 * Receives events from a {@link SegmentAssignmentWatcher} for the segments
 * assigned (or being assigned) to the local IS instance.
 *
 * <p>All callbacks are invoked from the watcher's dispatch thread; implementations
 * MUST NOT block on remote I/O for long, otherwise watcher dispatch backs up. A
 * typical implementation hands the event off to its own executor (e.g., a download
 * executor for {@link #onPendingAssignment}).
 */
public interface SegmentAssignmentListener {

    /**
     * Fired when a segment is observed for the first time with its
     * {@code pendingOwnerInstanceId} equal to the local instance — i.e. the
     * segment is being transferred to us. The listener should download the
     * segment artefacts and tombstone overlay, replay the tail of the commit log
     * up to the current cluster LSN, and then complete the transfer by
     * CAS-updating the registry znode to claim ownership (state ACTIVE,
     * owner = self, pendingOwner = NO_INSTANCE).
     */
    default void onPendingAssignment(VersionedSegmentMetadata segment) {
    }

    /**
     * Fired when a segment is observed with {@code ownerInstanceId} equal to the
     * local instance and the local state did not previously know about it — i.e.
     * the segment is now actively owned by us. This covers both segments we
     * created ourselves (during checkpoint Phase C) and segments whose ownership
     * we just claimed at the end of a transfer.
     */
    default void onSegmentAssigned(VersionedSegmentMetadata segment) {
    }

    /**
     * Fired when a segment that was previously owned by the local instance is no
     * longer owned by us (either ownership moved to another instance or the
     * segment was removed entirely). The listener should close its local handle
     * and free associated resources.
     *
     * @param previous the last metadata observed while the local instance still
     *                 held ownership. Always non-null: even when the segment znode
     *                 is deleted outright (rather than first set to DEPRECATED),
     *                 the watcher passes the last cached copy so downstream
     *                 listeners can identify and clean up the segment by UUID.
     *                 (Since issue #514; prior to that, null was passed on deletion.)
     */
    default void onSegmentReleased(SegmentMetadata previous) {
    }
}
