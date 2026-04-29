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
 * Helpers that orchestrate the segment ownership transfer protocol via CAS
 * updates on the segment registry znode. There are two conceptual operations:
 *
 * <ol>
 *   <li>{@link #initiate}: the caller (typically the optimizer) marks a segment
 *       as TRANSFERRING by setting {@code pendingOwnerInstanceId = newOwner}.
 *       The current owner is unchanged so reads keep flowing through it.</li>
 *   <li>{@link #complete}: invoked by the new owner after it has downloaded
 *       and opened the segment locally. Sets {@code ownerInstanceId = newOwner,
 *       pendingOwnerInstanceId = NO_INSTANCE, state = ACTIVE}.</li>
 * </ol>
 *
 * <p>Both methods are CAS operations on top of {@link SegmentRegistryClient};
 * callers are expected to retry on {@link SegmentRegistryException.VersionMismatch}
 * by re-reading and re-attempting the transition (or aborting if the segment
 * has reached an incompatible state).
 */
public final class OwnershipTransfer {

    private OwnershipTransfer() {
    }

    /**
     * Transitions a segment from ACTIVE → TRANSFERRING. The new owner must NOT
     * be the same as the current owner. Returns the post-CAS metadata.
     */
    public static VersionedSegmentMetadata initiate(SegmentRegistryClient registry,
                                                    VersionedSegmentMetadata current,
                                                    int newOwnerInstanceId)
            throws SegmentRegistryException {
        SegmentMetadata m = current.metadata();
        if (m.getState() != SegmentState.ACTIVE) {
            throw new IllegalStateException("cannot initiate transfer from state " + m.getState()
                    + " for segment " + m.getSegmentUuid());
        }
        if (m.getOwnerInstanceId() == newOwnerInstanceId) {
            throw new IllegalStateException("segment " + m.getSegmentUuid()
                    + " is already owned by instance " + newOwnerInstanceId);
        }
        SegmentMetadata next = m.toBuilder()
                .state(SegmentState.TRANSFERRING)
                .pendingOwnerInstanceId(newOwnerInstanceId)
                .build();
        return registry.casUpdateSegment(current, next);
    }

    /**
     * Transitions a segment from TRANSFERRING → ACTIVE under the new owner.
     * Must be called by the new owner after it has finished opening the segment
     * locally; on success the previous owner is expected to release the segment
     * via the watcher event.
     */
    public static VersionedSegmentMetadata complete(SegmentRegistryClient registry,
                                                    VersionedSegmentMetadata current,
                                                    int newOwnerInstanceId)
            throws SegmentRegistryException {
        SegmentMetadata m = current.metadata();
        if (m.getState() != SegmentState.TRANSFERRING) {
            throw new IllegalStateException("cannot complete transfer from state " + m.getState()
                    + " for segment " + m.getSegmentUuid());
        }
        if (m.getPendingOwnerInstanceId() != newOwnerInstanceId) {
            throw new IllegalStateException("segment " + m.getSegmentUuid()
                    + " pending owner is " + m.getPendingOwnerInstanceId()
                    + " but caller is " + newOwnerInstanceId);
        }
        SegmentMetadata next = m.toBuilder()
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(newOwnerInstanceId)
                .pendingOwnerInstanceId(SegmentMetadata.NO_INSTANCE)
                .build();
        return registry.casUpdateSegment(current, next);
    }
}
