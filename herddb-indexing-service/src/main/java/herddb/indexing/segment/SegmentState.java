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
 * Lifecycle state of a segmented-v2 vector index segment as recorded in the
 * ZooKeeper segment registry.
 */
public enum SegmentState {

    /**
     * Optimizer has staged a segment but not yet committed it. Used as a crash-recovery
     * marker: provisional znodes are paired with an ephemeral child whose disappearance
     * signals that the staging process died and the multipart files should be GC'd.
     */
    PROVISIONAL,

    /**
     * Segment is committed and currently owned by exactly one IS instance.
     */
    ACTIVE,

    /**
     * Ownership transfer is in progress. {@code pendingOwnerInstanceId} is the new owner
     * which is opening the segment locally; {@code ownerInstanceId} is the previous owner
     * still serving reads. The transition completes with a CAS that swaps
     * {@code ownerInstanceId} to the pending owner and clears the pending field.
     */
    TRANSFERRING,

    /**
     * Segment has been replaced by one or more compacted outputs ({@code replacedBy}).
     * It still serves reads until {@code retentionUntilEpochMillis} elapses, after which
     * it transitions to {@link #DELETED}.
     */
    DEPRECATED,

    /**
     * Multipart files have been deleted from remote storage. The znode itself is
     * removed shortly after.
     */
    DELETED
}
