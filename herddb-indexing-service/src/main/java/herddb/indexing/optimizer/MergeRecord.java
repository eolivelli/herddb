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

/**
 * Immutable record of a completed (or aborted / declined) merge cycle.
 * Stored in the engine's bounded merge history ({@link IndexOptimizerEngine#getMergeHistory})
 * and returned as a JSON array by {@code GET /merge-history}.
 *
 * <p>All time fields are in milliseconds. Timing fields that are not
 * applicable to a particular merge path (e.g. {@link #pqTrainingMs} on the
 * streaming path) are zero.
 */
public final class MergeRecord {

    /** UUID assigned to this merge cycle at start. */
    public final String mergeId;

    /** Index UUID that was the merge target. */
    public final String indexUuid;

    /** Wall-clock epoch-ms when the merge started. */
    public final long startedAtMs;

    /** Wall-clock epoch-ms when the merge finished (success or failure). */
    public final long finishedAtMs;

    /**
     * Outcome of the merge. One of:
     * <ul>
     *   <li>{@code "success"} — merged, published, inputs deprecated.</li>
     *   <li>{@code "failed"} — merger.merge() threw an exception.</li>
     *   <li>{@code "declined"} — merger returned null (all vectors tombstoned).</li>
     *   <li>{@code "aborted"} — post-merge revalidation found inputs drifted.</li>
     * </ul>
     */
    public final String outcome;

    /** Number of input segments. */
    public final int inputSegments;

    /** Total vector count across all input segments. */
    public final long inputVectors;

    /** Vector count in the merged output; 0 on non-success outcomes. */
    public final long outputVectors;

    /** Time spent downloading input graph/map files from remote storage. */
    public final long downloadMs;

    /**
     * Time spent in PQ K-means training (legacy in-memory path only; 0 for
     * the streaming path and for small shards below the FusedPQ threshold).
     */
    public final long pqTrainingMs;

    /**
     * Time spent building / compacting the graph (legacy: GraphIndexBuilder;
     * streaming: OnDiskGraphIndexCompactor). Includes the legacy in-memory
     * authority-map build but NOT PQ training or upload.
     */
    public final long compactionMs;

    /** Time spent uploading the graph + map files to remote storage. */
    public final long uploadMs;

    /**
     * Time spent calling {@code registry.createSegment()} (ZooKeeper write
     * for the merged output). 0 on non-success outcomes.
     */
    public final long zkRegisterMs;

    /**
     * Time spent deprecating input segments via {@code casUpdateSegment()} CAS
     * calls. 0 on non-success outcomes.
     */
    public final long deprecateMs;

    /** Heap used (bytes) sampled at the start of the merge. */
    public final long peakHeapUsedBytes;

    /** Netty direct-memory used (bytes) sampled at the start of the merge; -1 if unavailable. */
    public final long peakDirectUsedBytes;

    public MergeRecord(String mergeId, String indexUuid, long startedAtMs, long finishedAtMs,
                       String outcome, int inputSegments, long inputVectors, long outputVectors,
                       long downloadMs, long pqTrainingMs, long compactionMs, long uploadMs,
                       long zkRegisterMs, long deprecateMs,
                       long peakHeapUsedBytes, long peakDirectUsedBytes) {
        this.mergeId = mergeId;
        this.indexUuid = indexUuid;
        this.startedAtMs = startedAtMs;
        this.finishedAtMs = finishedAtMs;
        this.outcome = outcome;
        this.inputSegments = inputSegments;
        this.inputVectors = inputVectors;
        this.outputVectors = outputVectors;
        this.downloadMs = downloadMs;
        this.pqTrainingMs = pqTrainingMs;
        this.compactionMs = compactionMs;
        this.uploadMs = uploadMs;
        this.zkRegisterMs = zkRegisterMs;
        this.deprecateMs = deprecateMs;
        this.peakHeapUsedBytes = peakHeapUsedBytes;
        this.peakDirectUsedBytes = peakDirectUsedBytes;
    }

    /** Total wall-clock duration of the merge. */
    public long totalMs() {
        return finishedAtMs - startedAtMs;
    }
}
