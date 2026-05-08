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

import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentState;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only {@link SegmentMerger} that does no real graph work — it just
 * synthesises an output segment whose metadata is the union of the inputs'
 * sizes / vector counts. Used by the optimizer integration tests to exercise
 * the registry-side lifecycle without depending on the full vector graph
 * machinery.
 */
public final class InMemorySegmentMerger implements SegmentMerger {

    private final AtomicLong invocations = new AtomicLong();
    private final AtomicLong abandonInvocations = new AtomicLong();
    private final java.util.List<String> abandonedUuids =
            java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    /**
     * Test hooks (all dormant by default — set per-test to exercise specific
     * failure / decline / clock-advancing scenarios in {@code merge()}).
     */
    private volatile boolean failureMode;
    private volatile boolean returnNull;
    private volatile Runnable hook;

    /** When set, every {@link #merge} call throws a synthetic failure. */
    public void setFailureMode(boolean failureMode) {
        this.failureMode = failureMode;
    }

    /** When set, every {@link #merge} call returns {@code null} (declines the batch). */
    public void setReturnNull(boolean returnNull) {
        this.returnNull = returnNull;
    }

    /**
     * Hook invoked at the start of every {@link #merge}. Useful for advancing
     * a fake clock to make {@code lastMergeDurationMs} testable, or to inject
     * concurrent registry mutations between candidate-pick and revalidate.
     */
    public void setHook(Runnable hook) {
        this.hook = hook;
    }

    public long getInvocationCount() {
        return invocations.get();
    }

    public long getAbandonInvocationCount() {
        return abandonInvocations.get();
    }

    public java.util.List<String> getAbandonedUuids() {
        synchronized (abandonedUuids) {
            return new java.util.ArrayList<>(abandonedUuids);
        }
    }

    @Override
    public void abandon(SegmentMetadata produced) {
        abandonInvocations.incrementAndGet();
        abandonedUuids.add(produced.getSegmentUuid());
    }

    @Override
    public SegmentMetadata merge(List<SegmentMetadata> inputs, int newOwnerInstance) {
        invocations.incrementAndGet();
        Runnable h = this.hook;
        if (h != null) {
            h.run();
        }
        if (failureMode) {
            throw new RuntimeException("test-injected merge failure");
        }
        if (returnNull) {
            return null;
        }
        if (inputs == null || inputs.isEmpty()) {
            return null;
        }
        SegmentMetadata first = inputs.get(0);
        long totalBytes = 0L;
        long totalVectors = 0L;
        long maxGeneration = 0L;
        for (SegmentMetadata m : inputs) {
            totalBytes += m.getSizeBytes();
            totalVectors += m.getVectorCount();
            if (m.getGeneration() > maxGeneration) {
                maxGeneration = m.getGeneration();
            }
        }
        String mergedUuid = UUID.randomUUID().toString();
        return SegmentMetadata.builder()
                .segmentUuid(mergedUuid)
                .tablespaceUuid(first.getTablespaceUuid())
                .tableName(first.getTableName())
                .indexUuid(first.getIndexUuid())
                .indexName(first.getIndexName())
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(newOwnerInstance)
                .pendingOwnerInstanceId(SegmentMetadata.NO_INSTANCE)
                // Synthetic merger doesn't allocate a real local segmentId; the reaper
                // skips file deletion when this is NO_SEGMENT_ID, which is what the
                // unit tests expect (no DSM wired).
                .segmentId(SegmentMetadata.NO_SEGMENT_ID)
                .graphPath("merged/" + mergedUuid + "/graph")
                .mapPath("merged/" + mergedUuid + "/map")
                .baseLsn(first.baseLsn())
                .sizeBytes(totalBytes)
                .vectorCount(totalVectors)
                .generation(maxGeneration + 1)
                .createdAtEpochMillis(System.currentTimeMillis())
                .build();
    }
}
