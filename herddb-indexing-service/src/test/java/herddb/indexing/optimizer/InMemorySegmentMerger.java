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

    public long getInvocationCount() {
        return invocations.get();
    }

    @Override
    public SegmentMetadata merge(List<SegmentMetadata> inputs, int newOwnerInstance) {
        invocations.incrementAndGet();
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
