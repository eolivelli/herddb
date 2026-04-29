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

import herddb.index.vector.NewSegmentInfo;
import herddb.index.vector.SegmentPublisher;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Adapter that wires {@link SegmentPublisher} (called by
 * {@link herddb.index.vector.PersistentVectorStore} after each successful
 * checkpoint) to a {@link SegmentRegistryClient}, registering every freshly-emitted
 * segment as ACTIVE owned by the local IS instance.
 *
 * <p>Each registered segment is given a fresh {@link UUID#randomUUID() random UUID}
 * as its ZK address. Failures are logged and surfaced as a runtime exception so the
 * caller (PersistentVectorStore) can decide to swallow them — the publisher contract
 * documents that the local checkpoint must succeed regardless.
 */
public final class SegmentRegistryPublisher implements SegmentPublisher {

    private static final Logger LOGGER = Logger.getLogger(SegmentRegistryPublisher.class.getName());

    private final SegmentRegistryClient registry;
    private final String tablespaceUuid;
    private final String tableName;
    private final String indexUuid;
    private final String indexName;
    private final int instanceId;
    private final LongSupplier clock;

    public SegmentRegistryPublisher(SegmentRegistryClient registry,
                                    String tablespaceUuid, String tableName,
                                    String indexUuid, String indexName,
                                    int instanceId) {
        this(registry, tablespaceUuid, tableName, indexUuid, indexName, instanceId,
                System::currentTimeMillis);
    }

    /** Test-friendly constructor accepting an injected clock. */
    public SegmentRegistryPublisher(SegmentRegistryClient registry,
                                    String tablespaceUuid, String tableName,
                                    String indexUuid, String indexName,
                                    int instanceId, LongSupplier clock) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.indexUuid = Objects.requireNonNull(indexUuid, "indexUuid");
        this.indexName = Objects.requireNonNull(indexName, "indexName");
        this.instanceId = instanceId;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public void publishNewSegments(List<NewSegmentInfo> segments) {
        if (segments == null || segments.isEmpty()) {
            return;
        }
        long now = clock.getAsLong();
        for (NewSegmentInfo info : segments) {
            String segmentUuid = UUID.randomUUID().toString();
            SegmentMetadata metadata = SegmentMetadata.builder()
                    .segmentUuid(segmentUuid)
                    .tablespaceUuid(tablespaceUuid)
                    .tableName(tableName)
                    .indexUuid(indexUuid)
                    .indexName(indexName)
                    .state(SegmentState.ACTIVE)
                    .ownerInstanceId(instanceId)
                    .pendingOwnerInstanceId(SegmentMetadata.NO_INSTANCE)
                    .graphPath(info.getGraphFilePath())
                    .mapPath(info.getMapFilePath())
                    .baseLsn(info.getBaseLsn())
                    .sizeBytes(info.getEstimatedSizeBytes())
                    .vectorCount(info.getVectorCount())
                    .generation(info.getGeneration())
                    .createdAtEpochMillis(now)
                    .build();
            try {
                registry.createSegment(metadata);
                LOGGER.log(Level.FINE,
                        "registered segment {0} for index {1} (segmentId={2}, owner={3}, baseLsn={4})",
                        new Object[]{segmentUuid, indexName, info.getSegmentId(), instanceId,
                                info.getBaseLsn()});
            } catch (SegmentRegistryException.SegmentAlreadyExists alreadyExists) {
                // Should be impossible because we generate a fresh UUID; surface loudly if it
                // ever happens since it would indicate a UUID collision or buggy reuse.
                throw new IllegalStateException("UUID collision while registering segment "
                        + segmentUuid + " for index " + indexName, alreadyExists);
            } catch (SegmentRegistryException e) {
                throw new RuntimeException("failed to register segment " + segmentUuid
                        + " for index " + indexName, e);
            }
        }
    }
}
