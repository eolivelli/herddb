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

import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.metadata.IndexingServiceInstanceDescriptor;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Test-only stand-in for the IS-side ack producers (issue #555).
 *
 * <p>In production every interested IS pod writes an ephemeral swap-ack
 * znode under {@code {basePath}/index-segments-acks/{segmentUuid}/{serviceId}}
 * after a successful {@code adoptExternalSegment}. End-to-end optimizer
 * tests run {@link IndexOptimizerMain} without any real IS pod, so the acks
 * would never appear and the atomic swap would always abort on timeout.
 *
 * <p>This simulator runs a background poller that, on each tick:
 * <ol>
 *   <li>enumerates the registered IS instance descriptors,</li>
 *   <li>lists every {@code PROVISIONAL} segment in the registry,</li>
 *   <li>for each such segment, writes an ack znode for every IS descriptor
 *       whose {@code effectiveInstanceId} matches the segment's
 *       {@code ownerInstanceId}.</li>
 * </ol>
 * This is exactly the behaviour of the production
 * {@code IndexingServiceEngine} {@code SegmentAssignmentListener} — minus
 * the actual segment load — so optimizer integration tests can drive the
 * full PROVISIONAL → ack → atomic-swap path.
 */
public final class SwapAckSimulator implements AutoCloseable {

    private final SegmentRegistryClient segmentRegistry;
    private final ZookeeperMetadataStorageManager zkMeta;
    private final String tablespaceUuid;
    private final ScheduledExecutorService poller;
    private volatile boolean closed;

    public SwapAckSimulator(SegmentRegistryClient segmentRegistry,
                            ZookeeperMetadataStorageManager zkMeta,
                            String tablespaceUuid) {
        this.segmentRegistry = Objects.requireNonNull(segmentRegistry, "segmentRegistry");
        this.zkMeta = Objects.requireNonNull(zkMeta, "zkMeta");
        this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.poller = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "swap-ack-simulator");
            t.setDaemon(true);
            return t;
        });
        this.poller.scheduleWithFixedDelay(this::tickSafe, 0L, 50L, TimeUnit.MILLISECONDS);
    }

    private void tickSafe() {
        if (closed) {
            return;
        }
        try {
            tick();
        } catch (RuntimeException unexpected) {
            // Background poller must never die — swallow and retry next tick.
            // (Test helper: a noisy stack trace is acceptable for diagnosis.)
            unexpected.printStackTrace();
        }
    }

    private void tick() {
        List<IndexingServiceInstanceDescriptor> descriptors;
        try {
            descriptors = zkMeta.listIndexingServiceInstances();
        } catch (Exception listFailed) {
            // ZK blip — retry next tick.
            return;
        }
        List<String> indexes;
        try {
            indexes = segmentRegistry.listIndexes(tablespaceUuid);
        } catch (Exception listFailed) {
            return;
        }
        for (String indexUuid : indexes) {
            List<VersionedSegmentMetadata> segments;
            try {
                segments = segmentRegistry.listSegments(tablespaceUuid, indexUuid);
            } catch (Exception listFailed) {
                continue;
            }
            for (VersionedSegmentMetadata v : segments) {
                SegmentMetadata m = v.metadata();
                if (m.getState() != SegmentState.PROVISIONAL) {
                    continue;
                }
                for (IndexingServiceInstanceDescriptor d : descriptors) {
                    if (d.effectiveInstanceId() == m.getOwnerInstanceId()) {
                        try {
                            segmentRegistry.createSwapAckNode(m.getSegmentUuid(), d.getServiceId());
                        } catch (Exception ackFailed) {
                            // Parent gone (swap already committed/aborted) or ZK blip.
                            // Idempotent — next tick retries if still needed.
                        }
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        poller.shutdownNow();
        try {
            poller.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
