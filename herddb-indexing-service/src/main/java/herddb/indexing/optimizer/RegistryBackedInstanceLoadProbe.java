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
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentRegistryException;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Production {@link InstanceLoadProbe} that computes bytes-per-instance from
 * the segment registry. Slots for ordinals NOT in
 * {@link IndexingServiceInstanceDirectory#liveInstanceOrdinals()} are sentinel
 * {@link Long#MAX_VALUE} so the {@link LeastLoadedOwnerSelector} never picks
 * them.
 *
 * <p>Segments owned by a dead ordinal are not counted against any live slot;
 * they show up only if the dead ordinal comes back online (its bytes count is
 * preserved across restarts because the segment metadata is in ZK, not in the
 * pod). This matches the design invariant: ownership is recorded explicitly
 * in {@code SegmentMetadata.ownerInstanceId}, never reconstructed.
 */
public final class RegistryBackedInstanceLoadProbe implements InstanceLoadProbe {

    private static final Logger LOGGER =
            Logger.getLogger(RegistryBackedInstanceLoadProbe.class.getName());

    private final SegmentRegistryClient registry;
    private final IndexingServiceInstanceDirectory directory;

    public RegistryBackedInstanceLoadProbe(SegmentRegistryClient registry,
                                           IndexingServiceInstanceDirectory directory) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.directory = Objects.requireNonNull(directory, "directory");
    }

    @Override
    public long[] currentBytesPerInstance(String tablespaceUuid, String indexUuid) {
        int[] live = directory.liveInstanceOrdinals();
        if (live.length == 0) {
            return new long[0];
        }
        int maxLive = live[live.length - 1];
        long[] bytes = new long[maxLive + 1];
        Arrays.fill(bytes, Long.MAX_VALUE);
        for (int ordinal : live) {
            bytes[ordinal] = 0L;
        }
        List<VersionedSegmentMetadata> segments;
        try {
            segments = registry.listSegments(tablespaceUuid, indexUuid);
        } catch (SegmentRegistryException re) {
            LOGGER.log(Level.WARNING,
                    "registry probe failed for tablespace={0} index={1}: {2}",
                    new Object[]{tablespaceUuid, indexUuid, re.getMessage()});
            return new long[0];
        }
        for (VersionedSegmentMetadata vsm : segments) {
            SegmentMetadata sm = vsm.metadata();
            if (sm.getState() != SegmentState.ACTIVE) {
                continue;
            }
            int owner = sm.getOwnerInstanceId();
            if (owner >= 0 && owner < bytes.length && bytes[owner] != Long.MAX_VALUE) {
                bytes[owner] = saturatingAdd(bytes[owner], sm.getSizeBytes());
            }
        }
        return bytes;
    }

    private static long saturatingAdd(long a, long b) {
        long sum = a + b;
        if (((a ^ sum) & (b ^ sum)) < 0) {
            return Long.MAX_VALUE;
        }
        return sum;
    }
}
