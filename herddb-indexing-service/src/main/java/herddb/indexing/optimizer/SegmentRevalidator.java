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

import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentRegistryException;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Re-reads each candidate input segment from the registry just before a merge
 * output is published, verifying the segment is still ACTIVE at the version we
 * observed at pick time. If any input drifted (ownership transfer started,
 * another merger raced, znode disappeared), the caller MUST abort the merge:
 * publishing the output then would leave an orphan ACTIVE output covering some
 * inputs that get re-merged into a SECOND output, producing duplicate PKs.
 *
 * <p>Extracted from {@link IndexOptimizerEngine} in step 4 so the upcoming
 * step-6 {@code OptimizerTaskConsumer} can reuse the same logic without
 * pulling the entire engine on its classpath.
 *
 * <p>Hot-path note: one extra ZK read per candidate per merge cycle. Merges
 * run every few minutes and process 4–200 candidates each, so the cost is
 * inconsequential.
 */
public final class SegmentRevalidator {

    private static final Logger LOGGER = Logger.getLogger(SegmentRevalidator.class.getName());

    private final SegmentRegistryClient registry;
    private final String tablespaceUuid;

    public SegmentRevalidator(SegmentRegistryClient registry, String tablespaceUuid) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
    }

    /**
     * @return {@code true} when every candidate is still ACTIVE at the version
     *     captured in the {@link VersionedSegmentMetadata}; {@code false} when
     *     at least one drifted or the read failed.
     */
    public boolean revalidateInputsStillActive(String indexUuid,
                                               List<VersionedSegmentMetadata> candidates) {
        for (VersionedSegmentMetadata v : candidates) {
            try {
                Optional<VersionedSegmentMetadata> latest = registry.getSegment(
                        tablespaceUuid, indexUuid, v.metadata().getSegmentUuid());
                if (!latest.isPresent()) {
                    LOGGER.log(Level.INFO,
                            "input {0} disappeared from registry between pick and revalidate",
                            v.metadata().getSegmentUuid());
                    return false;
                }
                VersionedSegmentMetadata l = latest.get();
                if (l.zkVersion() != v.zkVersion() || l.metadata().getState() != SegmentState.ACTIVE) {
                    LOGGER.log(Level.INFO,
                            "input {0} drifted between pick (state=ACTIVE, version={1}) and"
                                    + " revalidate (state={2}, version={3})",
                            new Object[]{v.metadata().getSegmentUuid(), v.zkVersion(),
                                    l.metadata().getState(), l.zkVersion()});
                    return false;
                }
            } catch (SegmentRegistryException e) {
                LOGGER.log(Level.WARNING,
                        "revalidate failed for input " + v.metadata().getSegmentUuid()
                                + ": " + e.getMessage());
                return false;
            }
        }
        return true;
    }
}
