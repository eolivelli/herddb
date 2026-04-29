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
import java.util.List;

/**
 * Merges multiple input segments into a single output segment. The optimizer
 * engine drives the registry-side state machine; this interface encapsulates the
 * heavy lifting (downloading input artefacts, rebuilding the merged graph,
 * uploading the output to remote storage) so it can be plugged in or stubbed.
 *
 * <p>Implementations:
 * <ul>
 *   <li>The production implementation will reuse {@link herddb.index.vector.VectorIndexCompactor}
 *       once that code path is extracted from PersistentVectorStore (out of
 *       scope for the initial step 5 MVP).</li>
 *   <li>{@link InMemorySegmentMerger} (in tests) performs no graph work but
 *       produces a synthetic output segment so the registry-side lifecycle can
 *       be exercised end-to-end.</li>
 * </ul>
 */
public interface SegmentMerger {

    /**
     * @param inputs           segments to merge. The list is non-empty and all entries share
     *                         the same {@code (tablespaceUuid, indexUuid)}.
     * @param newOwnerInstance the IS instance that should own the freshly-created output segment.
     * @return metadata for the freshly-created (PROVISIONAL or ACTIVE) output segment, ready to
     *         hand to the optimizer engine for registry publishing. The caller is responsible
     *         for the actual {@code createSegment} call; the merger only produces the bytes and
     *         the metadata payload.
     */
    SegmentMetadata merge(List<SegmentMetadata> inputs, int newOwnerInstance) throws Exception;
}
