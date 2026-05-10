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

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.util.Objects;

/**
 * Immutable bundle of jvector build parameters for a single vector index.
 * Used by {@link IndexMergeConfigProvider} to supply per-index configuration
 * to {@link RemoteSegmentMerger} at merge time (issue #516).
 *
 * <p>Note: {@code dim} (vector dimension) is intentionally absent — it is read
 * at merge time by peeking the first entry of the first input map file
 * ({@link RemoteSegmentMerger#peekDimFromMapFile}). This avoids requiring
 * any external service to know the dimension: it is always embedded in the
 * map file binary format.
 *
 * <p>All fields must be positive / non-null. Callers that receive an
 * {@code IndexMergeConfig} from a provider may assume the invariants hold
 * (providers validate before returning).
 */
public final class IndexMergeConfig {

    /** jvector graph degree (edges per node, must be &gt; 0). */
    public final int graphM;
    /** jvector beam-width during graph build (must be &gt; 0). */
    public final int beamWidth;
    /** jvector neighbor-overflow factor (must be &gt; 0). */
    public final float neighborOverflow;
    /** jvector alpha factor (must be &gt; 0). */
    public final float alpha;
    /** Vector similarity function (must not be null). */
    public final VectorSimilarityFunction similarity;

    public IndexMergeConfig(int graphM, int beamWidth,
                            float neighborOverflow, float alpha,
                            VectorSimilarityFunction similarity) {
        if (graphM <= 0) {
            throw new IllegalArgumentException("graphM must be positive: " + graphM);
        }
        if (beamWidth <= 0) {
            throw new IllegalArgumentException("beamWidth must be positive: " + beamWidth);
        }
        if (neighborOverflow <= 0f) {
            throw new IllegalArgumentException("neighborOverflow must be positive: " + neighborOverflow);
        }
        if (alpha <= 0f) {
            throw new IllegalArgumentException("alpha must be positive: " + alpha);
        }
        this.graphM = graphM;
        this.beamWidth = beamWidth;
        this.neighborOverflow = neighborOverflow;
        this.alpha = alpha;
        this.similarity = Objects.requireNonNull(similarity, "similarity");
    }

    @Override
    public String toString() {
        return "IndexMergeConfig{"
                + "graphM=" + graphM
                + ", beamWidth=" + beamWidth
                + ", neighborOverflow=" + neighborOverflow
                + ", alpha=" + alpha
                + ", similarity=" + similarity
                + '}';
    }
}
