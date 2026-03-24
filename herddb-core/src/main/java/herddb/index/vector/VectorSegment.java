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

package herddb.index.vector;

import static herddb.index.vector.VectorIndexManager.ordinalToBytes;
import herddb.index.blink.BLink;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Encapsulates the state of a single on-disk vector index segment.
 * Each segment contains an independent FusedPQ graph with its own BLink-backed
 * ordinal-to-PK and PK-to-ordinal mappings.
 */
class VectorSegment implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(VectorSegment.class.getName());

    final int segmentId;
    OnDiskGraphIndex onDiskGraph;
    Path onDiskGraphFile;
    ReaderSupplier onDiskReaderSupplier;
    BLink<Bytes, Bytes> onDiskNodeToPk;
    BLink<Bytes, Long> onDiskPkToNode;
    long estimatedSizeBytes;
    volatile boolean dirty;

    /** Page IDs for the graph chunks (needed to re-persist sealed segments). */
    List<Long> graphPageIds = java.util.Collections.emptyList();
    /** Page IDs for the map chunks (needed to re-persist sealed segments). */
    List<Long> mapPageIds = java.util.Collections.emptyList();

    VectorSegment(int segmentId) {
        this.segmentId = segmentId;
    }

    /**
     * Searches this segment's on-disk graph and appends results to the given list.
     */
    void search(VectorFloat<?> qv, int topK, VectorSimilarityFunction similarityFunction,
                List<Map.Entry<Bytes, Float>> results) {
        OnDiskGraphIndex odg = this.onDiskGraph;
        if (odg == null) {
            return;
        }
        BLink<Bytes, Bytes> n2p = this.onDiskNodeToPk;
        if (n2p == null) {
            return;
        }
        int activeCount = (int) n2p.size();
        int k = Math.min(topK, activeCount);
        if (k == 0) {
            return;
        }
        Bits acceptBits = ordinal -> n2p.search(ordinalToBytes(ordinal)) != null;
        try {
            GraphSearcher searcher = new GraphSearcher(odg);
            OnDiskGraphIndex.View view = (OnDiskGraphIndex.View) searcher.getView();
            io.github.jbellis.jvector.graph.similarity.ScoreFunction.ApproximateScoreFunction approxSF =
                    view.approximateScoreFunctionFor(qv, similarityFunction);
            io.github.jbellis.jvector.graph.similarity.ScoreFunction.ExactScoreFunction reranker =
                    view.rerankerFor(qv, similarityFunction);
            DefaultSearchScoreProvider ssp = new DefaultSearchScoreProvider(approxSF, reranker);
            SearchResult sr = searcher.search(ssp, k, acceptBits);
            for (SearchResult.NodeScore ns : sr.getNodes()) {
                Bytes pk = n2p.search(ordinalToBytes(ns.node));
                if (pk != null) {
                    results.add(new AbstractMap.SimpleImmutableEntry<>(pk, ns.score));
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "error searching on-disk graph segment " + segmentId, e);
        }
    }

    /**
     * Attempts to delete the given primary key from this segment.
     *
     * @return true if the PK was found and deleted in this segment
     */
    boolean deletePk(Bytes key) {
        BLink<Bytes, Long> p2n = this.onDiskPkToNode;
        if (p2n == null) {
            return false;
        }
        Long onDiskOrdinal = p2n.delete(key);
        if (onDiskOrdinal == null) {
            return false;
        }
        BLink<Bytes, Bytes> n2p = this.onDiskNodeToPk;
        if (n2p != null) {
            n2p.delete(ordinalToBytes(onDiskOrdinal.intValue()));
        }
        dirty = true;
        return true;
    }

    /**
     * Returns the number of live nodes in this segment.
     */
    long size() {
        BLink<Bytes, Bytes> n2p = this.onDiskNodeToPk;
        return n2p != null ? n2p.size() : 0;
    }

    /**
     * Returns true if this segment is large enough to be considered sealed
     * (should not be rewritten during checkpoint unless it has deletes).
     */
    boolean isSealed(long maxSegmentSize) {
        return estimatedSizeBytes >= (long) (maxSegmentSize * 0.8) && !dirty;
    }

    /**
     * Scans all (ordinal, pk) entries in this segment via the BLink.
     * Caller must close the returned stream.
     */
    Stream<Map.Entry<Bytes, Bytes>> scanNodeToPk() {
        BLink<Bytes, Bytes> n2p = this.onDiskNodeToPk;
        if (n2p == null) {
            return Stream.empty();
        }
        return n2p.scan(Bytes.EMPTY_ARRAY, Bytes.POSITIVE_INFINITY);
    }

    @Override
    public void close() {
        OnDiskGraphIndex odg = this.onDiskGraph;
        if (odg != null) {
            try {
                odg.close();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "error closing on-disk graph segment " + segmentId, e);
            }
            this.onDiskGraph = null;
        }
        ReaderSupplier rs = this.onDiskReaderSupplier;
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                // ignore
            }
            this.onDiskReaderSupplier = null;
        }
        Path f = this.onDiskGraphFile;
        if (f != null) {
            try {
                Files.deleteIfExists(f);
            } catch (IOException e) {
                // ignore
            }
            this.onDiskGraphFile = null;
        }
        BLink<Bytes, Bytes> n2p = this.onDiskNodeToPk;
        if (n2p != null) {
            n2p.close();
            this.onDiskNodeToPk = null;
        }
        BLink<Bytes, Long> p2n = this.onDiskPkToNode;
        if (p2n != null) {
            p2n.close();
            this.onDiskPkToNode = null;
        }
    }

    @Override
    public String toString() {
        return "VectorSegment{id=" + segmentId
                + ", size=" + size()
                + ", estimatedBytes=" + estimatedSizeBytes
                + ", dirty=" + dirty + "}";
    }
}
