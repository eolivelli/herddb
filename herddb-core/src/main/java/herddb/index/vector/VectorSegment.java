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
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
    BLink<Bytes, Long> onDiskPkToNode;
    long estimatedSizeBytes;
    volatile boolean dirty;

    // Compact ordinal-to-PK cache: all PKs packed in one byte[],
    // with parallel offset/length arrays indexed by ordinal.
    byte[] pkData;
    int[] pkOffsets;   // -1 means deleted/absent
    int[] pkLengths;
    volatile int liveCount;

    // ThreadLocal cache of GraphSearcher to avoid per-search allocation
    private ThreadLocal<GraphSearcher> searcherCache = new ThreadLocal<>();

    /** Page IDs for the graph chunks (needed to re-persist sealed segments). */
    List<Long> graphPageIds = java.util.Collections.emptyList();
    /** Page IDs for the map chunks (needed to re-persist sealed segments). */
    List<Long> mapPageIds = java.util.Collections.emptyList();

    VectorSegment(int segmentId) {
        this.segmentId = segmentId;
    }

    /**
     * Returns the PK for the given ordinal, or null if deleted/absent.
     */
    Bytes getPkForOrdinal(int ordinal) {
        int[] offsets = this.pkOffsets;
        if (offsets == null || ordinal < 0 || ordinal >= offsets.length) {
            return null;
        }
        int off = offsets[ordinal];
        if (off < 0) {
            return null;
        }
        return Bytes.from_array(pkData, off, pkLengths[ordinal]);
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
        int[] offsets = this.pkOffsets;
        if (offsets == null) {
            return;
        }
        int activeCount = this.liveCount;
        int k = Math.min(topK, activeCount);
        if (k == 0) {
            return;
        }
        Bits acceptBits = ordinal -> ordinal >= 0 && ordinal < offsets.length && offsets[ordinal] >= 0;
        try {
            GraphSearcher searcher = searcherCache.get();
            if (searcher == null) {
                searcher = new GraphSearcher(odg);
                searcherCache.set(searcher);
            }
            OnDiskGraphIndex.View view = (OnDiskGraphIndex.View) searcher.getView();
            io.github.jbellis.jvector.graph.similarity.ScoreFunction.ApproximateScoreFunction approxSF =
                    view.approximateScoreFunctionFor(qv, similarityFunction);
            io.github.jbellis.jvector.graph.similarity.ScoreFunction.ExactScoreFunction reranker =
                    view.rerankerFor(qv, similarityFunction);
            DefaultSearchScoreProvider ssp = new DefaultSearchScoreProvider(approxSF, reranker);
            SearchResult sr = searcher.search(ssp, k, acceptBits);
            for (SearchResult.NodeScore ns : sr.getNodes()) {
                Bytes pk = getPkForOrdinal(ns.node);
                if (pk != null) {
                    results.add(new AbstractMap.SimpleImmutableEntry<>(pk, ns.score));
                }
            }
        } catch (OutOfMemoryError e) {
            LOGGER.log(Level.SEVERE, "OOM searching on-disk graph segment " + segmentId, e);
            throw e;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "error searching on-disk graph segment " + segmentId, e);
            throw new RuntimeException("error searching on-disk graph segment " + segmentId, e);
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
        int[] offsets = this.pkOffsets;
        if (offsets != null) {
            int ord = onDiskOrdinal.intValue();
            if (ord >= 0 && ord < offsets.length) {
                offsets[ord] = -1;
                liveCount--;
            }
        }
        dirty = true;
        return true;
    }

    /**
     * Returns the number of live nodes in this segment.
     */
    long size() {
        return liveCount;
    }

    /**
     * Returns true if this segment is large enough to be considered sealed
     * (should not be rewritten during checkpoint unless it has deletes).
     */
    boolean isSealed(long maxSegmentSize) {
        return estimatedSizeBytes >= (long) (maxSegmentSize * 0.8) && !dirty;
    }

    /**
     * Scans all (ordinal, pk) entries in this segment via the in-memory cache.
     * Returns entries where key = ordinalToBytes(ordinal) and value = pk.
     */
    Stream<Map.Entry<Bytes, Bytes>> scanNodeToPk() {
        int[] offsets = this.pkOffsets;
        if (offsets == null) {
            return Stream.empty();
        }
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                new java.util.Iterator<Map.Entry<Bytes, Bytes>>() {
                    int i = 0;
                    @Override
                    public boolean hasNext() {
                        while (i < offsets.length) {
                            if (offsets[i] >= 0) {
                                return true;
                            }
                            i++;
                        }
                        return false;
                    }
                    @Override
                    public Map.Entry<Bytes, Bytes> next() {
                        int ordinal = i++;
                        Bytes pk = Bytes.from_array(pkData, offsets[ordinal], pkLengths[ordinal]);
                        return new AbstractMap.SimpleImmutableEntry<>(ordinalToBytes(ordinal), pk);
                    }
                }, Spliterator.ORDERED | Spliterator.NONNULL), false);
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
        this.pkData = null;
        this.pkOffsets = null;
        this.pkLengths = null;
        this.liveCount = 0;
        this.searcherCache = new ThreadLocal<>();
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
