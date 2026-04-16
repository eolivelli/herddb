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

import herddb.index.blink.BLink;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
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
import java.util.concurrent.atomic.AtomicInteger;
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

    /**
     * Overquery factor for approximate search: we ask jvector for more candidates
     * than topK, then rerank them with exact scores to improve recall.
     */
    static final int OVERQUERY_FACTOR = 3;

    static Bytes ordinalToBytes(int ordinal) {
        byte[] b = new byte[4];
        b[0] = (byte) (ordinal >>> 24);
        b[1] = (byte) (ordinal >>> 16);
        b[2] = (byte) (ordinal >>> 8);
        b[3] = (byte) ordinal;
        return Bytes.from_array(b);
    }

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
    final AtomicInteger liveCount = new AtomicInteger();
    int maxOrdinal = -1;

    // ThreadLocal cache of GraphSearcher to avoid per-search allocation
    private ThreadLocal<GraphSearcher> searcherCache = new ThreadLocal<>();

    /** Page IDs for the graph chunks (needed to re-persist sealed segments, page-based mode). */
    List<Long> graphPageIds = java.util.Collections.emptyList();
    /** Page IDs for the map chunks (needed to re-persist sealed segments, page-based mode). */
    List<Long> mapPageIds = java.util.Collections.emptyList();
    /** Payload sizes (in bytes) for each graph page, from segment metadata. */
    List<Integer> graphPageSizes = java.util.Collections.emptyList();
    /** Payload sizes (in bytes) for each map page, from segment metadata. */
    List<Integer> mapPageSizes = java.util.Collections.emptyList();

    /** Logical path of the graph multipart file (non-null only in multipart mode). */
    String graphFilePath;
    /** Total size in bytes of the graph multipart file (valid only when graphFilePath != null). */
    long graphFileSize;
    /** Logical path of the map multipart file (non-null only in multipart mode). */
    String mapFilePath;
    /** Total size in bytes of the map multipart file (valid only when mapFilePath != null). */
    long mapFileSize;

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
            LOGGER.log(Level.FINE, "segment {0}: skipping search, no on-disk graph", segmentId);
            return;
        }
        int[] offsets = this.pkOffsets;
        if (offsets == null) {
            LOGGER.log(Level.FINE, "segment {0}: skipping search, no pk offsets", segmentId);
            return;
        }
        int activeCount = this.liveCount.get();
        int k = Math.min(topK, activeCount);
        if (k == 0) {
            LOGGER.log(Level.FINE, "segment {0}: skipping search, no active nodes", segmentId);
            return;
        }
        LOGGER.log(Level.FINE, "segment {0}: searching topK={1}, activeCount={2}, effectiveK={3}",
                new Object[]{segmentId, topK, activeCount, k});
        long segStart = System.nanoTime();
        Bits acceptBits = ordinal -> ordinal >= 0 && ordinal < offsets.length && offsets[ordinal] >= 0;
        try {
            GraphSearcher searcher = searcherCache.get();
            if (searcher == null) {
                searcher = new GraphSearcher(odg);
                searcherCache.set(searcher);
            }
            OnDiskGraphIndex.View view = (OnDiskGraphIndex.View) searcher.getView();
            io.github.jbellis.jvector.graph.similarity.ScoreFunction.ExactScoreFunction reranker =
                    view.rerankerFor(qv, similarityFunction);
            DefaultSearchScoreProvider ssp;
            int rerankK;
            if (odg.getFeatureSet().contains(FeatureId.FUSED_PQ)) {
                io.github.jbellis.jvector.graph.similarity.ScoreFunction.ApproximateScoreFunction approxSF =
                        view.approximateScoreFunctionFor(qv, similarityFunction);
                ssp = new DefaultSearchScoreProvider(approxSF, reranker);
                rerankK = Math.min(k * OVERQUERY_FACTOR, activeCount);
            } else {
                // Segment written without FusedPQ (tail shard < MIN_VECTORS_FOR_FUSED_PQ).
                // InlineVectors are always present; use exact scoring for the beam search.
                ssp = new DefaultSearchScoreProvider(reranker);
                rerankK = k;
            }
            SearchResult sr = searcher.search(ssp, k, rerankK, 0.0f, 0.0f, acceptBits);
            int matched = 0;
            for (SearchResult.NodeScore ns : sr.getNodes()) {
                Bytes pk = getPkForOrdinal(ns.node);
                if (pk != null) {
                    results.add(new AbstractMap.SimpleImmutableEntry<>(pk, ns.score));
                    matched++;
                }
            }
            long segElapsedUs = java.util.concurrent.TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - segStart);
            LOGGER.log(Level.FINE, "segment {0}: search completed in {1} us, {2} results matched",
                    new Object[]{segmentId, segElapsedUs, matched});
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
                liveCount.decrementAndGet();
            }
        }
        dirty = true;
        return true;
    }

    /**
     * Returns the number of live nodes in this segment.
     */
    long size() {
        return liveCount.get();
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
        this.liveCount.set(0);
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
