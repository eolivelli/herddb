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
package herddb.indexing.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression gate for the legacy-rebuild eager-download path. Both of the
 * fallback arms inside {@link VectorIndexCompactor#rebuildSegmentStreaming}
 * — the heterogeneous-feature-set guard and the
 * {@code localSources.size() < 2} guard — must hand the legacy rebuild a list
 * of eagerly-downloaded local-file-backed {@code OnDiskGraphIndex} objects
 * instead of letting it read through {@code seg.onDiskGraph} (which is
 * backed by {@code RemoteRandomAccessReader} + {@code SegmentBlockCache} +
 * gRPC). The pre-fix behaviour issued tens of millions of small remote block
 * reads per cycle and stalled Phase B for hours at large segment counts.
 *
 * <p>This test asserts:
 * <ol>
 *   <li>{@link VectorIndexCompactor#COMPACTION_EAGER_DOWNLOAD_COUNT} advances
 *       by the candidate count before the legacy fallback fires (i.e. the
 *       download ran first).</li>
 *   <li>The merged segment is produced correctly and search returns
 *       results.</li>
 *   <li>No {@code herddb-compact-src-*.idx} temp file leaks after the cycle
 *       (the streaming method's outer {@code finally} still cleans up the
 *       sources it owns, even when control flow returned through the legacy
 *       path).</li>
 * </ol>
 */
public class RebuildSegmentLegacyEagerDownloadTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private int savedMinLiveVectors;

    @Before
    public void setUp() {
        savedMinLiveVectors = PersistentVectorStore.minLiveVectorsForCheckpoint;
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void tearDown() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLiveVectors;
        VectorIndexCompactor.syntheticShardObserverForTest = null;
    }

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private static int countDownloadTempFiles(Path dir) throws IOException {
        int count = 0;
        try (DirectoryStream<Path> ds =
                Files.newDirectoryStream(dir, "herddb-compact-src-*.idx")) {
            for (Path ignored : ds) {
                count++;
            }
        }
        return count;
    }

    /**
     * Heterogeneous-feature fallback: build one segment below
     * {@code MIN_VECTORS_FOR_FUSED_PQ} (InlineVectors only) and one at the
     * threshold (FusedPQ + InlineVectors). The streaming compactor detects the
     * mismatch and falls back to the legacy rebuild. The eager-download
     * counter must advance by at least 2 — proving the download ran before
     * the heterogeneous guard sent control to the legacy path.
     */
    @Test
    public void heterogeneousFallbackUsesEagerDownload() throws Exception {
        final int dim = 8;
        final int smallCount = 30;
        final int largeCount = PersistentVectorStore.MIN_VECTORS_FOR_FUSED_PQ;

        Path tmpDir = tmpFolder.newFolder("hetero-fallback").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        // Capture the synthetic shard so we can also assert the legacy path
        // actually fired (the streaming path never constructs one).
        AtomicReference<PersistentVectorStore.LiveGraphShard> observed =
                new AtomicReference<>();
        VectorIndexCompactor.syntheticShardObserverForTest = observed::set;

        long countBefore = VectorIndexCompactor.COMPACTION_EAGER_DOWNLOAD_COUNT.get();
        long bytesBefore = VectorIndexCompactor.COMPACTION_EAGER_DOWNLOAD_BYTES.get();

        try (PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vec_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE)) {

            // Fire compaction on the first 2 segments.
            store.configureCompaction(
                    /*intervalMs*/ Long.MAX_VALUE,
                    /*minBytes*/ 1L,
                    /*maxBytes*/ Long.MAX_VALUE,
                    /*minCount*/ 2,
                    /*maxCount*/ Integer.MAX_VALUE,
                    /*retentionMs*/ 0);

            store.start();

            Random rng = new Random(628L);
            // First checkpoint: small (InlineVectors only).
            for (int i = 0; i < smallCount; i++) {
                store.addVector(Bytes.from_long(i), vec(rng, dim));
            }
            store.checkpoint();

            // Second checkpoint: large (FusedPQ + InlineVectors).
            for (int i = 0; i < largeCount; i++) {
                store.addVector(Bytes.from_long(1_000_000L + i), vec(rng, dim));
            }
            store.checkpoint();

            assertEquals("two segments before compaction", 2, store.getSegmentCount());

            store.runCompactionCycle();

            assertEquals("compaction must produce exactly one merged segment",
                    1, store.getSegmentCount());

            // Search returns results (vectors were read correctly from the
            // local-file-backed graphs).
            float[] queryVec = vec(rng, dim);
            List<Map.Entry<Bytes, Float>> hits = store.search(queryVec, 1);
            assertFalse("search on merged segment must return at least one result", hits.isEmpty());
        }

        long countAfter = VectorIndexCompactor.COMPACTION_EAGER_DOWNLOAD_COUNT.get();
        long bytesAfter = VectorIndexCompactor.COMPACTION_EAGER_DOWNLOAD_BYTES.get();
        assertTrue("eager-download count must advance by >= 2 (one per source);"
                        + " before=" + countBefore + " after=" + countAfter,
                countAfter - countBefore >= 2);
        assertTrue("eager-download byte total must advance;"
                        + " before=" + bytesBefore + " after=" + bytesAfter,
                bytesAfter > bytesBefore);

        // The legacy path actually fired (heterogeneous guard sent control to it).
        assertTrue("syntheticShardObserverForTest must have fired —"
                        + " the heterogeneous-feature fallback must run the legacy path",
                observed.get() != null);

        // No source-graph temp file leak.
        assertEquals("no herddb-compact-src-*.idx temp files may leak after the cycle",
                0, countDownloadTempFiles(tmpDir));
    }

    /**
     * Single-source fallback: with {@code maxCount=1} every cycle picks a
     * single segment. {@link VectorIndexCompactor#rebuildSegmentStreaming}'s
     * {@code localSources.size() < 2} guard sends control to the legacy
     * path. The eager-download counter must still advance — proving the
     * single-source candidate was downloaded locally before the legacy
     * rebuild fired.
     */
    @Test
    public void singleSourceFallbackUsesEagerDownload() throws Exception {
        final int dim = 16;
        final int vectorsPerCheckpoint = 200;

        Path tmpDir = tmpFolder.newFolder("single-source").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        AtomicReference<PersistentVectorStore.LiveGraphShard> observed =
                new AtomicReference<>();
        VectorIndexCompactor.syntheticShardObserverForTest = observed::set;

        long fallbackBefore = VectorIndexCompactor.getStreamingFallbackToLegacyTotal();
        long countBefore = VectorIndexCompactor.COMPACTION_EAGER_DOWNLOAD_COUNT.get();

        try (PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vec_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE)) {

            // minCount=1, maxCount=1: every cycle picks exactly one segment.
            store.configureCompaction(
                    /*intervalMs*/ Long.MAX_VALUE,
                    /*minBytes*/ 1L,
                    /*maxBytes*/ Long.MAX_VALUE,
                    /*minCount*/ 1,
                    /*maxCount*/ 1,
                    /*retentionMs*/ 0);

            store.start();

            Random rng = new Random(629L);
            for (int i = 0; i < vectorsPerCheckpoint; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }
            store.checkpoint();

            assertEquals("one segment before compaction", 1, store.getSegmentCount());

            store.runCompactionCycle();

            assertEquals("single-source compaction must produce exactly one segment",
                    1, store.getSegmentCount());
        }

        long fallbackAfter = VectorIndexCompactor.getStreamingFallbackToLegacyTotal();
        assertTrue("STREAMING_FALLBACK_TO_LEGACY_TOTAL must advance for the single-source cycle;"
                        + " before=" + fallbackBefore + " after=" + fallbackAfter,
                fallbackAfter > fallbackBefore);

        long countAfter = VectorIndexCompactor.COMPACTION_EAGER_DOWNLOAD_COUNT.get();
        assertTrue("eager-download count must advance for the single-source candidate;"
                        + " before=" + countBefore + " after=" + countAfter,
                countAfter > countBefore);

        assertTrue("syntheticShardObserverForTest must have fired —"
                        + " the single-source fallback must run the legacy path",
                observed.get() != null);

        assertEquals("no herddb-compact-src-*.idx temp files may leak after the cycle",
                0, countDownloadTempFiles(tmpDir));
    }
}
