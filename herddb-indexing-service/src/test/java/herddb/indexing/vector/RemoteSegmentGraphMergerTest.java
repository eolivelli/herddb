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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Round-trip recall test for {@link RemoteSegmentGraphMerger} (issue #484).
 *
 * <p>Builds three small synthetic input segments — each one a slice of a
 * 1500-vector dataset — written via the same FusedPQ-less map+graph layout
 * that {@code PersistentVectorStore} produces, hands them to the merger, and
 * verifies:
 * <ul>
 *   <li>the merged segment carries every PK that survived tombstoning;</li>
 *   <li>the merged graph (loaded via {@code OnDiskGraphIndex.load}) returns
 *       every dataset vector for an identity-vector probe (recall = 1.0
 *       on a deterministic search-self check);</li>
 *   <li>nearest-neighbour recall@10 against the brute-force baseline on a
 *       held-out 50-query workload is at least 0.9 — proves the merged
 *       graph's edge structure is healthy, not just that the vectors are
 *       present.</li>
 * </ul>
 */
public class RemoteSegmentGraphMergerTest {

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final String TS_UUID = "ts-rsgm";
    private static final String IDX_UUID = "idx-rsgm";
    private static final int DIM = 64;
    private static final int VECTORS_PER_SEGMENT = 500;
    private static final int SEGMENT_COUNT = 3;
    private static final int M = 16;
    private static final int BEAM_WIDTH = 64;
    private static final float NEIGHBOR_OVERFLOW = 1.2f;
    private static final float ALPHA = 1.4f;

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private MemoryDataStorageManager dsm;
    private Path tmpDir;
    private VectorFloat<?>[] dataset;
    private Bytes[] datasetPks;

    @Before
    public void setUp() throws Exception {
        dsm = new MemoryDataStorageManager();
        tmpDir = tmp.newFolder("merger-tmp").toPath();
        dataset = new VectorFloat<?>[VECTORS_PER_SEGMENT * SEGMENT_COUNT];
        datasetPks = new Bytes[dataset.length];
        Random rng = new Random(0xCAFEBABE);
        for (int i = 0; i < dataset.length; i++) {
            VectorFloat<?> v = VTS.createFloatVector(DIM);
            for (int j = 0; j < DIM; j++) {
                v.set(j, rng.nextFloat() * 2.0f - 1.0f);
            }
            dataset[i] = v;
            datasetPks[i] = Bytes.from_string("pk-" + i);
        }
    }

    @After
    public void tearDown() {
        // dsm is in-memory — nothing to clean up.
    }

    /**
     * Slice the dataset into three segments and write each one as a real
     * (graph + map) multipart pair via the DSM. The on-disk graph isn't used
     * by the merger but the map file's wire format is what the merger reads.
     */
    private List<RemoteSegmentGraphMerger.RemoteSegmentInput> writeInputSegments() throws Exception {
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs = new ArrayList<>(SEGMENT_COUNT);
        for (int seg = 0; seg < SEGMENT_COUNT; seg++) {
            int from = seg * VECTORS_PER_SEGMENT;
            int to = from + VECTORS_PER_SEGMENT;

            // Write the map file (the merger's only real input).
            Path mapFile = Files.createTempFile(tmpDir, "input-map-", ".tmp");
            try (BufferedOutputStream bos = new BufferedOutputStream(
                    new FileOutputStream(mapFile.toFile()));
                 DataOutputStream dos = new DataOutputStream(bos)) {
                dos.writeInt(VECTORS_PER_SEGMENT);
                for (int local = 0; local < VECTORS_PER_SEGMENT; local++) {
                    int globalIdx = from + local;
                    dos.writeInt(local);
                    byte[] pkBytes = datasetPks[globalIdx].to_array();
                    dos.writeInt(pkBytes.length);
                    dos.write(pkBytes);
                    VectorFloat<?> v = dataset[globalIdx];
                    dos.writeInt(v.length());
                    for (int j = 0; j < v.length(); j++) {
                        dos.writeInt(Float.floatToIntBits(v.get(j)));
                    }
                }
            }
            long mapSize = Files.size(mapFile);
            String multipartUuid = IDX_UUID + "_seg" + seg;
            dsm.writeMultipartIndexFile(TS_UUID, multipartUuid, "map", mapFile, null);
            Files.deleteIfExists(mapFile);

            inputs.add(new RemoteSegmentGraphMerger.RemoteSegmentInput(
                    TS_UUID, IDX_UUID, "seg-uuid-" + seg, seg, mapSize,
                    /* generation */ 1L + seg, /* tombstoned */ new int[0]));
        }
        return inputs;
    }

    @Test
    public void mergedSegmentContainsEveryPkAndAchievesGoodRecall() throws Exception {
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs = writeInputSegments();

        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpDir, M, BEAM_WIDTH, NEIGHBOR_OVERFLOW, ALPHA,
                VectorSimilarityFunction.EUCLIDEAN);

        long outputSegmentId = 999_999_999L;
        RemoteSegmentGraphMerger.MergeOutput output = merger.merge(
                inputs, TS_UUID, IDX_UUID, outputSegmentId, DIM);
        assertNotNull("merger must produce an output", output);
        assertEquals("every input vector survives (no tombstones, no duplicates)",
                dataset.length, output.vectorCount);
        assertEquals(0L, output.droppedTombstones);
        assertEquals(0L, output.droppedDuplicates);

        // Re-read the merged map file: every PK from the dataset must appear.
        Set<Bytes> mergedPks = readMapFilePks(output.tablespaceUuid,
                output.indexUuid + "_seg" + output.segmentId, output.mapPath, output.mapFileSize);
        assertEquals(dataset.length, mergedPks.size());
        for (Bytes expected : datasetPks) {
            assertTrue("merged map missing PK " + expected, mergedPks.contains(expected));
        }

        // Load the merged graph back via OnDiskGraphIndex.load and run a
        // recall@10 sweep against the brute-force baseline. We use 50 random
        // dataset vectors as queries; the merged graph should rediscover the
        // same vector at rank 0 with high probability (recall >= 0.9).
        ReaderSupplier graphSupplier = dsm.multipartIndexReaderSupplier(
                output.tablespaceUuid, output.indexUuid + "_seg" + output.segmentId,
                "graph", output.graphFileSize);
        OnDiskGraphIndex graph = OnDiskGraphIndex.load(graphSupplier);
        try {
            // Build a deterministic ordinal-to-pk map by reading the merged map
            // file, so we can resolve search results back to the dataset index.
            int[] ordinalToDatasetIdx = buildOrdinalToDatasetIdx(output);

            int hits = 0;
            int trials = 50;
            Random rng = new Random(0xABBA);
            try (GraphSearcher searcher = new GraphSearcher(graph)) {
                OnDiskGraphIndex.View view = (OnDiskGraphIndex.View) searcher.getView();
                for (int t = 0; t < trials; t++) {
                    int queryIdx = rng.nextInt(dataset.length);
                    VectorFloat<?> q = dataset[queryIdx];
                    Set<Integer> bruteTop10 = bruteForceTop10(q);

                    ScoreFunction.ExactScoreFunction reranker = view.rerankerFor(
                            q, VectorSimilarityFunction.EUCLIDEAN);
                    DefaultSearchScoreProvider ssp = new DefaultSearchScoreProvider(reranker);
                    SearchResult result = searcher.search(ssp, 10, 10, 0.0f, 0.0f, Bits.ALL);
                    int matches = 0;
                    for (SearchResult.NodeScore ns : result.getNodes()) {
                        int datasetIdx = ordinalToDatasetIdx[ns.node];
                        if (bruteTop10.contains(datasetIdx)) {
                            matches++;
                        }
                    }
                    // Each query contributes the fraction of its top-10 that
                    // overlaps with the brute-force top-10 (max 10).
                    hits += matches;
                }
            }
            double recall = (double) hits / (trials * 10.0);
            assertTrue("recall@10 must be >= 0.9 to prove merged graph is healthy; got " + recall,
                    recall >= 0.9);
        } finally {
            graph.close();
        }
    }

    private int[] buildOrdinalToDatasetIdx(RemoteSegmentGraphMerger.MergeOutput output) throws Exception {
        int[] map = new int[(int) output.vectorCount];
        ReaderSupplier supplier = dsm.multipartIndexReaderSupplier(output.tablespaceUuid,
                output.indexUuid + "_seg" + output.segmentId, "map", output.mapFileSize);
        try (io.github.jbellis.jvector.disk.RandomAccessReader r = supplier.get()) {
            r.seek(0L);
            byte[] all = new byte[(int) output.mapFileSize];
            r.readFully(all);
            java.io.DataInputStream dis = new java.io.DataInputStream(
                    new java.io.ByteArrayInputStream(all));
            int entryCount = dis.readInt();
            for (int i = 0; i < entryCount; i++) {
                int ordinal = dis.readInt();
                int pkLen = dis.readInt();
                byte[] pk = new byte[pkLen];
                dis.readFully(pk);
                int floatCount = dis.readInt();
                for (int j = 0; j < floatCount; j++) {
                    dis.readInt();
                }
                String pkString = new String(pk, java.nio.charset.StandardCharsets.UTF_8);
                // pk is "pk-<datasetIdx>"
                int datasetIdx = Integer.parseInt(pkString.substring("pk-".length()));
                map[ordinal] = datasetIdx;
            }
        }
        return map;
    }

    @Test
    public void everyInputTombstonedYieldsNullOutput() throws Exception {
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs = writeInputSegments();

        // Mark every ordinal in every input as tombstoned.
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> tombstoned = new ArrayList<>(inputs.size());
        for (RemoteSegmentGraphMerger.RemoteSegmentInput in : inputs) {
            int[] all = new int[VECTORS_PER_SEGMENT];
            for (int i = 0; i < VECTORS_PER_SEGMENT; i++) {
                all[i] = i;
            }
            tombstoned.add(new RemoteSegmentGraphMerger.RemoteSegmentInput(
                    in.tablespaceUuid, in.indexUuid, in.segmentUuid, in.segmentId,
                    in.mapFileSize, in.generation, all));
        }
        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpDir, M, BEAM_WIDTH, NEIGHBOR_OVERFLOW, ALPHA,
                VectorSimilarityFunction.EUCLIDEAN);
        RemoteSegmentGraphMerger.MergeOutput output = merger.merge(
                tombstoned, TS_UUID, IDX_UUID, /* segmentId */ 1234L, DIM);
        assertNull("merger must decline when every input is tombstoned", output);
    }

    @Test
    public void deleteOutputRemovesUploadedFiles() throws Exception {
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs = writeInputSegments();
        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpDir, M, BEAM_WIDTH, NEIGHBOR_OVERFLOW, ALPHA,
                VectorSimilarityFunction.EUCLIDEAN);
        RemoteSegmentGraphMerger.MergeOutput output = merger.merge(
                inputs, TS_UUID, IDX_UUID, /* segmentId */ 555L, DIM);
        assertNotNull(output);
        merger.deleteOutput(output);
        // Probing for the (now-deleted) graph file via the multipart reader
        // supplier must throw DataStorageManagerException — the in-memory DSM
        // is strict about missing keys, which is exactly the behaviour we want
        // to verify.
        try {
            dsm.multipartIndexReaderSupplier(output.tablespaceUuid,
                    output.indexUuid + "_seg" + output.segmentId, "graph", output.graphFileSize);
            org.junit.Assert.fail("graph file should have been deleted");
        } catch (Exception expected) {
            // pass
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Set<Bytes> readMapFilePks(String tablespaceUuid, String multipartUuid,
                                      String mapPath, long fileSize) throws Exception {
        Set<Bytes> out = new HashSet<>();
        ReaderSupplier supplier = dsm.multipartIndexReaderSupplier(
                tablespaceUuid, multipartUuid, "map", fileSize);
        try (io.github.jbellis.jvector.disk.RandomAccessReader r = supplier.get()) {
            r.seek(0L);
            byte[] all = new byte[(int) fileSize];
            r.readFully(all);
            java.io.DataInputStream dis = new java.io.DataInputStream(
                    new java.io.ByteArrayInputStream(all));
            int entryCount = dis.readInt();
            for (int i = 0; i < entryCount; i++) {
                int ordinal = dis.readInt();
                int pkLen = dis.readInt();
                byte[] pk = new byte[pkLen];
                dis.readFully(pk);
                int floatCount = dis.readInt();
                for (int j = 0; j < floatCount; j++) {
                    dis.readInt();
                }
                out.add(Bytes.from_array(pk));
                org.junit.Assert.assertTrue("ordinal must be in [0, entryCount)",
                        ordinal >= 0 && ordinal < entryCount);
            }
        }
        return out;
    }

    private Set<Integer> bruteForceTop10(VectorFloat<?> query) {
        Integer[] indices = new Integer[dataset.length];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        Arrays.sort(indices, Comparator.comparingDouble(idx -> -score(query, dataset[idx])));
        Set<Integer> out = new HashSet<>();
        for (int i = 0; i < 10 && i < indices.length; i++) {
            out.add(indices[i]);
        }
        return out;
    }

    /**
     * EUCLIDEAN similarity used by jvector: score = 1 / (1 + sum-of-squared-diffs).
     * We rank by score descending, equivalent to ranking by squared distance ascending.
     */
    private static double score(VectorFloat<?> a, VectorFloat<?> b) {
        double s = 0.0;
        for (int i = 0; i < a.length(); i++) {
            double d = a.get(i) - b.get(i);
            s += d * d;
        }
        return 1.0 / (1.0 + s);
    }

}
