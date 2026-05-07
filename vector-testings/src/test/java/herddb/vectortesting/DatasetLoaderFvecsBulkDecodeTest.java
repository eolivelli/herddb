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
package herddb.vectortesting;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import herddb.vectortesting.DatasetLoader.DatasetPreset;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Round-trip test for the FVECS streaming reader — see issue #443.
 *
 * <p>Validates the producer-side optimisations:
 * <ul>
 *   <li>{@code asFloatBuffer().get(float[])} bulk decode produces
 *       bit-identical results to writing via {@link SiftWriter}.</li>
 *   <li>The reused scratch byte buffer + FloatBuffer view does not leak
 *       data between consecutive vectors.</li>
 *   <li>{@code skipVectors} bulk-skip semantics correctly position the
 *       stream past the skipped portion (exact dim, no re-read of dim
 *       per skipped vector).</li>
 *   <li>The iterator returns a fresh {@code float[]} per vector — the
 *       reused scratch is internal-only.</li>
 * </ul>
 */
class DatasetLoaderFvecsBulkDecodeTest {

    @Test
    void bulkDecodeRoundtripsSmallDimVectors(@TempDir Path tmp) throws Exception {
        // dim=4 hits every code path with cheap data; the CUSTOM preset
        // exposes the full streamBaseVectors machinery to the test.
        roundtrip(tmp, /* dim */ 4, /* total */ 32, /* skip */ 0, /* maxRead */ 32);
    }

    @Test
    void bulkDecodeRoundtripsCommonProductionDim(@TempDir Path tmp) throws Exception {
        // dim=128 is the SIFT1M dimension; covers the bulk-FloatBuffer.get
        // hot path with realistic record sizes.
        roundtrip(tmp, /* dim */ 128, /* total */ 64, /* skip */ 0, /* maxRead */ 64);
    }

    @Test
    void bulkDecodeRoundtripsLargeDim(@TempDir Path tmp) throws Exception {
        // dim=1024 is typical of modern embedding models. Confirms the
        // bulk decode handles a record size that the JIT can vectorize.
        roundtrip(tmp, /* dim */ 1024, /* total */ 16, /* skip */ 0, /* maxRead */ 16);
    }

    @Test
    void skipBypassDoesNotMaterializeVectorsButLandsAtCorrectPosition(@TempDir Path tmp) throws Exception {
        // The bulk-skip path reads dim from the first record and then
        // bulk-skips the remaining (skipVectors - 1) records. We confirm
        // the stream resumes at the *correct* vector by verifying the
        // first emitted vector matches the (skipVectors)th written one.
        roundtrip(tmp, /* dim */ 8, /* total */ 100, /* skip */ 73, /* maxRead */ 27);
    }

    @Test
    void skipExceedingFileLengthThrowsClearException(@TempDir Path tmp) throws Exception {
        // Issue #443 review: the bulk-skip path reads dim from the first
        // record and then asks the underlying stream for one big skip(N)
        // covering the rest. A short / corrupt file must surface a loud
        // IOException rather than silently land at the wrong offset.
        int dim = 8;
        int totalWritten = 10;
        Path subDir = setupCustomDataset(tmp, dim, /* descriptorTotal */ 200);
        generateAndWriteFvecs(
                subDir.resolve("bulk_base.fvecs").toFile(), dim, totalWritten);
        generateAndWriteFvecs(subDir.resolve("bulk_query.fvecs").toFile(), dim, 2);

        DatasetLoader loader = new DatasetLoader(tmp.toString(), DatasetPreset.CUSTOM, null);
        loader.ensureDataset();
        loader.loadDescriptor();

        IOException ex = assertThrows(IOException.class,
                () -> loader.streamBaseVectors(/* skip */ 50, /* maxRead */ 1));
        // The bulk-skip path produces a "Unexpected end of stream while
        // skipping ..." message — assert that rather than a generic EOF.
        String msg = ex.getMessage();
        assertTrue(msg != null && msg.contains("Unexpected end of stream"),
                "expected bulk-skip EOF message, got: " + msg);
    }

    @Test
    void scratchReallocatesOnDimChange(@TempDir Path tmp) throws Exception {
        // Issue #443 review: the streaming reader caches one byte[] +
        // FloatBuffer view sized to the first vector's dim. A file that
        // violates the constant-dim invariant must still decode every
        // record correctly — the cachedDim != dim branch in
        // ensureScratch reallocates the scratch on the fly.
        Path subDir = tmp.resolve(DatasetPreset.CUSTOM.subDir);
        Files.createDirectories(subDir);
        // Write a hand-crafted FVECS file with two records of different
        // dim. We keep the descriptor's "dimensions" set to the first
        // record's dim (since the loader uses it for sanity logging
        // only — the streaming reader reads dim from each record header).
        File baseFile = subDir.resolve("bulk_base.fvecs").toFile();
        try (SiftWriter w = new SiftWriter(baseFile)) {
            float[] firstDim4 = new float[]{1.0f, 2.0f, 3.0f, 4.0f};
            float[] secondDim8 = new float[]{
                    10.5f, 11.5f, 12.5f, 13.5f, 14.5f, 15.5f, 16.5f, 17.5f};
            w.writeFvec(firstDim4);
            w.writeFvec(secondDim8);
        }
        // Query file required by descriptor (consistent dim is fine).
        generateAndWriteFvecs(subDir.resolve("bulk_query.fvecs").toFile(), 4, 2);
        // Descriptor declares dim=4; the second record's dim=8 is read
        // from the per-record header by the streamer.
        Files.writeString(subDir.resolve("bulk_descriptor.json"),
                "{\n"
                + "  \"name\": \"bulk\",\n"
                + "  \"format\": \"fvecs\",\n"
                + "  \"dimensions\": 4,\n"
                + "  \"similarity\": \"euclidean\",\n"
                + "  \"totalVectors\": 2,\n"
                + "  \"numQueries\": 2,\n"
                + "  \"groundTruthK\": 1,\n"
                + "  \"baseFile\": \"bulk_base.fvecs\",\n"
                + "  \"queryFile\": \"bulk_query.fvecs\",\n"
                + "  \"groundTruthFile\": \"bulk_groundtruth.ivecs\"\n"
                + "}\n");

        DatasetLoader loader = new DatasetLoader(tmp.toString(), DatasetPreset.CUSTOM, null);
        loader.ensureDataset();
        loader.loadDescriptor();

        try (DatasetLoader.VectorStream stream = loader.streamBaseVectors(0L, 2)) {
            Iterator<float[]> it = stream.iterator();
            float[] first = it.next();
            float[] second = it.next();
            assertEquals(4, first.length, "first record should be dim=4");
            assertEquals(8, second.length, "second record should be dim=8 (scratch reallocated)");
            assertArrayEquals(new float[]{1.0f, 2.0f, 3.0f, 4.0f}, first);
            assertArrayEquals(
                    new float[]{10.5f, 11.5f, 12.5f, 13.5f, 14.5f, 15.5f, 16.5f, 17.5f},
                    second);
        }
    }

    @Test
    void iteratorReturnsFreshFloatArrayPerVector(@TempDir Path tmp) throws Exception {
        // Issue #443: the optimised reader reuses an internal byte[] /
        // FloatBuffer view, but each iterator next() must still return a
        // distinct float[] so the caller can hold onto it without seeing
        // it overwritten by the next read.
        int dim = 16;
        int total = 8;
        Path subDir = setupCustomDataset(tmp, dim, total);
        List<float[]> written = generateAndWriteFvecs(
                subDir.resolve("bulk_base.fvecs").toFile(), dim, total);
        // Also write a query file (required by descriptor loader).
        generateAndWriteFvecs(subDir.resolve("bulk_query.fvecs").toFile(), dim, /* numQueries */ 2);

        DatasetLoader loader = new DatasetLoader(tmp.toString(), DatasetPreset.CUSTOM, null);
        loader.ensureDataset();
        loader.loadDescriptor();

        try (DatasetLoader.VectorStream stream = loader.streamBaseVectors(0L, total)) {
            Iterator<float[]> it = stream.iterator();
            float[] first = it.next();
            float[] second = it.next();
            assertNotSame(first, second, "iterator must return a fresh float[] per call");
            // Also verify the FIRST array is preserved — i.e., the second
            // read did not mutate it via the reused scratch.
            assertArrayEquals(written.get(0), first, "first vector should remain intact after second read");
            assertArrayEquals(written.get(1), second, "second vector should match the source");
        }
    }

    /**
     * End-to-end: write {@code total} vectors of dim {@code dim} via
     * {@link SiftWriter}, then re-read them via the streaming loader
     * with the configured {@code skipVectors} / {@code maxVectors}, and
     * assert bit-identical round-trip.
     */
    private void roundtrip(Path tmp, int dim, int total, int skip, int maxRead) throws Exception {
        Path subDir = setupCustomDataset(tmp, dim, total);
        List<float[]> written = generateAndWriteFvecs(
                subDir.resolve("bulk_base.fvecs").toFile(), dim, total);
        // Query file is required by the descriptor; one with the same
        // dim is enough for the loader to be happy.
        generateAndWriteFvecs(subDir.resolve("bulk_query.fvecs").toFile(), dim, /* numQueries */ 2);

        DatasetLoader loader = new DatasetLoader(tmp.toString(), DatasetPreset.CUSTOM, null);
        loader.ensureDataset();
        loader.loadDescriptor();

        List<float[]> read = new ArrayList<>(maxRead);
        try (DatasetLoader.VectorStream stream = loader.streamBaseVectors((long) skip, (long) maxRead)) {
            for (float[] vec : stream) {
                read.add(vec);
            }
        }

        int expectedReadCount = Math.min(maxRead, total - skip);
        assertEquals(expectedReadCount, read.size(),
                "expected " + expectedReadCount + " vectors after skip=" + skip
                        + " maxRead=" + maxRead + " total=" + total);

        for (int i = 0; i < read.size(); i++) {
            float[] expected = written.get(skip + i);
            float[] actual = read.get(i);
            assertArrayEquals(expected, actual,
                    "round-trip mismatch at vector " + (skip + i)
                            + " (dim=" + dim + ", skip=" + skip + ")");
        }
        // Sanity: the iteration emitted *the right vectors*, not just
        // any plausible-looking dim-aligned data.
        assertFalse(read.isEmpty(), "read list should not be empty for this configuration");
    }

    /**
     * Seeds a CUSTOM-preset dataset directory with a descriptor pointing
     * at the soon-to-be-written {@code bulk_base.fvecs} / {@code
     * bulk_query.fvecs} files.
     */
    private static Path setupCustomDataset(Path tmp, int dim, int totalVectors) throws IOException {
        Path subDir = tmp.resolve(DatasetPreset.CUSTOM.subDir);
        Files.createDirectories(subDir);
        String json = "{\n"
                + "  \"name\": \"bulk\",\n"
                + "  \"format\": \"fvecs\",\n"
                + "  \"dimensions\": " + dim + ",\n"
                + "  \"similarity\": \"euclidean\",\n"
                + "  \"totalVectors\": " + totalVectors + ",\n"
                + "  \"numQueries\": 2,\n"
                + "  \"groundTruthK\": 1,\n"
                + "  \"baseFile\": \"bulk_base.fvecs\",\n"
                + "  \"queryFile\": \"bulk_query.fvecs\",\n"
                + "  \"groundTruthFile\": \"bulk_groundtruth.ivecs\"\n"
                + "}\n";
        Files.writeString(subDir.resolve("bulk_descriptor.json"), json);
        return subDir;
    }

    /**
     * Writes {@code count} deterministic vectors of dimension {@code dim}
     * to {@code file}. The values are constructed so each component is
     * unique across the entire dataset (so off-by-one or wrap-around
     * round-trip bugs cannot accidentally pass).
     */
    private static List<float[]> generateAndWriteFvecs(File file, int dim, int count) throws IOException {
        List<float[]> vectors = new ArrayList<>(count);
        try (SiftWriter w = new SiftWriter(file)) {
            for (int v = 0; v < count; v++) {
                float[] vec = new float[dim];
                for (int d = 0; d < dim; d++) {
                    // (v + 1) * 1000 + d  → distinct float for every
                    // (vector, component) pair across the test dataset.
                    vec[d] = (v + 1) * 1000.0f + d;
                }
                w.writeFvec(vec);
                vectors.add(vec);
            }
        }
        return vectors;
    }
}
