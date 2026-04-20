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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import herddb.vectortesting.DatasetLoader.DatasetDescriptor;
import herddb.vectortesting.DatasetLoader.DatasetPreset;
import herddb.vectortesting.DatasetLoader.VecFormat;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers the descriptor + custom-mode loader round-trip for both FVECS and
 * BVECS on-disk formats without touching the network.
 */
class StandardDatasetPublisherTest {

    @Test
    void publishGist1mThenReadAsCustom(@TempDir Path tmp) throws Exception {
        // Arrange: emulate a tiny GIST1M-shaped source directory.
        int dim = 4;
        int baseCount = 32;
        int queryCount = 3;
        int gtK = 5;

        File sourceDatasetRoot = new File(tmp.toFile(), "src");
        File sourceSubDir = new File(sourceDatasetRoot, DatasetPreset.GIST1M.subDir);
        sourceSubDir.mkdirs();

        writeFvecs(new File(sourceSubDir, "gist_base.fvecs"), dim, baseCount, 0.0f);
        writeFvecs(new File(sourceSubDir, "gist_query.fvecs"), dim, queryCount, 100.0f);
        writeIvecs(new File(sourceSubDir, "gist_groundtruth.ivecs"), gtK, queryCount);

        File stagingDir = new File(tmp.toFile(), "staging");

        // Act: run the publisher (no --gs-path → staging only).
        StandardDatasetPublisher.main(new String[]{
                "--dataset", "gist1m",
                "--dataset-dir", sourceDatasetRoot.getAbsolutePath(),
                "--output-dir", stagingDir.getAbsolutePath()
        });

        // Assert: descriptor exists and has the right metadata.
        File descriptorFile = new File(stagingDir, "gist1m_descriptor.json");
        assertTrue(descriptorFile.isFile(), "descriptor not written");
        JsonNode root = new ObjectMapper().readTree(descriptorFile);
        assertEquals("gist1m", root.get("name").asText());
        assertEquals("fvecs", root.get("format").asText());
        assertEquals("euclidean", root.get("similarity").asText());
        assertEquals(dim, root.get("dimensions").asInt());
        assertEquals(baseCount, root.get("totalVectors").asLong());
        assertEquals(queryCount, root.get("numQueries").asLong());
        assertEquals(gtK, root.get("groundTruthK").asInt());

        // Round-trip: read the staged dataset back as CUSTOM.
        assertCustomRoundTripWorks(stagingDir, dim, baseCount, queryCount, gtK, VecFormat.FVECS);
    }

    @Test
    void publishBigannThenReadAsCustomPreservesBvecsFormat(@TempDir Path tmp) throws Exception {
        // Arrange: emulate a tiny BIGANN-shaped source directory with a nested
        // ground-truth path (bigann_gnd/idx_1000000000.ivecs) — the publisher
        // should flatten it.
        int dim = 4;
        int baseCount = 16;
        int queryCount = 2;
        int gtK = 3;

        File sourceDatasetRoot = new File(tmp.toFile(), "src");
        File sourceSubDir = new File(sourceDatasetRoot, DatasetPreset.BIGANN.subDir);
        new File(sourceSubDir, "bigann_gnd").mkdirs();

        writeBvecs(new File(sourceSubDir, "bigann_base.bvecs"), dim, baseCount, 0);
        writeBvecs(new File(sourceSubDir, "bigann_query.bvecs"), dim, queryCount, 50);
        writeIvecs(new File(sourceSubDir, "bigann_gnd/idx_1000000000.ivecs"), gtK, queryCount);

        File stagingDir = new File(tmp.toFile(), "staging");

        // Act.
        StandardDatasetPublisher.main(new String[]{
                "--dataset", "bigann",
                "--dataset-dir", sourceDatasetRoot.getAbsolutePath(),
                "--output-dir", stagingDir.getAbsolutePath()
        });

        // Assert: descriptor present, ground-truth flattened, format=bvecs.
        File descriptorFile = new File(stagingDir, "bigann_descriptor.json");
        assertTrue(descriptorFile.isFile(), "descriptor not written");
        JsonNode root = new ObjectMapper().readTree(descriptorFile);
        assertEquals("bigann", root.get("name").asText());
        assertEquals("bvecs", root.get("format").asText());
        assertEquals(dim, root.get("dimensions").asInt());
        assertEquals(baseCount, root.get("totalVectors").asLong());
        assertEquals(queryCount, root.get("numQueries").asLong());
        assertEquals(gtK, root.get("groundTruthK").asInt());
        assertEquals("bigann_groundtruth.ivecs", root.get("groundTruthFile").asText());
        assertTrue(new File(stagingDir, "bigann_groundtruth.ivecs").isFile(),
                "flattened ground truth should exist at the staging root");

        // Round-trip: CUSTOM loader must honour format=bvecs from the descriptor.
        assertCustomRoundTripWorks(stagingDir, dim, baseCount, queryCount, gtK, VecFormat.BVECS);
    }

    @Test
    void descriptorDefaultsToFvecsWhenFormatMissing(@TempDir Path tmp) throws Exception {
        File dir = tmp.toFile();
        File descriptor = new File(dir, "noformat_descriptor.json");
        Files.writeString(descriptor.toPath(),
                "{\"name\":\"noformat\",\"similarity\":\"euclidean\","
                        + "\"dimensions\":3,\"totalVectors\":10,\"numQueries\":1,"
                        + "\"groundTruthK\":1,"
                        + "\"baseFile\":\"b.fvecs\",\"queryFile\":\"q.fvecs\","
                        + "\"groundTruthFile\":\"g.ivecs\"}");
        DatasetDescriptor desc = DatasetDescriptor.load(descriptor);
        assertEquals(VecFormat.FVECS, desc.format, "missing format should default to FVECS");
    }

    // ---- helpers ----

    private static void assertCustomRoundTripWorks(File stagingDir, int dim, int baseCount,
                                                   int queryCount, int gtK, VecFormat expectedFormat)
            throws IOException {
        // Use the staging dir *as* the dataset root for CUSTOM mode: subDir is
        // "custom", so place the staged files under <dataset-dir>/custom/.
        File datasetRoot = new File(stagingDir.getParentFile(), "roundtrip-root");
        File customSubDir = new File(datasetRoot, DatasetPreset.CUSTOM.subDir);
        customSubDir.mkdirs();
        for (File f : stagingDir.listFiles()) {
            Files.copy(f.toPath(), new File(customSubDir, f.getName()).toPath());
        }

        DatasetLoader loader = new DatasetLoader(datasetRoot.getAbsolutePath(),
                DatasetPreset.CUSTOM, "file://dummy_descriptor.json");
        DatasetDescriptor desc = loader.loadDescriptor();
        assertNotNull(desc);
        assertEquals(expectedFormat, desc.format);

        List<float[]> base = loader.loadBaseVectors(baseCount);
        assertEquals(baseCount, base.size());
        assertEquals(dim, base.get(0).length);

        List<float[]> queries = loader.loadQueryVectors(queryCount);
        assertEquals(queryCount, queries.size());
        assertEquals(dim, queries.get(0).length);

        List<int[]> gt = loader.loadGroundTruth(queryCount);
        assertEquals(queryCount, gt.size());
        assertEquals(gtK, gt.get(0).length);

        // Streaming path must also honour the descriptor-provided format.
        try (DatasetLoader.VectorStream stream = loader.streamBaseVectors(0L, baseCount)) {
            int seen = 0;
            for (float[] v : stream) {
                assertEquals(dim, v.length);
                seen++;
            }
            assertEquals(baseCount, seen);
        }
    }

    private static void writeFvecs(File f, int dim, int count, float bias) throws IOException {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(f))) {
            ByteBuffer header = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            ByteBuffer vec = ByteBuffer.allocate(4 * dim).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < count; i++) {
                header.clear();
                header.putInt(dim);
                out.write(header.array());
                vec.clear();
                for (int d = 0; d < dim; d++) {
                    vec.putFloat(bias + i + d * 0.1f);
                }
                out.write(vec.array());
            }
        }
    }

    private static void writeBvecs(File f, int dim, int count, int bias) throws IOException {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(f))) {
            ByteBuffer header = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            byte[] vec = new byte[dim];
            for (int i = 0; i < count; i++) {
                header.clear();
                header.putInt(dim);
                out.write(header.array());
                for (int d = 0; d < dim; d++) {
                    vec[d] = (byte) ((bias + i + d) & 0xFF);
                }
                out.write(vec);
            }
        }
    }

    private static void writeIvecs(File f, int k, int count) throws IOException {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(f))) {
            ByteBuffer header = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            ByteBuffer rec = ByteBuffer.allocate(4 * k).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < count; i++) {
                header.clear();
                header.putInt(k);
                out.write(header.array());
                rec.clear();
                for (int j = 0; j < k; j++) {
                    rec.putInt(i * 100 + j);
                }
                out.write(rec.array());
            }
        }
    }
}
