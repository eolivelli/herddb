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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import herddb.vectortesting.DatasetLoader.DatasetPreset;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies {@link DatasetLoader#loadGroundTruth(int, long)} picks the right per-prefix
 * ground-truth file out of a multi-checkpoint custom descriptor and fails hard on a
 * count that does not match any checkpoint.
 */
class DatasetLoaderGroundTruthTest {

    private static final int Q = 2;
    private static final int K = 3;

    @Test
    void exactMatchSelectsTheRightCheckpointFile(@TempDir Path tmp) throws Exception {
        Path subDir = setupCustomDataset(tmp, true);
        // Each ivecs file has a sentinel first id encoded as the size of the prefix it
        // represents, so we can confirm which file the loader actually opened.
        writeIvecs(subDir.resolve("multi_groundtruth_1000.ivecs").toFile(), 1000);
        writeIvecs(subDir.resolve("multi_groundtruth_5000.ivecs").toFile(), 5000);
        writeIvecs(subDir.resolve("multi_groundtruth.ivecs").toFile(), 100_000);

        DatasetLoader loader = new DatasetLoader(tmp.toString(), DatasetPreset.CUSTOM, null);
        loader.ensureDataset(); // short-circuits because the descriptor is already on disk
        loader.loadDescriptor();

        assertEquals(1000, firstId(loader.loadGroundTruth(Q, 1000)));
        assertEquals(5000, firstId(loader.loadGroundTruth(Q, 5000)));
        assertEquals(100_000, firstId(loader.loadGroundTruth(Q, 100_000)));
    }

    @Test
    void noMatchThrowsWithAvailableCheckpointsListed(@TempDir Path tmp) throws Exception {
        Path subDir = setupCustomDataset(tmp, true);
        writeIvecs(subDir.resolve("multi_groundtruth_1000.ivecs").toFile(), 1000);
        writeIvecs(subDir.resolve("multi_groundtruth_5000.ivecs").toFile(), 5000);
        writeIvecs(subDir.resolve("multi_groundtruth.ivecs").toFile(), 100_000);

        DatasetLoader loader = new DatasetLoader(tmp.toString(), DatasetPreset.CUSTOM, null);
        loader.ensureDataset();
        loader.loadDescriptor();

        IOException ex = assertThrows(IOException.class,
                () -> loader.loadGroundTruth(Q, 7777));
        String msg = ex.getMessage();
        assertTrue(msg.contains("baseVectorCount=7777"), msg);
        assertTrue(msg.contains("1000"), msg);
        assertTrue(msg.contains("5000"), msg);
        assertTrue(msg.contains("100000"), msg);
    }

    @Test
    void legacyDescriptorSingleFileResolvesAtTotalVectors(@TempDir Path tmp) throws Exception {
        // Old descriptor with only groundTruthFile. The synthesised singleton should be
        // the one returned for baseVectorCount == totalVectors.
        Path subDir = setupCustomDataset(tmp, false);
        writeIvecs(subDir.resolve("legacy_groundtruth.ivecs").toFile(), 100_000);

        DatasetLoader loader = new DatasetLoader(tmp.toString(), DatasetPreset.CUSTOM, null);
        loader.ensureDataset();
        loader.loadDescriptor();

        assertEquals(100_000, firstId(loader.loadGroundTruth(Q, 100_000)));
        assertThrows(IOException.class, () -> loader.loadGroundTruth(Q, 50_000));
    }

    private static Path setupCustomDataset(Path tmp, boolean withCheckpoints) throws IOException {
        // CUSTOM preset's subDir is "custom". We seed it with a descriptor and the IVECS
        // files the test wants to exercise.
        Path subDir = tmp.resolve(DatasetPreset.CUSTOM.subDir);
        Files.createDirectories(subDir);
        String prefix = withCheckpoints ? "multi" : "legacy";
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"name\": \"").append(prefix).append("\",\n");
        json.append("  \"format\": \"fvecs\",\n");
        json.append("  \"dimensions\": 4,\n");
        json.append("  \"similarity\": \"euclidean\",\n");
        json.append("  \"totalVectors\": 100000,\n");
        json.append("  \"numQueries\": 2,\n");
        json.append("  \"groundTruthK\": 3,\n");
        json.append("  \"baseFile\": \"").append(prefix).append("_base.fvecs\",\n");
        json.append("  \"queryFile\": \"").append(prefix).append("_query.fvecs\",\n");
        json.append("  \"groundTruthFile\": \"").append(prefix).append("_groundtruth.ivecs\"");
        if (withCheckpoints) {
            json.append(",\n  \"groundTruthCheckpoints\": [\n");
            json.append("    {\"baseVectorCount\": 1000, \"file\": \"multi_groundtruth_1000.ivecs\"},\n");
            json.append("    {\"baseVectorCount\": 5000, \"file\": \"multi_groundtruth_5000.ivecs\"},\n");
            json.append("    {\"baseVectorCount\": 100000, \"file\": \"multi_groundtruth.ivecs\"}\n");
            json.append("  ]");
        }
        json.append("\n}\n");
        Files.writeString(subDir.resolve(prefix + "_descriptor.json"), json.toString());
        return subDir;
    }

    /**
     * Writes a tiny IVECS file with {@code Q} rows each of length {@code K}. The first
     * value of the first row is set to {@code sentinel} so the test can identify which
     * file the loader actually opened.
     */
    private static void writeIvecs(File file, int sentinel) throws IOException {
        try (SiftWriter w = new SiftWriter(file)) {
            int[] firstRow = new int[K];
            firstRow[0] = sentinel;
            for (int i = 1; i < K; i++) {
                firstRow[i] = i;
            }
            w.writeIvec(firstRow);
            int[] secondRow = new int[K];
            for (int i = 0; i < K; i++) {
                secondRow[i] = sentinel + 100 + i;
            }
            w.writeIvec(secondRow);
        }
    }

    private static int firstId(List<int[]> gt) {
        return gt.get(0)[0];
    }
}
