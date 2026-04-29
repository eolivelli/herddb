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
import static org.junit.jupiter.api.Assertions.assertTrue;
import herddb.vectortesting.DatasetLoader.DatasetDescriptor;
import herddb.vectortesting.DatasetLoader.DatasetDescriptor.GroundTruthCheckpoint;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises {@link DatasetDescriptor#load(File)} for both the legacy descriptor
 * shape (no {@code groundTruthCheckpoints} array) and the new multi-checkpoint
 * shape, including the synthesised-singleton fallback path.
 */
class DatasetDescriptorTest {

    @Test
    void legacyDescriptorSynthesizesSingletonCheckpoint(@TempDir Path tmp) throws Exception {
        // Old-format descriptor with only the legacy groundTruthFile field.
        String json = "{\n"
                + "  \"name\": \"legacy\",\n"
                + "  \"format\": \"fvecs\",\n"
                + "  \"dimensions\": 32,\n"
                + "  \"similarity\": \"euclidean\",\n"
                + "  \"totalVectors\": 1000000,\n"
                + "  \"numQueries\": 1000,\n"
                + "  \"groundTruthK\": 100,\n"
                + "  \"baseFile\": \"legacy_base.fvecs\",\n"
                + "  \"queryFile\": \"legacy_query.fvecs\",\n"
                + "  \"groundTruthFile\": \"legacy_groundtruth.ivecs\"\n"
                + "}\n";
        File f = tmp.resolve("legacy_descriptor.json").toFile();
        Files.writeString(f.toPath(), json);

        DatasetDescriptor desc = DatasetDescriptor.load(f);

        assertEquals(1_000_000L, desc.totalVectors);
        assertEquals("legacy_groundtruth.ivecs", desc.groundTruthFile);
        assertEquals(1, desc.groundTruthCheckpoints.size(),
                "Old descriptor should synthesize a singleton checkpoint at totalVectors");
        GroundTruthCheckpoint cp = desc.groundTruthCheckpoints.get(0);
        assertEquals(1_000_000L, cp.baseVectorCount);
        assertEquals("legacy_groundtruth.ivecs", cp.file);
    }

    @Test
    void newDescriptorPreservesCheckpointArray(@TempDir Path tmp) throws Exception {
        String json = "{\n"
                + "  \"name\": \"multi\",\n"
                + "  \"format\": \"fvecs\",\n"
                + "  \"dimensions\": 4,\n"
                + "  \"similarity\": \"cosine\",\n"
                + "  \"totalVectors\": 1000000000,\n"
                + "  \"numQueries\": 1000,\n"
                + "  \"groundTruthK\": 100,\n"
                + "  \"baseFile\": \"multi_base.fvecs\",\n"
                + "  \"queryFile\": \"multi_query.fvecs\",\n"
                + "  \"groundTruthFile\": \"multi_groundtruth.ivecs\",\n"
                + "  \"groundTruthCheckpoints\": [\n"
                + "    {\"baseVectorCount\": 1000000, \"file\": \"multi_groundtruth_1000000.ivecs\"},\n"
                + "    {\"baseVectorCount\": 50000000, \"file\": \"multi_groundtruth_50000000.ivecs\"},\n"
                + "    {\"baseVectorCount\": 1000000000, \"file\": \"multi_groundtruth.ivecs\"}\n"
                + "  ]\n"
                + "}\n";
        File f = tmp.resolve("multi_descriptor.json").toFile();
        Files.writeString(f.toPath(), json);

        DatasetDescriptor desc = DatasetDescriptor.load(f);

        List<GroundTruthCheckpoint> cps = desc.groundTruthCheckpoints;
        assertEquals(3, cps.size());
        assertEquals(1_000_000L, cps.get(0).baseVectorCount);
        assertEquals("multi_groundtruth_1000000.ivecs", cps.get(0).file);
        assertEquals(50_000_000L, cps.get(1).baseVectorCount);
        assertEquals(1_000_000_000L, cps.get(2).baseVectorCount);
        assertEquals("multi_groundtruth.ivecs", cps.get(2).file);
    }

    @Test
    void descriptorWithoutAnyGroundTruthYieldsEmptyCheckpointList(@TempDir Path tmp) throws Exception {
        // Hypothetical HDF5-style or in-flight descriptor where ground truth is not yet
        // present. Should not crash and should return an empty list, letting the loader
        // fall through to its own error path at use time.
        String json = "{\n"
                + "  \"name\": \"empty\",\n"
                + "  \"format\": \"fvecs\",\n"
                + "  \"dimensions\": 16,\n"
                + "  \"similarity\": \"euclidean\",\n"
                + "  \"totalVectors\": 100,\n"
                + "  \"numQueries\": 0,\n"
                + "  \"groundTruthK\": 0,\n"
                + "  \"baseFile\": \"empty_base.fvecs\",\n"
                + "  \"queryFile\": null\n"
                + "}\n";
        File f = tmp.resolve("empty_descriptor.json").toFile();
        Files.writeString(f.toPath(), json);

        DatasetDescriptor desc = DatasetDescriptor.load(f);

        assertTrue(desc.groundTruthCheckpoints.isEmpty(),
                "Descriptor missing both groundTruthFile and groundTruthCheckpoints "
                        + "should yield an empty list");
    }
}
