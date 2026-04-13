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
import static org.junit.jupiter.api.Assertions.assertSame;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class VectorBenchTest {

    @Test
    void cycleVectorsExpandsList() {
        List<float[]> original = Arrays.asList(
                new float[]{1.0f}, new float[]{2.0f}, new float[]{3.0f});

        List<float[]> cycled = VectorBench.cycleVectors(original, 7);

        assertEquals(7, cycled.size());
        assertSame(original.get(0), cycled.get(0));
        assertSame(original.get(1), cycled.get(1));
        assertSame(original.get(2), cycled.get(2));
        assertSame(original.get(0), cycled.get(3));
        assertSame(original.get(1), cycled.get(4));
        assertSame(original.get(2), cycled.get(5));
        assertSame(original.get(0), cycled.get(6));
    }

    @Test
    void cycleVectorsReturnsOriginalWhenTargetSmallerOrEqual() {
        List<float[]> original = Arrays.asList(
                new float[]{1.0f}, new float[]{2.0f});

        assertSame(original, VectorBench.cycleVectors(original, 2));
        assertSame(original, VectorBench.cycleVectors(original, 1));
    }

    @Test
    void cycleVectorsExactMultiple() {
        List<float[]> original = Arrays.asList(
                new float[]{1.0f}, new float[]{2.0f});

        List<float[]> cycled = VectorBench.cycleVectors(original, 6);

        assertEquals(6, cycled.size());
        for (int i = 0; i < 6; i++) {
            assertSame(original.get(i % 2), cycled.get(i));
        }
    }

    @Test
    void configResumeFromDefaultIsZero() throws Exception {
        Config cfg = Config.parse(new String[]{});
        assertEquals(0, cfg.resumeFrom);
    }

    @Test
    void configResumeFromParsedFromCli() throws Exception {
        Config cfg = Config.parse(new String[]{"--resume-from", "500000"});
        assertEquals(500000, cfg.resumeFrom);
    }

    @Test
    void streamBaseVectorsWithSkipReturnsTailVectors(@TempDir File tmpDir) throws Exception {
        // Write 5 fvecs vectors with dim=2: [1,1], [2,2], [3,3], [4,4], [5,5]
        File datasetDir = new File(tmpDir, "sift");
        datasetDir.mkdirs();
        File fvecsFile = new File(datasetDir, "sift_base.fvecs");
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(fvecsFile))) {
            for (int i = 1; i <= 5; i++) {
                writeLittleEndianInt(dos, 2); // dim
                writeLittleEndianFloat(dos, i);
                writeLittleEndianFloat(dos, i);
            }
        }

        DatasetLoader loader = new DatasetLoader(tmpDir.getAbsolutePath(),
                DatasetLoader.DatasetPreset.SIFT1M, null);

        // Skip first 2 vectors, read next 3
        List<float[]> result = new ArrayList<>();
        try (DatasetLoader.VectorStream stream = loader.streamBaseVectors(2, 3)) {
            for (float[] v : stream) {
                result.add(v);
            }
        }

        assertEquals(3, result.size());
        assertArrayEquals(new float[]{3.0f, 3.0f}, result.get(0), 0.001f);
        assertArrayEquals(new float[]{4.0f, 4.0f}, result.get(1), 0.001f);
        assertArrayEquals(new float[]{5.0f, 5.0f}, result.get(2), 0.001f);
    }

    @Test
    void streamBaseVectorsWithZeroSkipReturnsAllVectors(@TempDir File tmpDir) throws Exception {
        File datasetDir = new File(tmpDir, "sift");
        datasetDir.mkdirs();
        File fvecsFile = new File(datasetDir, "sift_base.fvecs");
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(fvecsFile))) {
            for (int i = 1; i <= 3; i++) {
                writeLittleEndianInt(dos, 1); // dim
                writeLittleEndianFloat(dos, i);
            }
        }

        DatasetLoader loader = new DatasetLoader(tmpDir.getAbsolutePath(),
                DatasetLoader.DatasetPreset.SIFT1M, null);

        List<float[]> result = new ArrayList<>();
        try (DatasetLoader.VectorStream stream = loader.streamBaseVectors(0, 3)) {
            for (float[] v : stream) {
                result.add(v);
            }
        }

        assertEquals(3, result.size());
        assertArrayEquals(new float[]{1.0f}, result.get(0), 0.001f);
        assertArrayEquals(new float[]{2.0f}, result.get(1), 0.001f);
        assertArrayEquals(new float[]{3.0f}, result.get(2), 0.001f);
    }

    @Test
    void configCheckpointTimeoutDefaultIs300() throws Exception {
        Config cfg = Config.parse(new String[]{});
        assertEquals(300, cfg.checkpointTimeoutSeconds);
    }

    @Test
    void configCheckpointTimeoutParsedFromCli() throws Exception {
        Config cfg = Config.parse(new String[]{"--checkpoint-timeout-seconds", "7200"});
        assertEquals(7200, cfg.checkpointTimeoutSeconds);
    }

    @Test
    void configNoProgressDefaultIsFalse() throws Exception {
        Config cfg = Config.parse(new String[]{});
        assertEquals(false, cfg.noProgress);
        assertEquals(Config.OutputFormat.TEXT, cfg.outputFormat);
    }

    @Test
    void configNoProgressParsedFromCli() throws Exception {
        Config cfg = Config.parse(new String[]{"--no-progress"});
        assertEquals(true, cfg.noProgress);
        assertEquals(Config.OutputFormat.TEXT, cfg.outputFormat);
    }

    @Test
    void configOutputFormatJsonImpliesNoProgress() throws Exception {
        Config cfg = Config.parse(new String[]{"--output-format", "json"});
        assertEquals(Config.OutputFormat.JSON, cfg.outputFormat);
        assertEquals(true, cfg.noProgress);
    }

    @Test
    void configOutputFormatTextLeavesNoProgressFalse() throws Exception {
        Config cfg = Config.parse(new String[]{"--output-format", "text"});
        assertEquals(Config.OutputFormat.TEXT, cfg.outputFormat);
        assertEquals(false, cfg.noProgress);
    }

    @Test
    void configOutputFormatNdjsonAliasIsJson() throws Exception {
        Config cfg = Config.parse(new String[]{"--output-format", "NDJSON"});
        assertEquals(Config.OutputFormat.JSON, cfg.outputFormat);
        assertEquals(true, cfg.noProgress);
    }

    @Test
    void configOutputFormatBogusRejected() {
        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> Config.parse(new String[]{"--output-format", "yaml"}));
    }

    @Test
    void configNoProgressFromPropertiesFile(@TempDir File tmpDir) throws Exception {
        File props = new File(tmpDir, "bench.properties");
        try (java.io.PrintWriter pw = new java.io.PrintWriter(props)) {
            pw.println("no-progress=true");
            pw.println("output-format=json");
        }
        Config cfg = Config.parse(new String[]{"--config", props.getAbsolutePath()});
        assertEquals(true, cfg.noProgress);
        assertEquals(Config.OutputFormat.JSON, cfg.outputFormat);
    }

    private static void writeLittleEndianInt(DataOutputStream dos, int value) throws Exception {
        byte[] buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
        dos.write(buf);
    }

    private static void writeLittleEndianFloat(DataOutputStream dos, float value) throws Exception {
        byte[] buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
        dos.write(buf);
    }
}
