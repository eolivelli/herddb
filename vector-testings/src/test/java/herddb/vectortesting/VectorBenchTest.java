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
        assertEquals(false, cfg.resumeFromAuto);
    }

    @Test
    void configResumeFromAutoSetsFlag() throws Exception {
        Config cfg = Config.parse(new String[]{"--resume-from", "auto"});
        assertEquals(0, cfg.resumeFrom);
        assertEquals(true, cfg.resumeFromAuto);
    }

    @Test
    void configResumeFromAutoIsCaseInsensitive() throws Exception {
        Config cfg = Config.parse(new String[]{"--resume-from", "AUTO"});
        assertEquals(true, cfg.resumeFromAuto);

        Config cfg2 = Config.parse(new String[]{"--resume-from", "Auto"});
        assertEquals(true, cfg2.resumeFromAuto);
    }

    @Test
    void configResumeFromNumericLeavesAutoFalse() throws Exception {
        Config cfg = Config.parse(new String[]{"--resume-from", "42"});
        assertEquals(42L, cfg.resumeFrom);
        assertEquals(false, cfg.resumeFromAuto);
    }

    @Test
    void configQueryMaxOpsDefaultIsTen() throws Exception {
        Config cfg = Config.parse(new String[]{});
        assertEquals(10, cfg.queryMaxOpsPerSecond);
    }

    @Test
    void configQueryMaxOpsOverride() throws Exception {
        Config cfg = Config.parse(new String[]{"--query-max-ops", "250"});
        assertEquals(250, cfg.queryMaxOpsPerSecond);
    }

    @Test
    void configQueryMaxOpsZeroMeansUnlimited() throws Exception {
        Config cfg = Config.parse(new String[]{"--query-max-ops", "0"});
        assertEquals(0, cfg.queryMaxOpsPerSecond);
    }

    @Test
    void configToStringIncludesQueryMaxOps() throws Exception {
        Config cfg = Config.parse(new String[]{"--query-max-ops", "42"});
        String s = cfg.toString();
        org.junit.jupiter.api.Assertions.assertTrue(
                s.contains("queryMaxOpsPerSecond=42"),
                "toString() must include queryMaxOpsPerSecond; got: " + s);
    }

    @Test
    void configToStringShowsUnlimitedWhenQueryMaxOpsIsZero() throws Exception {
        Config cfg = Config.parse(new String[]{"--query-max-ops", "0"});
        String s = cfg.toString();
        org.junit.jupiter.api.Assertions.assertTrue(
                s.contains("queryMaxOpsPerSecond=unlimited"),
                "toString() must show 'unlimited' when queryMaxOpsPerSecond=0; got: " + s);
    }

    @Test
    void buildCreateVectorIndexSqlDefaultIncludesNumShardsFour() throws Exception {
        Config cfg = Config.parse(new String[]{});

        String sql = VectorBench.buildCreateVectorIndexSql(cfg);

        assertEquals(
                "CREATE VECTOR INDEX vidx ON vector_bench(vec)"
                        + " WITH m=16 beamWidth=100 similarity=" + cfg.effectiveSimilarity()
                        + " fusedPQ=true numShards 4",
                sql);
    }

    @Test
    void buildCreateVectorIndexSqlOmitsClauseWhenNumShardsIsOne() throws Exception {
        Config cfg = Config.parse(new String[]{"--index-num-shards", "1"});

        String sql = VectorBench.buildCreateVectorIndexSql(cfg);

        org.junit.jupiter.api.Assertions.assertFalse(
                sql.contains("numShards"),
                "numShards clause must be omitted when indexNumShards <= 1, got: " + sql);
        assertEquals(
                "CREATE VECTOR INDEX vidx ON vector_bench(vec)"
                        + " WITH m=16 beamWidth=100 similarity=" + cfg.effectiveSimilarity()
                        + " fusedPQ=true",
                sql);
    }

    @Test
    void buildCreateVectorIndexSqlOmitsClauseWhenNumShardsIsZero() throws Exception {
        Config cfg = Config.parse(new String[]{"--index-num-shards", "0"});

        String sql = VectorBench.buildCreateVectorIndexSql(cfg);

        org.junit.jupiter.api.Assertions.assertFalse(
                sql.contains("numShards"),
                "numShards clause must be omitted when indexNumShards <= 1, got: " + sql);
    }

    @Test
    void buildCreateVectorIndexSqlUsesCustomNumShards() throws Exception {
        Config cfg = Config.parse(new String[]{"--index-num-shards", "8"});

        String sql = VectorBench.buildCreateVectorIndexSql(cfg);

        org.junit.jupiter.api.Assertions.assertTrue(
                sql.endsWith("numShards 8"),
                "expected trailing `numShards 8`, got: " + sql);
    }

    @Test
    void buildCreateVectorIndexSqlReflectsCustomTableAndIndexParams() throws Exception {
        Config cfg = Config.parse(new String[]{
                "--table", "my_vectors",
                "--m", "32",
                "--beam-width", "200",
                "--similarity", "cosine",
                "--index-num-shards", "2"
        });

        String sql = VectorBench.buildCreateVectorIndexSql(cfg);

        assertEquals(
                "CREATE VECTOR INDEX vidx ON my_vectors(vec)"
                        + " WITH m=32 beamWidth=200 similarity=cosine"
                        + " fusedPQ=true numShards 2",
                sql);
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

    /**
     * Issue #251: when the dataset on disk has fewer vectors than the bench
     * was asked to ingest, the stream silently terminates early. The
     * {@code vectorsEmitted != toIngest} guard added in
     * {@code VectorBench.runBenchmark} relies on the count of vectors
     * actually emitted by this iterator — this test pins down the underlying
     * behaviour the guard depends on.
     */
    @Test
    void streamBaseVectorsStopsAtEofWhenRequestExceedsFileSize(@TempDir File tmpDir) throws Exception {
        File datasetDir = new File(tmpDir, "sift");
        datasetDir.mkdirs();
        File fvecsFile = new File(datasetDir, "sift_base.fvecs");
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(fvecsFile))) {
            for (int i = 1; i <= 4; i++) { // only 4 vectors on disk
                writeLittleEndianInt(dos, 2); // dim
                writeLittleEndianFloat(dos, i);
                writeLittleEndianFloat(dos, i);
            }
        }

        DatasetLoader loader = new DatasetLoader(tmpDir.getAbsolutePath(),
                DatasetLoader.DatasetPreset.SIFT1M, null);

        long emitted = 0;
        try (DatasetLoader.VectorStream stream = loader.streamBaseVectors(0, 100)) {
            for (float[] v : stream) {
                emitted++;
                org.junit.jupiter.api.Assertions.assertEquals(2, v.length, "dim mismatch");
            }
        }

        // The stream stopped at 4 (file EOF) instead of throwing for the
        // remaining 96 — VectorBench must therefore audit this count itself,
        // because committed-rows accounting alone would not detect the gap.
        assertEquals(4L, emitted,
                "stream must terminate at EOF without throwing — bench's vectorsEmitted check relies on this");
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

    // -----------------------------------------------------------------------
    // buildVectorIndexExistsSql tests
    // -----------------------------------------------------------------------

    @Test
    void buildVectorIndexExistsSqlQueriesSysindexes() {
        String sql = VectorBench.buildVectorIndexExistsSql();
        org.junit.jupiter.api.Assertions.assertTrue(
                sql.contains("sysindexes"),
                "SQL must query sysindexes; got: " + sql);
    }

    @Test
    void buildVectorIndexExistsSqlFiltersOnIndexName() {
        String sql = VectorBench.buildVectorIndexExistsSql();
        org.junit.jupiter.api.Assertions.assertTrue(
                sql.contains("index_name='vidx'"),
                "SQL must filter by index_name='vidx'; got: " + sql);
    }

    @Test
    void buildVectorIndexExistsSqlFiltersOnVectorType() {
        String sql = VectorBench.buildVectorIndexExistsSql();
        org.junit.jupiter.api.Assertions.assertTrue(
                sql.contains("index_type='vector'"),
                "SQL must filter by index_type='vector'; got: " + sql);
    }

    @Test
    void buildVectorIndexExistsSqlUsesPositionalParameterForTableName() {
        String sql = VectorBench.buildVectorIndexExistsSql();
        org.junit.jupiter.api.Assertions.assertTrue(
                sql.contains("table_name=?"),
                "SQL must use a positional parameter (?) for table_name; got: " + sql);
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
