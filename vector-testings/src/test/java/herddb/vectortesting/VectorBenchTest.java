package herddb.vectortesting;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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

    private static void writeLittleEndianInt(DataOutputStream dos, int value) throws Exception {
        byte[] buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
        dos.write(buf);
    }

    private static void writeLittleEndianFloat(DataOutputStream dos, float value) throws Exception {
        byte[] buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
        dos.write(buf);
    }
}
