package herddb.vectortesting;

import org.junit.jupiter.api.Test;

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
}
