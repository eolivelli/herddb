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
import static org.junit.jupiter.api.Assertions.assertSame;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

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
