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

package herddb.indexing;

import static org.junit.Assert.assertEquals;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that the zero-copy {@link PersistentVectorStore#addVector(Bytes, ByteBuffer)}
 * and {@link PersistentVectorStore#search(ByteBuffer, int)} overloads produce the
 * same results as their {@code float[]} counterparts, across both little- and
 * big-endian buffers.
 *
 * <p>The little-endian buffer hits jvector's native-SIMD fast path on x86 / ARM;
 * the big-endian buffer falls back to the Panama scalar path. Both are expected
 * to produce identical top-K results for identical inputs.
 */
public class PersistentVectorStoreByteBufferOverloadTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
    }

    private static float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private static ByteBuffer toBuffer(float[] v, ByteOrder order) {
        ByteBuffer bb = ByteBuffer.allocate(v.length * Float.BYTES).order(order);
        bb.asFloatBuffer().put(v);
        return bb;
    }

    private static void assertResultsEqual(List<Map.Entry<Bytes, Float>> expected,
                                           List<Map.Entry<Bytes, Float>> actual) {
        assertEquals("result size mismatch", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("PK mismatch at rank " + i,
                    expected.get(i).getKey(), actual.get(i).getKey());
            // Scores should be numerically identical for identical vectors — allow
            // a small tolerance in case the SIMD vs scalar paths round slightly
            // differently for big-endian inputs.
            assertEquals("score mismatch at rank " + i,
                    expected.get(i).getValue(), actual.get(i).getValue(), 1e-5f);
        }
    }

    @Test
    public void testAddViaByteBufferMatchesFloatArray() throws Exception {
        for (ByteOrder order : new ByteOrder[]{ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN}) {
            Path dirA = tmpFolder.newFolder().toPath();
            Path dirB = tmpFolder.newFolder().toPath();
            try (PersistentVectorStore storeA = createStore(dirA);
                 PersistentVectorStore storeB = createStore(dirB)) {
                storeA.start();
                storeB.start();

                Random rng = new Random(42);
                int dim = 32;
                int n = 80;
                float[] query = randomVector(rng, dim);

                // Generate the same vectors in both stores but insert them via
                // different overloads.
                rng = new Random(1234);
                for (int i = 0; i < n; i++) {
                    float[] v = randomVector(rng, dim);
                    storeA.addVector(Bytes.from_int(i), v);
                    storeB.addVector(Bytes.from_int(i), toBuffer(v, order));
                }

                assertEquals(n, storeA.size());
                assertEquals(n, storeB.size());

                List<Map.Entry<Bytes, Float>> resultsA = storeA.search(query, 10);
                List<Map.Entry<Bytes, Float>> resultsB = storeB.search(query, 10);
                assertResultsEqual(resultsA, resultsB);
            }
        }
    }

    @Test
    public void testSearchViaByteBufferMatchesFloatArray() throws Exception {
        for (ByteOrder order : new ByteOrder[]{ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN}) {
            Path dir = tmpFolder.newFolder().toPath();
            try (PersistentVectorStore store = createStore(dir)) {
                store.start();

                Random rng = new Random(7);
                int dim = 32;
                int n = 80;
                for (int i = 0; i < n; i++) {
                    store.addVector(Bytes.from_int(i), randomVector(rng, dim));
                }

                float[] query = randomVector(rng, dim);
                List<Map.Entry<Bytes, Float>> fromArray = store.search(query, 10);
                List<Map.Entry<Bytes, Float>> fromBuffer = store.search(toBuffer(query, order), 10);
                assertResultsEqual(fromArray, fromBuffer);
            }
        }
    }

    @Test
    public void testEmptyAndNullBuffersAreNoOps() throws Exception {
        Path dir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(dir)) {
            store.start();
            // null and empty should be no-ops — same contract as the float[] overload
            store.addVector(Bytes.from_int(1), (ByteBuffer) null);
            store.addVector(Bytes.from_int(2), ByteBuffer.allocate(0));
            assertEquals(0, store.size());
        }
    }
}
