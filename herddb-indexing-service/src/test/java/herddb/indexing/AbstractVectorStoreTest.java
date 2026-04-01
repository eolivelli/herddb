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

import static org.junit.Assert.*;

import herddb.index.vector.AbstractVectorStore;
import herddb.utils.Bytes;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Tests for {@link AbstractVectorStore} using {@link InMemoryVectorStore} implementation.
 *
 * @author enrico.olivelli
 */
public class AbstractVectorStoreTest {

    private AbstractVectorStore createStore() {
        return new InMemoryVectorStore("embedding");
    }

    @Test
    public void testAddAndSearchTopK() {
        AbstractVectorStore store = createStore();

        // Add 10 vectors of dimension 3
        for (int i = 0; i < 10; i++) {
            float[] vec = new float[]{i * 0.1f, (10 - i) * 0.1f, 0.5f};
            store.addVector(Bytes.from_int(i), vec);
        }

        assertEquals(10, store.size());

        // Search for top-3 closest to query
        float[] query = new float[]{0.9f, 0.1f, 0.5f};
        List<Map.Entry<Bytes, Float>> results = store.search(query, 3);

        assertEquals(3, results.size());

        // Results should be sorted by score descending
        for (int i = 0; i < results.size() - 1; i++) {
            assertTrue("Results should be sorted by score descending",
                    results.get(i).getValue() >= results.get(i + 1).getValue());
        }
    }

    @Test
    public void testRemoveVector() {
        AbstractVectorStore store = createStore();

        Bytes pk1 = Bytes.from_int(1);
        Bytes pk2 = Bytes.from_int(2);
        Bytes pk3 = Bytes.from_int(3);

        store.addVector(pk1, new float[]{1.0f, 0.0f, 0.0f});
        store.addVector(pk2, new float[]{0.0f, 1.0f, 0.0f});
        store.addVector(pk3, new float[]{0.0f, 0.0f, 1.0f});

        assertEquals(3, store.size());

        store.removeVector(pk2);
        assertEquals(2, store.size());

        // Search should not return removed vector
        List<Map.Entry<Bytes, Float>> results = store.search(new float[]{0.0f, 1.0f, 0.0f}, 10);
        assertEquals(2, results.size());
        for (Map.Entry<Bytes, Float> entry : results) {
            assertNotEquals("Removed vector should not appear in results", pk2, entry.getKey());
        }
    }

    @Test
    public void testEstimatedMemoryUsageBytes() {
        AbstractVectorStore store = createStore();

        assertEquals(0, store.estimatedMemoryUsageBytes());

        store.addVector(Bytes.from_int(1), new float[]{1.0f, 2.0f, 3.0f});
        store.addVector(Bytes.from_int(2), new float[]{4.0f, 5.0f, 6.0f});

        long memUsage = store.estimatedMemoryUsageBytes();
        assertTrue("Memory usage should be > 0 after adding vectors", memUsage > 0);
    }

    @Test
    public void testSearchOnEmptyStore() {
        AbstractVectorStore store = createStore();
        List<Map.Entry<Bytes, Float>> results = store.search(new float[]{1.0f, 0.0f}, 5);
        assertTrue("Search on empty store should return empty list", results.isEmpty());
    }

    @Test
    public void testSearchTopKGreaterThanSize() {
        AbstractVectorStore store = createStore();

        store.addVector(Bytes.from_int(1), new float[]{1.0f, 0.0f});
        store.addVector(Bytes.from_int(2), new float[]{0.0f, 1.0f});

        List<Map.Entry<Bytes, Float>> results = store.search(new float[]{1.0f, 0.0f}, 100);
        assertEquals("Should return all results when topK > size", 2, results.size());
    }

    @Test
    public void testRemoveNonExistentKey() {
        AbstractVectorStore store = createStore();
        store.addVector(Bytes.from_int(1), new float[]{1.0f, 0.0f});

        // Should not throw
        store.removeVector(Bytes.from_int(999));
        assertEquals(1, store.size());
    }

    @Test
    public void testStartAndClose() throws Exception {
        AbstractVectorStore store = createStore();

        // start() and close() should not throw
        store.start();

        store.addVector(Bytes.from_int(1), new float[]{1.0f, 0.0f});
        assertEquals(1, store.size());

        store.close();
    }

    @Test
    public void testGetVectorColumnName() {
        AbstractVectorStore store = createStore();
        assertEquals("embedding", store.getVectorColumnName());
    }
}
