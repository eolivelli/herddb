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

import herddb.utils.Bytes;

import java.util.List;
import java.util.Map;

import org.junit.Test;

/**
 * Tests that InMemoryVectorStore respects the configured similarity function.
 */
public class InMemoryVectorStoreSimilarityTest {

    @Test
    public void testCosineIsDefault() {
        InMemoryVectorStore store = new InMemoryVectorStore("vec");
        assertEquals(InMemoryVectorStore.SimilarityType.COSINE, store.getSimilarityType());
    }

    @Test
    public void testParseSimilarityType() {
        assertEquals(InMemoryVectorStore.SimilarityType.COSINE,
                InMemoryVectorStore.parseSimilarityType(null));
        assertEquals(InMemoryVectorStore.SimilarityType.COSINE,
                InMemoryVectorStore.parseSimilarityType("cosine"));
        assertEquals(InMemoryVectorStore.SimilarityType.COSINE,
                InMemoryVectorStore.parseSimilarityType("COSINE"));
        assertEquals(InMemoryVectorStore.SimilarityType.EUCLIDEAN,
                InMemoryVectorStore.parseSimilarityType("euclidean"));
        assertEquals(InMemoryVectorStore.SimilarityType.EUCLIDEAN,
                InMemoryVectorStore.parseSimilarityType("Euclidean"));
        assertEquals(InMemoryVectorStore.SimilarityType.DOT,
                InMemoryVectorStore.parseSimilarityType("dot"));
    }

    @Test
    public void testEuclideanProducesDifferentRankingThanCosine() throws Exception {
        // Two vectors with different magnitudes but same direction should rank
        // differently under cosine (direction-only) vs euclidean (distance-based).

        // Query vector
        float[] query = {1.0f, 0.0f};

        // Vector A: same direction as query, magnitude 1
        float[] vecA = {1.0f, 0.0f};
        // Vector B: slightly off direction, magnitude 1
        float[] vecB = {0.9f, 0.4f};
        // Vector C: same direction as query, magnitude 10 (far away in euclidean space)
        float[] vecC = {10.0f, 0.0f};

        Bytes keyA = Bytes.from_int(1);
        Bytes keyB = Bytes.from_int(2);
        Bytes keyC = Bytes.from_int(3);

        // With cosine: A and C are equally similar (same direction), B is less similar
        InMemoryVectorStore cosineStore = new InMemoryVectorStore("vec", InMemoryVectorStore.SimilarityType.COSINE);
        cosineStore.addVector(keyA, vecA);
        cosineStore.addVector(keyB, vecB);
        cosineStore.addVector(keyC, vecC);

        List<Map.Entry<Bytes, Float>> cosineResults = cosineStore.search(query, 3);
        // Cosine: A and C have score ~1.0 (same direction), B has lower score
        Bytes cosineThird = cosineResults.get(2).getKey();
        // A and C should both rank above B
        assertEquals(keyB, cosineThird);

        // With euclidean: A is closest (distance 0), C is farthest (distance 9)
        InMemoryVectorStore euclideanStore = new InMemoryVectorStore("vec", InMemoryVectorStore.SimilarityType.EUCLIDEAN);
        euclideanStore.addVector(keyA, vecA);
        euclideanStore.addVector(keyB, vecB);
        euclideanStore.addVector(keyC, vecC);

        List<Map.Entry<Bytes, Float>> euclideanResults = euclideanStore.search(query, 3);
        // Euclidean: A (distance 0) should be first, C (distance 9) should be last
        assertEquals(keyA, euclideanResults.get(0).getKey());
        assertEquals(keyC, euclideanResults.get(2).getKey());

        // The key difference: cosine ranks C near the top (same direction),
        // but euclidean ranks C last (far away)
        assertNotEquals("Euclidean and cosine should rank differently",
                cosineThird, euclideanResults.get(2).getKey());
    }

    @Test
    public void testDotProductSimilarity() throws Exception {
        float[] query = {1.0f, 0.0f};
        float[] vecA = {1.0f, 0.0f};
        float[] vecB = {10.0f, 0.0f};

        Bytes keyA = Bytes.from_int(1);
        Bytes keyB = Bytes.from_int(2);

        InMemoryVectorStore store = new InMemoryVectorStore("vec", InMemoryVectorStore.SimilarityType.DOT);
        store.addVector(keyA, vecA);
        store.addVector(keyB, vecB);

        List<Map.Entry<Bytes, Float>> results = store.search(query, 2);
        // Dot product: B (dot=10) should rank above A (dot=1)
        assertEquals(keyB, results.get(0).getKey());
        assertEquals(keyA, results.get(1).getKey());
    }
}
