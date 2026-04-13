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

import herddb.index.vector.AbstractVectorStore;
import herddb.utils.Bytes;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * In-memory brute-force vector store used by the IndexingServiceEngine.
 * Stores vectors keyed by primary key and supports similarity search.
 *
 * @author enrico.olivelli
 */
class InMemoryVectorStore extends AbstractVectorStore {

    enum SimilarityType { COSINE, EUCLIDEAN, DOT }

    private final List<VectorEntry> entries = Collections.synchronizedList(new ArrayList<>());
    private final SimilarityType similarityType;

    InMemoryVectorStore(String vectorColumnName) {
        this(vectorColumnName, SimilarityType.COSINE);
    }

    InMemoryVectorStore(String vectorColumnName, SimilarityType similarityType) {
        super(vectorColumnName);
        this.similarityType = similarityType;
    }

    SimilarityType getSimilarityType() {
        return similarityType;
    }

    static SimilarityType parseSimilarityType(String similarity) {
        if (similarity == null) {
            return SimilarityType.COSINE;
        }
        switch (similarity.toLowerCase()) {
            case "euclidean":
                return SimilarityType.EUCLIDEAN;
            case "dot":
                return SimilarityType.DOT;
            case "cosine":
            default:
                return SimilarityType.COSINE;
        }
    }

    @Override
    public void addVector(Bytes pk, float[] vector) {
        entries.add(new VectorEntry(pk, vector));
    }

    @Override
    public void removeVector(Bytes pk) {
        entries.removeIf(e -> e.pk.equals(pk));
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public List<Map.Entry<Bytes, Float>> search(float[] queryVector, int topK) {
        List<Map.Entry<Bytes, Float>> results = new ArrayList<>();
        // Take a snapshot to avoid ConcurrentModificationException
        List<VectorEntry> snapshot;
        synchronized (entries) {
            snapshot = new ArrayList<>(entries);
        }
        for (VectorEntry entry : snapshot) {
            float score = computeSimilarity(queryVector, entry.vector);
            results.add(new AbstractMap.SimpleEntry<>(entry.pk, score));
        }
        if (similarityType == SimilarityType.EUCLIDEAN) {
            // Lower distance = more similar
            results.sort((a, b) -> Float.compare(a.getValue(), b.getValue()));
        } else {
            // Higher score = more similar (cosine, dot)
            results.sort((a, b) -> Float.compare(b.getValue(), a.getValue()));
        }
        return results.size() <= topK ? results : results.subList(0, topK);
    }

    @Override
    public void forEachPrimaryKey(boolean includeOnDisk, Predicate<Bytes> visitor) {
        List<VectorEntry> snapshot;
        synchronized (entries) {
            snapshot = new ArrayList<>(entries);
        }
        for (VectorEntry entry : snapshot) {
            if (!visitor.test(entry.pk)) {
                return;
            }
        }
    }

    @Override
    public long estimatedMemoryUsageBytes() {
        long total = 0;
        List<VectorEntry> snapshot;
        synchronized (entries) {
            snapshot = new ArrayList<>(entries);
        }
        for (VectorEntry entry : snapshot) {
            total += entry.pk.to_array().length + (long) entry.vector.length * 4;
        }
        return total;
    }

    @Override
    public void start() throws Exception {
        // no-op for in-memory store
    }

    @Override
    public void close() throws Exception {
        entries.clear();
    }

    private float computeSimilarity(float[] a, float[] b) {
        switch (similarityType) {
            case EUCLIDEAN:
                return euclideanDistance(a, b);
            case DOT:
                return dotProduct(a, b);
            case COSINE:
            default:
                return cosineSimilarity(a, b);
        }
    }

    private static float cosineSimilarity(float[] a, float[] b) {
        float dot = 0, normA = 0, normB = 0;
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        float denom = (float) (Math.sqrt(normA) * Math.sqrt(normB));
        return denom == 0 ? 0 : dot / denom;
    }

    private static float euclideanDistance(float[] a, float[] b) {
        float sum = 0;
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }
        return (float) Math.sqrt(sum);
    }

    private static float dotProduct(float[] a, float[] b) {
        float dot = 0;
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            dot += a[i] * b[i];
        }
        return dot;
    }

    private static class VectorEntry {
        final Bytes pk;
        final float[] vector;

        VectorEntry(Bytes pk, float[] vector) {
            this.pk = pk;
            this.vector = vector;
        }
    }
}
