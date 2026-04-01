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

/**
 * In-memory brute-force vector store used by the IndexingServiceEngine.
 * Stores vectors keyed by primary key and supports cosine similarity search.
 *
 * @author enrico.olivelli
 */
class InMemoryVectorStore extends AbstractVectorStore {

    private final List<VectorEntry> entries = Collections.synchronizedList(new ArrayList<>());

    InMemoryVectorStore(String vectorColumnName) {
        super(vectorColumnName);
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
            float score = cosineSimilarity(queryVector, entry.vector);
            results.add(new AbstractMap.SimpleEntry<>(entry.pk, score));
        }
        results.sort((a, b) -> Float.compare(b.getValue(), a.getValue()));
        return results.size() <= topK ? results : results.subList(0, topK);
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

    private static class VectorEntry {
        final Bytes pk;
        final float[] vector;

        VectorEntry(Bytes pk, float[] vector) {
            this.pk = pk;
            this.vector = vector;
        }
    }
}
