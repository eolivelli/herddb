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

package herddb.core.indexes;

import herddb.index.vector.RemoteVectorIndexService;
import herddb.log.LogSequenceNumber;
import herddb.utils.Bytes;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory mock of RemoteVectorIndexService for testing.
 * Stores vectors in a map keyed by (tablespace, table, index, primaryKey) and
 * implements brute-force cosine similarity search.
 */
public class MockRemoteVectorIndexService implements RemoteVectorIndexService {

    private final ConcurrentHashMap<String, List<VectorEntry>> indexes = new ConcurrentHashMap<>();

    private static String indexKey(String table, String index) {
        return table + "." + index;
    }

    public void addVector(String table, String index, Bytes pk, float[] vector) {
        String key = indexKey(table, index);
        indexes.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>()))
                .add(new VectorEntry(pk, vector));
    }

    public void removeVector(String table, String index, Bytes pk) {
        String key = indexKey(table, index);
        List<VectorEntry> entries = indexes.get(key);
        if (entries != null) {
            entries.removeIf(e -> e.pk.equals(pk));
        }
    }

    @Override
    public List<Map.Entry<Bytes, Float>> search(String tablespace, String table, String index,
                                                  float[] vector, int topK) {
        String key = indexKey(table, index);
        List<VectorEntry> entries = indexes.getOrDefault(key, Collections.emptyList());
        List<Map.Entry<Bytes, Float>> results = new ArrayList<>();
        for (VectorEntry entry : entries) {
            float score = cosineSimilarity(vector, entry.vector);
            results.add(new AbstractMap.SimpleEntry<>(entry.pk, score));
        }
        results.sort((a, b) -> Float.compare(b.getValue(), a.getValue()));
        return results.size() <= topK ? results : results.subList(0, topK);
    }

    @Override
    public IndexStatusInfo getIndexStatus(String tablespace, String table, String index) {
        String key = indexKey(table, index);
        List<VectorEntry> entries = indexes.getOrDefault(key, Collections.emptyList());
        return new IndexStatusInfo(entries.size(), 1, 0, 0, "mock");
    }

    @Override
    public void waitForCatchUp(String tablespace, LogSequenceNumber sequenceNumber) {
        // no-op
    }

    @Override
    public void close() {
        indexes.clear();
    }

    private static float cosineSimilarity(float[] a, float[] b) {
        float dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
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
