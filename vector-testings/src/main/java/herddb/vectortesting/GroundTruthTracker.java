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

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Tracks the K nearest neighbors for each query vector using bounded max-heaps.
 * After all base vectors have been offered, call {@link #getGroundTruth()} to
 * retrieve the ground truth in IVECS-compatible format (nearest first).
 */
public class GroundTruthTracker {

    private final float[][] queryVectors;
    private final int k;
    private final boolean cosine;

    // Each heap entry: [0] = Float.floatToIntBits(distance) stored as int in long upper bits,
    // [1] = vectorId. We use a simple record instead.
    private static record Neighbor(float distance, int id) {}

    // Max-heap: farthest neighbor at head, so we can cheaply evict it.
    private static final Comparator<Neighbor> MAX_DISTANCE = Comparator.comparingDouble(Neighbor::distance).reversed();

    @SuppressWarnings("unchecked")
    private final PriorityQueue<Neighbor>[] heaps;

    public GroundTruthTracker(float[][] queryVectors, int k, String similarity) {
        this.queryVectors = queryVectors;
        this.k = k;
        this.cosine = "cosine".equalsIgnoreCase(similarity);
        this.heaps = new PriorityQueue[queryVectors.length];
        for (int i = 0; i < queryVectors.length; i++) {
            heaps[i] = new PriorityQueue<>(k + 1, MAX_DISTANCE);
        }
    }

    /**
     * Offer a base vector. Computes distance to every query vector and updates
     * the top-K heaps accordingly.
     */
    public void offer(int vectorId, float[] vector) {
        for (int i = 0; i < queryVectors.length; i++) {
            float dist = cosine
                    ? cosineDistance(queryVectors[i], vector)
                    : euclideanDistanceSq(queryVectors[i], vector);
            PriorityQueue<Neighbor> heap = heaps[i];
            if (heap.size() < k) {
                heap.add(new Neighbor(dist, vectorId));
            } else if (dist < heap.peek().distance) {
                heap.poll();
                heap.add(new Neighbor(dist, vectorId));
            }
        }
    }

    /**
     * Returns the ground truth as int[][] (one row per query, K columns),
     * sorted nearest-first (ascending distance).
     */
    public int[][] getGroundTruth() {
        int[][] result = new int[queryVectors.length][];
        for (int i = 0; i < queryVectors.length; i++) {
            PriorityQueue<Neighbor> heap = heaps[i];
            int size = heap.size();
            Neighbor[] sorted = heap.toArray(new Neighbor[0]);
            java.util.Arrays.sort(sorted, Comparator.comparingDouble(Neighbor::distance));
            int[] ids = new int[size];
            for (int j = 0; j < size; j++) {
                ids[j] = sorted[j].id;
            }
            result[i] = ids;
        }
        return result;
    }

    private static float euclideanDistanceSq(float[] a, float[] b) {
        float sum = 0;
        for (int i = 0; i < a.length; i++) {
            float d = a[i] - b[i];
            sum += d * d;
        }
        return sum;
    }

    private static float cosineDistance(float[] a, float[] b) {
        float dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0 || normB == 0) {
            return 1.0f;
        }
        return 1.0f - dot / (float) (Math.sqrt(normA) * Math.sqrt(normB));
    }
}
