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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

/**
 * Verifies the contract that {@link GroundTruthTracker#getGroundTruth()} is
 * non-destructive — i.e. callable multiple times during generation to emit
 * intermediate ground-truth checkpoint files without disturbing tracker state.
 */
class GroundTruthTrackerTest {

    @Test
    void getGroundTruthIsIdempotent() {
        // Two query vectors, K=2, three offered base vectors.
        float[][] queries = new float[][]{
                {0.0f, 0.0f},
                {10.0f, 10.0f}
        };
        GroundTruthTracker tracker = new GroundTruthTracker(queries, 2, "euclidean");
        // Offer the queries themselves (they are part of the base set in practice).
        tracker.offer(0, queries[0]);
        tracker.offer(1, queries[1]);
        tracker.offer(2, new float[]{1.0f, 1.0f});
        tracker.offer(3, new float[]{9.5f, 9.5f});

        int[][] firstCall = tracker.getGroundTruth();
        int[][] secondCall = tracker.getGroundTruth();

        assertEquals(firstCall.length, secondCall.length);
        for (int i = 0; i < firstCall.length; i++) {
            assertArrayEquals(firstCall[i], secondCall[i],
                    "Snapshot at row " + i + " differs between back-to-back calls");
        }

        // Adding more vectors after the snapshots should change subsequent results,
        // but not retroactively change the snapshots we already captured (defensive
        // copy contract: the int[][] handed to callers is safe to retain).
        int[] originalFirstRow = firstCall[0].clone();
        tracker.offer(4, new float[]{0.5f, 0.5f});
        assertArrayEquals(originalFirstRow, firstCall[0],
                "Earlier snapshot mutated by later offer()");
    }
}
