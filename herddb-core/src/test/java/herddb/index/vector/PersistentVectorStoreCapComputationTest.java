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

package herddb.index.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Unit tests for {@link PersistentVectorStore#computeLiveVectorCapDuringCheckpoint}.
 *
 * The method is static and package-protected so it can be called directly without
 * constructing a full store instance.
 */
public class PersistentVectorStoreCapComputationTest {

    /** Default value of MAX_LIVE_BYTES_DURING_CHECKPOINT (4 GB). */
    private static final long MAX_LIVE_BYTES = 4L * 1024 * 1024 * 1024;

    // -------------------------------------------------------------------------
    // Budget-based (headroom) path
    // -------------------------------------------------------------------------

    @Test
    public void testCapWithBudget_normalHeadroom() {
        // budget=100 MB, frozenCount=1000, dim=128, mult=5.0, minShard=10000
        // estimatedBytesPerVector = 128 * 4 * 5.0 = 2560
        // frozenEstimated         = 1000 * 2560   = 2_560_000
        // headroom                = 100_000_000 - 2_560_000 = 97_440_000
        // cap from headroom       = 97_440_000 / 2560 = 38062
        // result                  = max(38062, 10000) = 38062
        long budget = 100_000_000L;
        int frozenCount = 1_000;
        int dim = 128;
        double mult = 5.0;
        int minShard = 10_000;

        long estimatedBytesPerVector = (long) (dim * Float.BYTES * mult);
        long frozenEstimated = frozenCount * estimatedBytesPerVector;
        long headroom = budget - frozenEstimated;
        int expectedCap = (int) (headroom / estimatedBytesPerVector);

        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                frozenCount, dim, budget, mult, minShard);

        assertEquals(expectedCap, result);
        assertTrue("cap must be >= minShard", result >= minShard);
    }

    @Test
    public void testCapWithBudget_frozenConsumesAlmostAllBudget() {
        // frozen shards already exceed the budget → headroom = 0 → cap = 0 → floor applied
        long budget = 10_000_000L;
        int frozenCount = 10_000;   // 10000 * 128*4*5 = 25_600_000 > budget
        int dim = 128;
        double mult = 5.0;
        int minShard = 5_000;

        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                frozenCount, dim, budget, mult, minShard);

        // headroom <= 0, raw cap = 0 → minimum floor kicks in
        assertEquals(minShard, result);
    }

    @Test
    public void testCapMinimumFloor() {
        // Extremely tight budget: headroom rounds to 0 vectors → must return minShardSize
        long budget = 1L; // 1 byte
        int frozenCount = 0;
        int dim = 128;
        double mult = 5.0;
        int minShard = 12_345;

        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                frozenCount, dim, budget, mult, minShard);

        assertEquals(minShard, result);
    }

    @Test
    public void testCapWithBudget_frozenZero() {
        // No frozen vectors: entire budget available for live
        // cap = budget / estimatedBytesPerVector
        long budget = 50_000_000L;
        int frozenCount = 0;
        int dim = 64;
        double mult = 5.0;
        int minShard = 1_000;

        long estimatedBytesPerVector = (long) (dim * Float.BYTES * mult);
        int expectedCap = (int) (budget / estimatedBytesPerVector);

        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                frozenCount, dim, budget, mult, minShard);

        assertEquals(expectedCap, result);
    }

    @Test
    public void testCapWithBudget_exactlyFull() {
        // frozenEstimated == budget → headroom = 0 → floor
        int dim = 128;
        double mult = 5.0;
        long estimatedBytesPerVector = (long) (dim * Float.BYTES * mult); // 2560
        int frozenCount = 100;
        long budget = frozenCount * estimatedBytesPerVector; // exactly full
        int minShard = 8_000;

        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                frozenCount, dim, budget, mult, minShard);

        assertEquals(minShard, result);
    }

    // -------------------------------------------------------------------------
    // Fallback path (no budget)
    // -------------------------------------------------------------------------

    @Test
    public void testCapFallbackNoBudget() {
        // effectiveBudget == Long.MAX_VALUE → falls back to raw-byte static limit
        // rawBytesPerVector = dim*4 + 64
        int dim = 128;
        double mult = 5.0;
        int minShard = 10_000;

        long rawBytesPerVector = (long) dim * Float.BYTES + 64; // 576
        int expectedCap = (int) (MAX_LIVE_BYTES / rawBytesPerVector);

        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                0, dim, Long.MAX_VALUE, mult, minShard);

        assertEquals(expectedCap, result);
    }

    @Test
    public void testCapFallbackNoBudget_higherDim() {
        // Same fallback, larger dim → smaller cap
        int dim = 1536;
        double mult = 5.0;
        int minShard = 10_000;

        long rawBytesPerVector = (long) dim * Float.BYTES + 64;
        int expectedCap = (int) (MAX_LIVE_BYTES / rawBytesPerVector);

        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                0, dim, Long.MAX_VALUE, mult, minShard);

        assertEquals(expectedCap, result);
    }

    // -------------------------------------------------------------------------
    // Guard against arithmetic edge cases
    // -------------------------------------------------------------------------

    @Test
    public void testCapDim0NoBudget_noDivisionByZero() {
        // dim=0, no budget: rawBytesPerVector clamped to max(1, 0+64) = 64
        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                0, 0, Long.MAX_VALUE, 5.0, 1_000);

        // Should not throw; result = MAX_LIVE_BYTES / 64
        int expected = (int) (MAX_LIVE_BYTES / 64L);
        assertEquals(expected, result);
    }

    @Test
    public void testCapDim0WithBudget_noDivisionByZero() {
        // dim=0 with budget: estimatedBytesPerVector clamped to max(1, 0) = 1
        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                0, 0, 1_000_000L, 5.0, 1_000);

        // Should not throw; cap = min(MAX_INT, 1_000_000 / 1) = 1_000_000
        assertTrue("cap must be >= minShard", result >= 1_000);
    }
}
