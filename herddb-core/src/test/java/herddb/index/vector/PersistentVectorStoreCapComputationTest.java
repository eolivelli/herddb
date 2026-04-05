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
 * Unit tests for {@link PersistentVectorStore#computeLiveVectorCapDuringCheckpoint}
 * and {@link PersistentVectorStore#estimatedBytesPerVector}.
 *
 * The methods are static and package-protected so they can be called directly without
 * constructing a full store instance.
 */
public class PersistentVectorStoreCapComputationTest {

    /** Default value of MAX_LIVE_BYTES_DURING_CHECKPOINT (4 GB). */
    private static final long MAX_LIVE_BYTES = 4L * 1024 * 1024 * 1024;

    // -------------------------------------------------------------------------
    // estimatedBytesPerVector
    // -------------------------------------------------------------------------

    @Test
    public void testEstimatedBytesPerVector_basic() {
        int dim = 128;
        int m = 16;
        float neighborOverflow = 1.2f;
        int nodeArrayLen = (int) (m * neighborOverflow) + 1; // 20
        long graphBytesPerNode = 8L + 16L + 4L + 24L + 24L + (long) nodeArrayLen * 8L + 8L;
        long expected = (long) dim * 4 + 250L + graphBytesPerNode;

        long result = PersistentVectorStore.estimatedBytesPerVector(dim, m, neighborOverflow);
        assertEquals(expected, result);
    }

    // -------------------------------------------------------------------------
    // Budget-based (headroom) path
    // -------------------------------------------------------------------------

    @Test
    public void testCapWithBudget_normalHeadroom() {
        // budget=100 MB, frozenCount=1000, dim=128, m=16, no=1.2f, minShard=10000
        long budget = 100_000_000L;
        int frozenCount = 1_000;
        int dim = 128;
        int m = 16;
        float neighborOverflow = 1.2f;
        int minShard = 10_000;

        long estimatedBytesPerVector = PersistentVectorStore.estimatedBytesPerVector(dim, m, neighborOverflow);
        long frozenEstimated = frozenCount * estimatedBytesPerVector;
        long headroom = budget - frozenEstimated;
        int expectedCap = (int) (headroom / estimatedBytesPerVector);

        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                frozenCount, dim, m, neighborOverflow, budget, minShard);

        assertEquals(expectedCap, result);
        assertTrue("cap must be >= minShard", result >= minShard);
    }

    @Test
    public void testCapWithBudget_frozenConsumesAlmostAllBudget() {
        // frozen shards already exceed the budget -> headroom = 0 -> cap = 0 -> floor applied
        long budget = 10_000_000L;
        int frozenCount = 10_000;
        int dim = 128;
        int m = 16;
        float neighborOverflow = 1.2f;
        int minShard = 5_000;

        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                frozenCount, dim, m, neighborOverflow, budget, minShard);

        // headroom <= 0, raw cap = 0 -> minimum floor kicks in
        assertEquals(minShard, result);
    }

    @Test
    public void testCapMinimumFloor() {
        // Extremely tight budget: headroom rounds to 0 vectors -> must return minShardSize
        long budget = 1L; // 1 byte
        int frozenCount = 0;
        int dim = 128;
        int m = 16;
        float neighborOverflow = 1.2f;
        int minShard = 12_345;

        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                frozenCount, dim, m, neighborOverflow, budget, minShard);

        assertEquals(minShard, result);
    }

    @Test
    public void testCapWithBudget_frozenZero() {
        // No frozen vectors: entire budget available for live
        long budget = 50_000_000L;
        int frozenCount = 0;
        int dim = 64;
        int m = 16;
        float neighborOverflow = 1.2f;
        int minShard = 1_000;

        long estimatedBytesPerVector = PersistentVectorStore.estimatedBytesPerVector(dim, m, neighborOverflow);
        int expectedCap = (int) (budget / estimatedBytesPerVector);

        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                frozenCount, dim, m, neighborOverflow, budget, minShard);

        assertEquals(expectedCap, result);
    }

    @Test
    public void testCapWithBudget_exactlyFull() {
        // frozenEstimated == budget -> headroom = 0 -> floor
        int dim = 128;
        int m = 16;
        float neighborOverflow = 1.2f;
        long estimatedBytesPerVector = PersistentVectorStore.estimatedBytesPerVector(dim, m, neighborOverflow);
        int frozenCount = 100;
        long budget = frozenCount * estimatedBytesPerVector; // exactly full
        int minShard = 8_000;

        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                frozenCount, dim, m, neighborOverflow, budget, minShard);

        assertEquals(minShard, result);
    }

    // -------------------------------------------------------------------------
    // Fallback path (no budget)
    // -------------------------------------------------------------------------

    @Test
    public void testCapFallbackNoBudget() {
        // effectiveBudget == Long.MAX_VALUE -> falls back to static raw-byte static limit
        int dim = 128;
        int m = 16;
        float neighborOverflow = 1.2f;
        int minShard = 10_000;

        long estimatedBytesPerVector = PersistentVectorStore.estimatedBytesPerVector(dim, m, neighborOverflow);
        int expectedCap = (int) (MAX_LIVE_BYTES / Math.max(1L, estimatedBytesPerVector));

        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                0, dim, m, neighborOverflow, Long.MAX_VALUE, minShard);

        assertEquals(expectedCap, result);
    }

    @Test
    public void testCapFallbackNoBudget_higherDim() {
        // Same fallback, larger dim -> smaller cap
        int dim = 1536;
        int m = 16;
        float neighborOverflow = 1.2f;
        int minShard = 10_000;

        long estimatedBytesPerVector = PersistentVectorStore.estimatedBytesPerVector(dim, m, neighborOverflow);
        int expectedCap = (int) (MAX_LIVE_BYTES / Math.max(1L, estimatedBytesPerVector));

        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                0, dim, m, neighborOverflow, Long.MAX_VALUE, minShard);

        assertEquals(expectedCap, result);
    }

    // -------------------------------------------------------------------------
    // Guard against arithmetic edge cases
    // -------------------------------------------------------------------------

    @Test
    public void testCapDim0NoBudget_noDivisionByZero() {
        // dim=0, no budget: estimatedBytesPerVector still positive due to graph overhead
        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                0, 0, 16, 1.2f, Long.MAX_VALUE, 1_000);

        // Should not throw; result = MAX_LIVE_BYTES / estimatedBytesPerVector(0, 16, 1.2f)
        long bytesPerVector = PersistentVectorStore.estimatedBytesPerVector(0, 16, 1.2f);
        int expected = (int) (MAX_LIVE_BYTES / Math.max(1L, bytesPerVector));
        assertEquals(expected, result);
    }

    @Test
    public void testCapDim0WithBudget_noDivisionByZero() {
        // dim=0 with budget: estimatedBytesPerVector still positive
        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                0, 0, 16, 1.2f, 1_000_000L, 1_000);

        // Should not throw
        assertTrue("cap must be >= minShard", result >= 1_000);
    }

    // -------------------------------------------------------------------------
    // Absolute max-live-vectors override (P2.6)
    // -------------------------------------------------------------------------

    @Test
    public void absoluteCapTightens() {
        // Memory-derived cap would be large; the absolute cap must clamp it.
        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                0, 128, 16, 1.2f, Long.MAX_VALUE, 100, 1000);
        assertEquals("absolute cap must win when < memory cap", 1000, result);
    }

    @Test
    public void absoluteCapZeroIsIgnored() {
        // When absolute cap is 0, only the memory-derived cap applies.
        int withNoAbsolute = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                0, 128, 16, 1.2f, Long.MAX_VALUE, 100, 0);
        int memoryOnly = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                0, 128, 16, 1.2f, Long.MAX_VALUE, 100);
        assertEquals(memoryOnly, withNoAbsolute);
    }

    @Test
    public void absoluteCapDoesNotExceedMemoryCap() {
        // If absolute cap is higher than memory cap, memory cap wins.
        int withHigh = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                0, 128, 16, 1.2f, Long.MAX_VALUE, 100, Integer.MAX_VALUE);
        int memoryOnly = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                0, 128, 16, 1.2f, Long.MAX_VALUE, 100);
        assertEquals("larger absolute cap must not raise the effective cap",
                memoryOnly, withHigh);
    }

    @Test
    public void absoluteCapIsFlooredByMinShardSize() {
        // Absolute cap smaller than minShardSize → result floored at minShardSize.
        int result = PersistentVectorStore.computeLiveVectorCapDuringCheckpoint(
                0, 128, 16, 1.2f, Long.MAX_VALUE, 500, 10);
        assertTrue("result must be at least minShardSize", result >= 500);
    }
}
