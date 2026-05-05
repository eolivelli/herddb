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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 * Issue #402: unit tests for {@link IngestRateLimiterGroup}. Cover the
 * per-child rate split, resize semantics, and out-of-range index handling.
 */
class IngestRateLimiterGroupTest {

    @Test
    void perChildRateIsTotalDividedBySize() {
        IngestRateLimiterGroup g = new IngestRateLimiterGroup(4, 1000.0);
        assertEquals(1000.0, g.getEffectiveRate(), 1e-6);
        for (int i = 0; i < 4; i++) {
            assertEquals(250.0, g.limiterFor(i).getRate(), 1e-3,
                    "child " + i + " should be totalRate / size");
        }
    }

    @Test
    void zeroTotalRateMapsAllChildrenToUnlimited() {
        IngestRateLimiterGroup g = new IngestRateLimiterGroup(4, 0.0);
        assertEquals(0.0, g.getEffectiveRate(), 1e-6);
        for (int i = 0; i < 4; i++) {
            assertEquals(PerThreadRateLimiter.UNLIMITED_RATE, g.limiterFor(i).getRate(), 1.0);
        }
    }

    @Test
    void setEffectiveRatePushesToAllChildren() {
        IngestRateLimiterGroup g = new IngestRateLimiterGroup(4, 1000.0);
        g.setEffectiveRate(2000.0);
        for (int i = 0; i < 4; i++) {
            assertEquals(500.0, g.limiterFor(i).getRate(), 1e-3);
        }
        assertEquals(2000.0, g.getEffectiveRate(), 1e-6);
    }

    @Test
    void resizeGrowsAndRebalances() {
        IngestRateLimiterGroup g = new IngestRateLimiterGroup(4, 1000.0);
        g.resize(8);
        assertEquals(8, g.size());
        for (int i = 0; i < 8; i++) {
            assertEquals(125.0, g.limiterFor(i).getRate(), 1e-3,
                    "after grow: child " + i);
        }
    }

    @Test
    void resizeShrinksAndRebalances() {
        IngestRateLimiterGroup g = new IngestRateLimiterGroup(8, 1000.0);
        g.resize(4);
        assertEquals(4, g.size());
        for (int i = 0; i < 4; i++) {
            assertEquals(250.0, g.limiterFor(i).getRate(), 1e-3,
                    "after shrink: child " + i);
        }
        assertThrows(IndexOutOfBoundsException.class, () -> g.limiterFor(4));
    }

    @Test
    void resizeRejectsZeroOrNegative() {
        IngestRateLimiterGroup g = new IngestRateLimiterGroup(4, 1000.0);
        assertThrows(IllegalArgumentException.class, () -> g.resize(0));
        assertThrows(IllegalArgumentException.class, () -> g.resize(-1));
    }

    @Test
    void acquireOnOutOfRangeIndexReturnsImmediately() throws InterruptedException {
        IngestRateLimiterGroup g = new IngestRateLimiterGroup(2, 1.0);
        long t0 = System.nanoTime();
        g.acquire(99, 100); // way out of range
        long elapsed = System.nanoTime() - t0;
        assertTrue(elapsed < 100_000_000L,
                "out-of-range acquire should return immediately, took " + (elapsed / 1e6) + " ms");
    }

    @Test
    void attachThreadOnOutOfRangeIndexIsNoOp() {
        IngestRateLimiterGroup g = new IngestRateLimiterGroup(2, 1.0);
        // Should not throw.
        g.attachThread(99, Thread.currentThread());
    }

    @Test
    void resizePreservesChildIdentity() {
        IngestRateLimiterGroup g = new IngestRateLimiterGroup(2, 1000.0);
        PerThreadRateLimiter zero = g.limiterFor(0);
        PerThreadRateLimiter one = g.limiterFor(1);
        g.resize(4);
        // Existing children must keep their identity so attached threads stay attached.
        assertTrue(zero == g.limiterFor(0), "child 0 should be the same instance after grow");
        assertTrue(one == g.limiterFor(1), "child 1 should be the same instance after grow");
    }

    @Test
    void initialSizeMustBePositive() {
        assertThrows(IllegalArgumentException.class,
                () -> new IngestRateLimiterGroup(0, 1.0));
        assertThrows(IllegalArgumentException.class,
                () -> new IngestRateLimiterGroup(-1, 1.0));
    }
}
