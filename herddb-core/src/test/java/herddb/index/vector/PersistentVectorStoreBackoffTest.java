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
 * Unit tests for {@link PersistentVectorStore#computeBackoffMs(long, long, long)}.
 * The backoff policy drives how long the compaction loop sleeps between
 * retries of a failing Phase B, and needs to be pure/deterministic so it can
 * be reasoned about without spinning up a full store.
 */
public class PersistentVectorStoreBackoffTest {

    private static final long MAX = 30L * 60 * 1000; // 30 min
    private static final long BASE = 60_000L;        // 60 s

    @Test
    public void zeroFailuresYieldsZeroBackoff() {
        assertEquals(0L, PersistentVectorStore.computeBackoffMs(BASE, 0, MAX));
    }

    @Test
    public void backoffDoublesPerFailure() {
        assertEquals(BASE, PersistentVectorStore.computeBackoffMs(BASE, 1, MAX));
        assertEquals(BASE * 2, PersistentVectorStore.computeBackoffMs(BASE, 2, MAX));
        assertEquals(BASE * 4, PersistentVectorStore.computeBackoffMs(BASE, 3, MAX));
        assertEquals(BASE * 8, PersistentVectorStore.computeBackoffMs(BASE, 4, MAX));
    }

    @Test
    public void backoffCapsAtMax() {
        // After enough failures the exponential growth would exceed 30 min;
        // the function must saturate at MAX and never regress.
        long last = 0L;
        for (int f = 1; f <= 50; f++) {
            long back = PersistentVectorStore.computeBackoffMs(BASE, f, MAX);
            assertTrue("backoff must be monotonic at failure " + f
                            + " (was " + last + " now " + back + ")",
                    back >= last);
            assertTrue("backoff must not exceed cap at failure " + f
                            + " (was " + back + ")",
                    back <= MAX);
            last = back;
        }
        assertEquals("should eventually saturate at MAX", MAX, last);
    }

    @Test
    public void shortIntervalIsFloored() {
        // compactionIntervalMs = 0 (the default in the test suite) used to
        // make the compaction thread spin; the floor inside computeBackoffMs
        // guarantees a sensible minimum after a failure.
        long back = PersistentVectorStore.computeBackoffMs(0L, 1, MAX);
        assertTrue("even with zero base, first-failure backoff is non-trivial (was " + back + ")",
                back >= 60_000L);
    }

    @Test
    public void largeFailureCountDoesNotOverflow() {
        // Regression guard: shifting by a huge number could overflow long.
        long back = PersistentVectorStore.computeBackoffMs(BASE, Long.MAX_VALUE, MAX);
        assertEquals(MAX, back);
    }
}
