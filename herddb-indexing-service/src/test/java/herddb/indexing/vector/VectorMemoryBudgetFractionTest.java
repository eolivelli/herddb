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

package herddb.indexing.vector;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Pure unit test for {@link VectorMemoryBudget#budgetFraction()} (issue #646).
 */
public class VectorMemoryBudgetFractionTest {

    private static VectorMemoryBudget budget(final long usage, final long max) {
        return new VectorMemoryBudget() {
            @Override
            public long totalEstimatedMemoryUsageBytes() {
                return usage;
            }

            @Override
            public long maxMemoryBytes() {
                return max;
            }
        };
    }

    @Test
    public void zeroWhenLimitIsUnbounded() {
        assertEquals(0.0d, budget(123_456_789L, Long.MAX_VALUE).budgetFraction(), 0.0d);
    }

    @Test
    public void zeroWhenLimitIsZeroOrNegative() {
        assertEquals(0.0d, budget(123L, 0L).budgetFraction(), 0.0d);
        assertEquals(0.0d, budget(123L, -1L).budgetFraction(), 0.0d);
    }

    @Test
    public void halfWhenUsageIsHalfOfMax() {
        assertEquals(0.5d, budget(50L, 100L).budgetFraction(), 1e-12);
    }

    @Test
    public void oneWhenUsageEqualsMax() {
        assertEquals(1.0d, budget(100L, 100L).budgetFraction(), 1e-12);
    }

    @Test
    public void aboveOneWhenUsageExceedsMax() {
        // The accessor is intentionally not clamped: it can legitimately
        // exceed 1.0 during the brief window between crossing the hard
        // limit and the back-pressure path draining the live shards.
        assertEquals(2.0d, budget(200L, 100L).budgetFraction(), 1e-12);
    }
}
