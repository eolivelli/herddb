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

package herddb.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import herddb.utils.VectorSearchRequestContext;
import org.junit.After;
import org.junit.Test;

/**
 * Verifies the lifecycle and accumulator semantics of
 * {@link VectorSearchRequestContext}.
 */
public class VectorSearchRequestContextTest {

    @After
    public void ensureContextCleared() {
        VectorSearchRequestContext.end();
    }

    @Test
    public void currentIsNullOutsideRequest() {
        assertNull("no active context at thread start", VectorSearchRequestContext.current());
    }

    @Test
    public void beginInstallsFreshContext() {
        VectorSearchRequestContext ctx = VectorSearchRequestContext.begin();
        assertNotNull(ctx);
        assertSame(ctx, VectorSearchRequestContext.current());
        assertEquals(0L, ctx.getReadFileRangeCalls());
        assertEquals(0L, ctx.getReadFileRangeBytes());
        assertEquals(0L, ctx.getReadFileRangeWaitNanos());
        assertEquals(0L, ctx.getCacheHits());
        assertEquals(0L, ctx.getCacheMisses());
    }

    @Test
    public void endClearsThreadLocal() {
        VectorSearchRequestContext.begin();
        VectorSearchRequestContext.end();
        assertNull(VectorSearchRequestContext.current());
    }

    @Test
    public void recordReadFileRangeAccumulates() {
        VectorSearchRequestContext ctx = VectorSearchRequestContext.begin();
        ctx.recordReadFileRange(1024, 1_000_000L);
        ctx.recordReadFileRange(2048, 500_000L);
        assertEquals(2L, ctx.getReadFileRangeCalls());
        assertEquals(3072L, ctx.getReadFileRangeBytes());
        assertEquals(1_500_000L, ctx.getReadFileRangeWaitNanos());
    }

    @Test
    public void recordCacheHitsAndMissesTrackSeparately() {
        VectorSearchRequestContext ctx = VectorSearchRequestContext.begin();
        ctx.recordCacheHit();
        ctx.recordCacheHit();
        ctx.recordCacheHit();
        ctx.recordCacheMiss();
        assertEquals(3L, ctx.getCacheHits());
        assertEquals(1L, ctx.getCacheMisses());
    }

    @Test
    public void beginReturnsNewInstanceOnEachCall() {
        VectorSearchRequestContext first = VectorSearchRequestContext.begin();
        first.recordReadFileRange(16, 1L);
        VectorSearchRequestContext.end();
        VectorSearchRequestContext second = VectorSearchRequestContext.begin();
        assertNotSame(first, second);
        // The second request must start from a clean slate — counters from
        // the previous request must not leak across begin() boundaries.
        assertEquals(0L, second.getReadFileRangeCalls());
        assertEquals(0L, second.getReadFileRangeBytes());
    }

    @Test
    public void threadLocalIsolatesBetweenThreads() throws Exception {
        VectorSearchRequestContext.begin().recordReadFileRange(100, 1L);

        final VectorSearchRequestContext[] observedOnOtherThread = new VectorSearchRequestContext[1];
        Thread t = new Thread(() -> observedOnOtherThread[0] = VectorSearchRequestContext.current());
        t.start();
        t.join();

        // A second thread must not inherit this thread's context — otherwise a
        // stray reader running on a different thread would pollute our counters.
        assertNull(observedOnOtherThread[0]);
    }
}
