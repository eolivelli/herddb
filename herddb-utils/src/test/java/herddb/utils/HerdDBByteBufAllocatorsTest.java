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

package herddb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import java.util.function.LongSupplier;
import org.junit.Test;

public class HerdDBByteBufAllocatorsTest {

    @Test
    public void dataAndIndexAllocatorsAreNonNullAndPooled() {
        assertNotNull(HerdDBByteBufAllocators.dataPagesAllocator());
        assertNotNull(HerdDBByteBufAllocators.indexPagesAllocator());
        // PooledByteBufAllocator instances expose metric().
        assertNotNull(HerdDBByteBufAllocators.dataPagesMetric());
        assertNotNull(HerdDBByteBufAllocators.indexPagesMetric());
    }

    @Test
    public void allocatorsAreDistinctAmongstThemselvesAndFromDefault() {
        PooledByteBufAllocator data = HerdDBByteBufAllocators.dataPagesAllocator();
        PooledByteBufAllocator index = HerdDBByteBufAllocators.indexPagesAllocator();
        assertNotSame("data pages pool must not be the Netty default", PooledByteBufAllocator.DEFAULT, data);
        assertNotSame("index pages pool must not be the Netty default", PooledByteBufAllocator.DEFAULT, index);
        assertNotSame("data pages pool must be distinct from the index pages pool", data, index);
    }

    @Test
    public void singletonAccessorsReturnTheSameInstance() {
        assertSame(HerdDBByteBufAllocators.dataPagesAllocator(), HerdDBByteBufAllocators.dataPagesAllocator());
        assertSame(HerdDBByteBufAllocators.indexPagesAllocator(), HerdDBByteBufAllocators.indexPagesAllocator());
    }

    @Test
    public void allocationsRouteThroughTheDedicatedPools() {
        PooledByteBufAllocator data = HerdDBByteBufAllocators.dataPagesAllocator();
        PooledByteBufAllocator index = HerdDBByteBufAllocators.indexPagesAllocator();
        ByteBuf dataBuf = data.directBuffer(512);
        try {
            assertSame("buffer's allocator must match the data-pages pool", data, dataBuf.alloc());
        } finally {
            dataBuf.release();
        }
        ByteBuf indexBuf = index.directBuffer(256);
        try {
            assertSame("buffer's allocator must match the index-pages pool", index, indexBuf.alloc());
        } finally {
            indexBuf.release();
        }
    }

    @Test
    public void metricsTrackAllocations() {
        PooledByteBufAllocatorMetric beforeData = HerdDBByteBufAllocators.dataPagesMetric();
        long heapBefore = beforeData.usedHeapMemory();
        long directBefore = beforeData.usedDirectMemory();
        ByteBuf buf = HerdDBByteBufAllocators.dataPagesAllocator().directBuffer(8192);
        try {
            PooledByteBufAllocatorMetric afterData = HerdDBByteBufAllocators.dataPagesMetric();
            // Either the heap or the direct counter must have moved (or both —
            // depending on Netty's preferDirect default for this JVM).
            long heapAfter = afterData.usedHeapMemory();
            long directAfter = afterData.usedDirectMemory();
            assertTrue("at least one of the data-pool used-memory counters must increase"
                            + " (heap " + heapBefore + " -> " + heapAfter
                            + ", direct " + directBefore + " -> " + directAfter + ")",
                    (heapAfter > heapBefore) || (directAfter > directBefore));
        } finally {
            buf.release();
        }
    }

    @Test
    public void maxDirectMemoryBytesIsPositiveAndCached() {
        long first = HerdDBByteBufAllocators.maxDirectMemoryBytes();
        assertTrue("maxDirectMemoryBytes must be positive on a normal JVM, got " + first, first > 0L);
        long second = HerdDBByteBufAllocators.maxDirectMemoryBytes();
        assertEquals("repeated calls must return the cached value", first, second);
    }

    /**
     * Strong invariant the rest of issue #399's series depends on: an
     * allocation from the dedicated data-pool must NOT show up on the
     * {@link PooledByteBufAllocator#DEFAULT} metric. Without this guarantee
     * the dedicated pools would be a no-op rebrand of the default arena.
     */
    @Test
    public void allocationsBypassNettyDefaultPool() {
        PooledByteBufAllocatorMetric defaultMetric = PooledByteBufAllocator.DEFAULT.metric();
        PooledByteBufAllocatorMetric dataMetric = HerdDBByteBufAllocators.dataPagesMetric();

        long defaultDirectBefore = defaultMetric.usedDirectMemory();
        long defaultHeapBefore = defaultMetric.usedHeapMemory();
        long dataDirectBefore = dataMetric.usedDirectMemory();
        long dataHeapBefore = dataMetric.usedHeapMemory();

        // Allocate one direct and one heap buffer of distinctly different
        // sizes from the data pool so the deltas are non-zero on either path
        // regardless of the JVM's preferDirect default.
        ByteBuf direct = HerdDBByteBufAllocators.dataPagesAllocator().directBuffer(64 * 1024);
        ByteBuf heap = HerdDBByteBufAllocators.dataPagesAllocator().heapBuffer(64 * 1024);
        try {
            assertSame("direct buffer must be owned by the data pool",
                    HerdDBByteBufAllocators.dataPagesAllocator(), direct.alloc());
            assertSame("heap buffer must be owned by the data pool",
                    HerdDBByteBufAllocators.dataPagesAllocator(), heap.alloc());

            long defaultDirectAfter = defaultMetric.usedDirectMemory();
            long defaultHeapAfter = defaultMetric.usedHeapMemory();
            long dataDirectAfter = dataMetric.usedDirectMemory();
            long dataHeapAfter = dataMetric.usedHeapMemory();

            assertEquals("DEFAULT direct memory must NOT move when allocating from the data pool",
                    defaultDirectBefore, defaultDirectAfter);
            assertEquals("DEFAULT heap memory must NOT move when allocating from the data pool",
                    defaultHeapBefore, defaultHeapAfter);
            assertTrue("data pool used memory must move (direct " + dataDirectBefore + "->" + dataDirectAfter
                            + ", heap " + dataHeapBefore + "->" + dataHeapAfter + ")",
                    dataDirectAfter > dataDirectBefore || dataHeapAfter > dataHeapBefore);
        } finally {
            direct.release();
            heap.release();
        }
    }

    /**
     * Drive {@link HerdDBByteBufAllocators#resolveMaxDirectMemoryBytes(LongSupplier)}
     * with a stub Netty probe so the JDK-internal / {@link Runtime#maxMemory()}
     * fallback chain is reachable. On a normal JVM the reflective branch
     * succeeds; on hardened runtimes the {@code Runtime.maxMemory()} branch
     * fires. Either way the resolver must return a positive value.
     */
    @Test
    public void fallbackChainReturnsPositiveWhenNettyProbeFailsOrReportsZero() {
        LongSupplier nettyReportsZero = () -> 0L;
        long resolved = HerdDBByteBufAllocators.resolveMaxDirectMemoryBytes(nettyReportsZero);
        assertTrue("fallback chain must return a positive limit, got " + resolved, resolved > 0L);

        LongSupplier nettyThrows = () -> {
            throw new UnsupportedOperationException("simulated hardened runtime");
        };
        long resolvedThrowing = HerdDBByteBufAllocators.resolveMaxDirectMemoryBytes(nettyThrows);
        assertTrue("fallback chain must catch UnsupportedOperationException, got " + resolvedThrowing,
                resolvedThrowing > 0L);
    }
}
