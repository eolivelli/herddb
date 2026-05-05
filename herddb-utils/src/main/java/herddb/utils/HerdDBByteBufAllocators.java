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

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.netty.util.internal.PlatformDependent;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Centralised access to dedicated Netty {@link PooledByteBufAllocator} instances
 * used by HerdDB for data-page and index-page memory.
 *
 * <p>Two singleton allocators are exposed:
 * <ul>
 *   <li>{@link #dataPagesAllocator()} — for data-page payload buffers
 *       (e.g. {@code Record.value} slabs).</li>
 *   <li>{@link #indexPagesAllocator()} — for index-page key/value slabs
 *       (BLink and BRIN nodes).</li>
 * </ul>
 *
 * <p>Both are independent from {@link PooledByteBufAllocator#DEFAULT}, which
 * stays reserved for transient network/PDU buffers and the commit-log codec.
 * Keeping page memory in dedicated arenas isolates allocation profiles
 * (long-lived page slabs vs. short-lived network frames), avoids arena
 * fragmentation, and lets operators tune each pool independently via the
 * system properties listed below.
 *
 * <p>System properties (defaults follow Netty's {@code DEFAULT}):
 * <ul>
 *   <li>{@code herddb.allocator.data.numHeapArenas}</li>
 *   <li>{@code herddb.allocator.data.numDirectArenas}</li>
 *   <li>{@code herddb.allocator.data.pageSize}</li>
 *   <li>{@code herddb.allocator.data.maxOrder}</li>
 *   <li>{@code herddb.allocator.data.smallCacheSize}</li>
 *   <li>{@code herddb.allocator.data.normalCacheSize}</li>
 *   <li>{@code herddb.allocator.data.useCacheForAllThreads}</li>
 *   <li>same set under {@code herddb.allocator.index.*}</li>
 * </ul>
 *
 * <h3>Direct-memory budget</h3>
 * <p>Page memory now lives off-heap, so HerdDB's data/index/PK budgets must be
 * derived from the JVM's direct-memory limit, not from the JVM heap size.
 * {@link #maxDirectMemoryBytes()} returns Netty's effective direct-memory limit
 * for that purpose, with documented fallbacks when the limit cannot be observed.
 */
public final class HerdDBByteBufAllocators {

    private static final Logger LOGGER = Logger.getLogger(HerdDBByteBufAllocators.class.getName());

    private static final String PROP_PREFIX_DATA = "herddb.allocator.data.";
    private static final String PROP_PREFIX_INDEX = "herddb.allocator.index.";

    private static final PooledByteBufAllocator DATA_PAGES = build(PROP_PREFIX_DATA);
    private static final PooledByteBufAllocator INDEX_PAGES = build(PROP_PREFIX_INDEX);

    /**
     * Cached result of {@link #maxDirectMemoryBytes()}. Resolved lazily on first
     * call; never updates afterwards because the JVM's direct-memory limit is
     * fixed at startup.
     */
    private static volatile long cachedMaxDirectMemoryBytes = -1L;

    private HerdDBByteBufAllocators() {
    }

    /**
     * Dedicated allocator for data-page payload buffers (e.g. record-value
     * slabs held by an in-memory data page).
     *
     * @return a singleton {@link PooledByteBufAllocator} distinct from
     *         {@link PooledByteBufAllocator#DEFAULT}.
     */
    public static PooledByteBufAllocator dataPagesAllocator() {
        return DATA_PAGES;
    }

    /**
     * Dedicated allocator for index-page slabs (BLink / BRIN node key/value
     * storage).
     *
     * @return a singleton {@link PooledByteBufAllocator} distinct from
     *         {@link #dataPagesAllocator()} and {@link PooledByteBufAllocator#DEFAULT}.
     */
    public static PooledByteBufAllocator indexPagesAllocator() {
        return INDEX_PAGES;
    }

    /**
     * Metric snapshot for the data-pages pool.
     */
    public static PooledByteBufAllocatorMetric dataPagesMetric() {
        return DATA_PAGES.metric();
    }

    /**
     * Metric snapshot for the index-pages pool.
     */
    public static PooledByteBufAllocatorMetric indexPagesMetric() {
        return INDEX_PAGES.metric();
    }

    /**
     * Returns the JVM's effective direct-memory limit in bytes, used by
     * {@code ServerConfiguration} as the reference for default data/index/PK
     * memory budgets when no explicit {@code server.memory.max.limit} is set.
     *
     * <p>Resolution order:
     * <ol>
     *   <li>{@link PlatformDependent#maxDirectMemory()} when it returns a
     *       positive value (the documented Netty path; this also accounts for
     *       {@code -Dio.netty.maxDirectMemory}).</li>
     *   <li>Reflective fallback to {@code sun.misc.VM.maxDirectMemory()} (or
     *       its {@code jdk.internal.misc.VM} equivalent) for JVMs where
     *       {@code PlatformDependent} reports a non-positive value.</li>
     *   <li>{@link Runtime#maxMemory()} as a last-resort safety net,
     *       accompanied by a {@code WARNING} log so operators can spot the
     *       fallback.</li>
     * </ol>
     *
     * <p>Cached on first call.
     */
    public static long maxDirectMemoryBytes() {
        long cached = cachedMaxDirectMemoryBytes;
        if (cached > 0L) {
            return cached;
        }
        long resolved = resolveMaxDirectMemoryBytes();
        cachedMaxDirectMemoryBytes = resolved;
        return resolved;
    }

    /**
     * Test-only hook to force re-resolution on the next call to
     * {@link #maxDirectMemoryBytes()}.
     */
    static void resetMaxDirectMemoryCacheForTesting() {
        cachedMaxDirectMemoryBytes = -1L;
    }

    private static long resolveMaxDirectMemoryBytes() {
        long fromNetty;
        try {
            fromNetty = PlatformDependent.maxDirectMemory();
        } catch (UnsupportedOperationException e) {
            // PlatformDependent throws on JVMs where the limit cannot be observed
            // (e.g. some hardened runtimes). Fall through to the reflective path.
            fromNetty = -1L;
        }
        if (fromNetty > 0L) {
            return fromNetty;
        }
        long fromVm = reflectVmMaxDirectMemory();
        if (fromVm > 0L) {
            return fromVm;
        }
        long fallback = Runtime.getRuntime().maxMemory();
        LOGGER.log(Level.WARNING,
                "Could not determine -XX:MaxDirectMemorySize via Netty PlatformDependent or"
                        + " sun.misc.VM; falling back to Runtime.maxMemory()={0} bytes for"
                        + " HerdDB memory-budget defaults. Set -XX:MaxDirectMemorySize=<bytes>"
                        + " explicitly for predictable budgets.",
                fallback);
        return fallback;
    }

    private static long reflectVmMaxDirectMemory() {
        for (String className : new String[]{"sun.misc.VM", "jdk.internal.misc.VM"}) {
            try {
                Class<?> vmClass = Class.forName(className);
                Method m = vmClass.getMethod("maxDirectMemory");
                Object value = m.invoke(null);
                if (value instanceof Long) {
                    return (Long) value;
                }
            } catch (ClassNotFoundException | NoSuchMethodException ignored) {
                // try next candidate
            } catch (ReflectiveOperationException e) {
                LOGGER.log(Level.FINE, "Reflective access to {0}.maxDirectMemory failed: {1}",
                        new Object[]{className, e.toString()});
            }
        }
        return -1L;
    }

    private static PooledByteBufAllocator build(String prefix) {
        boolean preferDirect = PooledByteBufAllocator.defaultPreferDirect();
        int numHeapArenas = SystemProperties.getIntSystemProperty(
                prefix + "numHeapArenas", PooledByteBufAllocator.defaultNumHeapArena());
        int numDirectArenas = SystemProperties.getIntSystemProperty(
                prefix + "numDirectArenas", PooledByteBufAllocator.defaultNumDirectArena());
        int pageSize = SystemProperties.getIntSystemProperty(
                prefix + "pageSize", PooledByteBufAllocator.defaultPageSize());
        int maxOrder = SystemProperties.getIntSystemProperty(
                prefix + "maxOrder", PooledByteBufAllocator.defaultMaxOrder());
        int smallCacheSize = SystemProperties.getIntSystemProperty(
                prefix + "smallCacheSize", PooledByteBufAllocator.defaultSmallCacheSize());
        int normalCacheSize = SystemProperties.getIntSystemProperty(
                prefix + "normalCacheSize", PooledByteBufAllocator.defaultNormalCacheSize());
        boolean useCacheForAllThreads = SystemProperties.getBooleanSystemProperty(
                prefix + "useCacheForAllThreads", PooledByteBufAllocator.defaultUseCacheForAllThreads());
        return new PooledByteBufAllocator(
                preferDirect,
                numHeapArenas,
                numDirectArenas,
                pageSize,
                maxOrder,
                smallCacheSize,
                normalCacheSize,
                useCacheForAllThreads);
    }
}
