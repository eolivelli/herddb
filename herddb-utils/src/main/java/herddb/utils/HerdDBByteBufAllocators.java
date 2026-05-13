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
import java.util.function.LongSupplier;
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

    /**
     * Initialization-on-Demand Holder for the data-pages pool. A bad system
     * property surfaces as a clean {@link RuntimeException} from the
     * {@link #dataPagesAllocator()} accessor instead of a
     * {@link NoClassDefFoundError} at first use of any other constant in this
     * class.
     */
    private static final class DataPagesHolder {
        static final PooledByteBufAllocator INSTANCE = build(PROP_PREFIX_DATA);
    }

    /** Initialization-on-Demand Holder for the index-pages pool. */
    private static final class IndexPagesHolder {
        static final PooledByteBufAllocator INSTANCE = build(PROP_PREFIX_INDEX);
    }

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
        return DataPagesHolder.INSTANCE;
    }

    /**
     * Dedicated allocator for index-page slabs (BLink / BRIN node key/value
     * storage).
     *
     * @return a singleton {@link PooledByteBufAllocator} distinct from
     *         {@link #dataPagesAllocator()} and {@link PooledByteBufAllocator#DEFAULT}.
     */
    public static PooledByteBufAllocator indexPagesAllocator() {
        return IndexPagesHolder.INSTANCE;
    }

    /**
     * Metric snapshot for the data-pages pool.
     */
    public static PooledByteBufAllocatorMetric dataPagesMetric() {
        return DataPagesHolder.INSTANCE.metric();
    }

    /**
     * Metric snapshot for the index-pages pool.
     */
    public static PooledByteBufAllocatorMetric indexPagesMetric() {
        return IndexPagesHolder.INSTANCE.metric();
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
        long resolved = resolveMaxDirectMemoryBytes(PlatformDependentMaxDirectMemoryProbe.INSTANCE);
        cachedMaxDirectMemoryBytes = resolved;
        return resolved;
    }

    /**
     * Test-only hook to force re-resolution on the next call to
     * {@link #maxDirectMemoryBytes()}. Made public so tests in other packages
     * (e.g. {@code herddb.indexing}) can clear the lazy cache after injecting
     * a custom probe or changing JVM state between test methods.
     */
    public static void resetMaxDirectMemoryCacheForTesting() {
        cachedMaxDirectMemoryBytes = -1L;
    }

    /**
     * Package-private resolver that lets tests inject a probe other than
     * Netty's {@link PlatformDependent#maxDirectMemory()} so the reflective /
     * {@link Runtime#maxMemory()} fallback branches are reachable.
     */
    static long resolveMaxDirectMemoryBytes(LongSupplier nettyProbe) {
        long fromNetty;
        try {
            fromNetty = nettyProbe.getAsLong();
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
                Long.toString(fallback));
        return fallback;
    }

    /**
     * Returns a positive long when {@code className.maxDirectMemory()} is
     * reachable, or {@code -1L} otherwise. JDK 17+ may throw
     * {@link InaccessibleObjectException} (a {@link RuntimeException}) when
     * {@code java.base/jdk.internal.misc} is not opened to the unnamed module,
     * and a hardened JVM may install a {@link SecurityManager} that throws
     * {@link SecurityException} on reflective access; both are caught so the
     * resolver falls through to {@link Runtime#maxMemory()} instead of
     * crashing server boot. The broad catch is intentional and limited to a
     * tiny diagnostic helper.
     */
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
            } catch (RuntimeException e) {
                // Broad catch required to handle InaccessibleObjectException
                // (JDK 17+ module encapsulation) and SecurityException on
                // hardened runtimes — both are RuntimeException subtypes that
                // are not part of ReflectiveOperationException's hierarchy.
                LOGGER.log(Level.FINE, "Reflective access to {0}.maxDirectMemory blocked: {1}",
                        new Object[]{className, e.toString()});
            }
        }
        return -1L;
    }

    /**
     * Default {@link LongSupplier} that delegates to
     * {@link PlatformDependent#maxDirectMemory()}. Extracted as a singleton so
     * the resolver lambda allocation only happens at static-init time.
     */
    private enum PlatformDependentMaxDirectMemoryProbe implements LongSupplier {
        INSTANCE;

        @Override
        public long getAsLong() {
            return PlatformDependent.maxDirectMemory();
        }
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
