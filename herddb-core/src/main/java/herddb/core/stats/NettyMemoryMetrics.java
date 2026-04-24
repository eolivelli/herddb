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

package herddb.core.stats;

import io.netty.util.internal.PlatformDependent;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Registers Prometheus gauges for Netty's own direct-memory accounting so the
 * unified JVM dashboard has visibility into the pool-arena footprint that is
 * invisible to the standard {@code jvm_buffer_direct_*} series when the JVM
 * is configured with {@code -Dio.netty.maxDirectMemory=0} (which makes Netty
 * account under {@code Bits.reserveMemory} and therefore appear in the JVM
 * {@code direct} buffer pool) or vice-versa when Netty tracks its own
 * counter.
 *
 * <p>Gauges registered at the top level of the caller-provided scope
 * (prefix them with {@code netty_} by passing the root
 * {@code StatsLogger} rather than a nested scope):
 * <ul>
 *     <li>{@code netty_used_direct_memory_bytes} — {@link
 *         PlatformDependent#usedDirectMemory()}. Returns {@code -1} when
 *         Netty defers to the JDK's accounting — in that case
 *         {@code jvm_buffer_direct_used_bytes} is authoritative.</li>
 *     <li>{@code netty_max_direct_memory_bytes} — {@link
 *         PlatformDependent#maxDirectMemory()}. Returns the ceiling Netty
 *         enforces; see {@code io.netty.maxDirectMemory}.</li>
 * </ul>
 *
 * <p>Each JVM — server, indexing service, remote file service — should call
 * {@link #register} once at startup so the dashboard can correlate direct-
 * memory pressure against the {@code MaxDirectMemorySize} budget without
 * having to know which service it is looking at.
 */
public final class NettyMemoryMetrics {

    private NettyMemoryMetrics() {
    }

    /**
     * Registers the Netty direct-memory gauges on {@code statsLogger}. The
     * gauges are named with a {@code netty_} prefix baked into the metric
     * names themselves so every service emits the same series regardless of
     * the caller-chosen scope. Safe to call once per JVM; calling repeatedly
     * registers duplicates, which is undefined behaviour in most
     * {@link StatsLogger} implementations.
     */
    public static void register(StatsLogger statsLogger) {
        if (statsLogger == null) {
            return;
        }
        statsLogger.registerGauge("netty_used_direct_memory_bytes", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return PlatformDependent.usedDirectMemory();
            }
        });
        statsLogger.registerGauge("netty_max_direct_memory_bytes", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return PlatformDependent.maxDirectMemory();
            }
        });
    }
}
