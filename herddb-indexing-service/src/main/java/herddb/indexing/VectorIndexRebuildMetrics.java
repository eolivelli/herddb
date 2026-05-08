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
package herddb.indexing;

import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Issue #471 — engine-wide counters for the {@code rebuild=true}
 * back-fill pass driven by {@link VectorIndexRebuilder}.
 *
 * <p>The two counters track every record the rebuild scan visited and
 * the subset that was actually inserted into the local vector store
 * (the difference is the shard-filter rejection count for non-local
 * keys). They are monotonically increasing for the lifetime of the
 * engine, even across multiple CREATE VECTOR INDEX runs — operators
 * derive rate via Prometheus.
 *
 * <p>Threading: writes happen only on the tailer thread inside
 * {@code VectorIndexRebuilder.run}, but reads happen from arbitrary
 * Prometheus-exporter threads via the registered {@link Gauge}s.
 * {@link LongAdder} gives lock-free striped writes and an
 * eventually-consistent read.
 *
 * @author enrico.olivelli
 */
public final class VectorIndexRebuildMetrics {

    /**
     * Total records the rebuild scan has streamed from the table's
     * pinned checkpoint, regardless of whether they were locally
     * indexed. Bumped in the rebuilder's {@code acceptPage} consumer
     * once per {@link herddb.model.Record}.
     */
    public final LongAdder recordsScanned = new LongAdder();

    /**
     * Subset of {@link #recordsScanned} that was actually inserted
     * into the local vector store — i.e. records whose primary key
     * passed the engine's shard predicate
     * ({@code IndexingServiceEngine.isAcceptedLocally}). The delta
     * {@code scanned - indexed} is the count of records the rebuild
     * skipped because another IS replica owns them.
     */
    public final LongAdder recordsIndexed = new LongAdder();

    /**
     * Wall-clock millis when the most recent rebuild started, or
     * {@code 0} if no rebuild has ever run on this engine. Updated
     * exactly once per rebuild attempt before
     * {@code dataStorageManager.fullTableScan} is invoked.
     */
    public volatile long lastRebuildStartTimeMillis;

    /**
     * Wall-clock millis when the most recent rebuild ENDED (regardless
     * of success or failure), or {@code 0} if no rebuild has ever
     * run. Updated exactly once per rebuild attempt in the rebuilder's
     * {@code finally}, AFTER the scan returns or throws.
     *
     * <p>Operators detect "rebuild in flight" by
     * {@code lastRebuildStartTimeMillis > lastRebuildEndTimeMillis}.
     */
    public volatile long lastRebuildEndTimeMillis;

    /**
     * Registers Prometheus gauges for {@link #recordsScanned},
     * {@link #recordsIndexed}, and the start/end millis fields under
     * the {@code rebuild.*} scope of {@code statsLogger}. Idempotent —
     * the underlying {@link StatsLogger} re-registers the same key
     * gracefully — but callers should invoke this exactly once per
     * engine instance during startup.
     */
    public void register(StatsLogger statsLogger) {
        if (statsLogger == null) {
            return;
        }
        StatsLogger scope = statsLogger.scope("rebuild");
        scope.registerGauge("records_scanned", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0L;
            }

            @Override
            public Number getSample() {
                return recordsScanned.sum();
            }
        });
        scope.registerGauge("records_indexed", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0L;
            }

            @Override
            public Number getSample() {
                return recordsIndexed.sum();
            }
        });
        scope.registerGauge("last_start_time_ms", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0L;
            }

            @Override
            public Number getSample() {
                return lastRebuildStartTimeMillis;
            }
        });
        scope.registerGauge("last_end_time_ms", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0L;
            }

            @Override
            public Number getSample() {
                return lastRebuildEndTimeMillis;
            }
        });
    }
}
