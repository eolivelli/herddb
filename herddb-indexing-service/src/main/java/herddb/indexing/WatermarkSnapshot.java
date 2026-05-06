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

import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Table;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable bundle of the durable indexing-service recovery state captured at
 * a successful checkpoint: the last applied {@link LogSequenceNumber}, the
 * wall-clock timestamp of that {@link herddb.log.LogEntry}, the engine's
 * effective {@code numInstances} at that point, and a snapshot of the schema
 * ({@link Table} and vector {@link Index} definitions) tracked by the engine's
 * {@link SchemaTracker} at the time of the checkpoint.
 *
 * <p>Persisting the schema alongside the watermark LSN ensures that a
 * restarting engine can hydrate its in-memory {@code SchemaTracker} from the
 * snapshot and start the commit-log tailer from the watermark position (rather
 * than {@code START_OF_TIME}), even when early ledgers containing the original
 * {@code CREATE_TABLE} / {@code CREATE_INDEX} entries have been trimmed from
 * BookKeeper by the server's retention policy.
 *
 * <p>Persisting {@link #lastEntryTimestamp} (issue #423) lets diagnostic
 * tooling report the freshness of the durable state as
 * {@code now - lastEntryTimestamp} (in milliseconds) — an operator-friendly
 * complement to the LSN coordinates that does not require knowing the
 * commit-log layout.
 *
 * @author enrico.olivelli
 */
public final class WatermarkSnapshot {

    /**
     * Sentinel for "no recovery state yet" — used when the watermark file
     * does not exist on disk. {@link #numInstances} is 0, telling the engine
     * to fall back to its JVM-property bootstrap value. {@link #tables} and
     * {@link #vectorIndexes} are empty, telling the engine to start the tailer
     * from {@code START_OF_TIME} and rebuild its schema by replaying DDL entries.
     * {@link #lastEntryTimestamp} is 0 ("unknown") so diagnostic consumers can
     * treat it as "no freshness information yet".
     */
    public static final WatermarkSnapshot START_OF_TIME =
            new WatermarkSnapshot(LogSequenceNumber.START_OF_TIME, 0, 0L,
                    Collections.emptyList(), Collections.emptyList());

    public final LogSequenceNumber lsn;

    /**
     * Effective {@code numInstances} the engine was using at the time of the
     * checkpoint. Zero means "unknown" — typically because the watermark
     * file was written by a pre-feature build that did not persist this
     * value. The engine treats zero as "fall back to the bootstrap value".
     */
    public final int numInstances;

    /**
     * Wall-clock timestamp (epoch ms) of the {@link herddb.log.LogEntry} at
     * {@link #lsn} — i.e. the freshness of the durable recovery state at the
     * moment this snapshot was published. {@code 0} means "unknown" and is
     * used by {@link #START_OF_TIME} (no entries have been processed yet).
     * Issue #423: surfaced in {@code GetIndexStatus} as
     * {@code durable_lsn_timestamp} so dashboards can report
     * {@code durable_lag_ms = now - lastEntryTimestamp}.
     */
    public final long lastEntryTimestamp;

    /**
     * Snapshot of every {@link Table} tracked by the engine's
     * {@link SchemaTracker} at the time of the checkpoint. Empty when the
     * snapshot is {@link #START_OF_TIME}. On a non-empty schema, the engine
     * can skip replaying DDL entries from the commit log and start the tailer
     * directly from {@link #lsn}.
     */
    public final List<Table> tables;

    /**
     * Snapshot of every vector {@link Index} tracked by the engine's
     * {@link SchemaTracker} at the time of the checkpoint. Empty when the
     * snapshot has no schema (see {@link #tables}).
     */
    public final List<Index> vectorIndexes;

    /**
     * Full constructor — creates a snapshot with schema bundled.
     *
     * @param lsn                last applied LSN covered by the checkpoint
     * @param numInstances       effective routing fanout at checkpoint time
     * @param lastEntryTimestamp wall-clock (epoch ms) of the LogEntry at {@code lsn},
     *                           or {@code 0} when unknown
     * @param tables             all tables tracked by {@link SchemaTracker}
     * @param vectorIndexes      all vector indexes tracked by {@link SchemaTracker}
     */
    public WatermarkSnapshot(LogSequenceNumber lsn, int numInstances,
                              long lastEntryTimestamp,
                              List<Table> tables, List<Index> vectorIndexes) {
        this.lsn = Objects.requireNonNull(lsn, "lsn");
        if (numInstances < 0) {
            throw new IllegalArgumentException("numInstances must be >= 0, got " + numInstances);
        }
        if (lastEntryTimestamp < 0L) {
            throw new IllegalArgumentException(
                    "lastEntryTimestamp must be >= 0, got " + lastEntryTimestamp);
        }
        this.numInstances = numInstances;
        this.lastEntryTimestamp = lastEntryTimestamp;
        this.tables = Collections.unmodifiableList(
                Objects.requireNonNull(tables, "tables"));
        this.vectorIndexes = Collections.unmodifiableList(
                Objects.requireNonNull(vectorIndexes, "vectorIndexes"));
    }

    /**
     * Returns {@code true} when this snapshot carries a non-empty schema that
     * the engine can use to hydrate its {@link SchemaTracker} without replaying
     * commit-log DDL entries.
     */
    public boolean hasSchema() {
        return !tables.isEmpty() || !vectorIndexes.isEmpty();
    }

    @Override
    public String toString() {
        return "WatermarkSnapshot{lsn=" + lsn
                + ", numInstances=" + numInstances
                + ", lastEntryTimestamp=" + lastEntryTimestamp
                + ", tables=" + tables.size()
                + ", vectorIndexes=" + vectorIndexes.size()
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WatermarkSnapshot)) {
            return false;
        }
        WatermarkSnapshot that = (WatermarkSnapshot) o;
        return numInstances == that.numInstances
                && lastEntryTimestamp == that.lastEntryTimestamp
                && lsn.equals(that.lsn)
                && tables.equals(that.tables)
                && vectorIndexes.equals(that.vectorIndexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lsn, numInstances, lastEntryTimestamp, tables, vectorIndexes);
    }
}
