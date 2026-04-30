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
 * engine's effective {@code numInstances} at that point, and a snapshot of the
 * schema ({@link Table} and vector {@link Index} definitions) tracked by the
 * engine's {@link SchemaTracker} at the time of the checkpoint.
 *
 * <p>Persisting the schema alongside the watermark LSN ensures that a
 * restarting engine can hydrate its in-memory {@code SchemaTracker} from the
 * snapshot and start the commit-log tailer from the watermark position (rather
 * than {@code START_OF_TIME}), even when early ledgers containing the original
 * {@code CREATE_TABLE} / {@code CREATE_INDEX} entries have been trimmed from
 * BookKeeper by the server's retention policy.
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
     */
    public static final WatermarkSnapshot START_OF_TIME =
            new WatermarkSnapshot(LogSequenceNumber.START_OF_TIME, 0,
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
     * Snapshot of every {@link Table} tracked by the engine's
     * {@link SchemaTracker} at the time of the checkpoint. Empty when the
     * snapshot was loaded from a pre-schema watermark file (old format or
     * {@link #START_OF_TIME}). On a non-empty schema, the engine can skip
     * replaying DDL entries from the commit log and start the tailer directly
     * from {@link #lsn}.
     */
    public final List<Table> tables;

    /**
     * Snapshot of every vector {@link Index} tracked by the engine's
     * {@link SchemaTracker} at the time of the checkpoint. Empty when the
     * snapshot has no schema (see {@link #tables}).
     */
    public final List<Index> vectorIndexes;

    /**
     * Legacy constructor — creates a snapshot with no schema (empty tables and
     * vector-indexes lists). The engine will start the tailer from
     * {@code START_OF_TIME} and rebuild its schema by replaying DDL entries.
     */
    public WatermarkSnapshot(LogSequenceNumber lsn, int numInstances) {
        this(lsn, numInstances, Collections.emptyList(), Collections.emptyList());
    }

    /**
     * Full constructor — creates a snapshot with schema bundled.
     *
     * @param lsn            last applied LSN covered by the checkpoint
     * @param numInstances   effective routing fanout at checkpoint time
     * @param tables         all tables tracked by {@link SchemaTracker}
     * @param vectorIndexes  all vector indexes tracked by {@link SchemaTracker}
     */
    public WatermarkSnapshot(LogSequenceNumber lsn, int numInstances,
                              List<Table> tables, List<Index> vectorIndexes) {
        this.lsn = Objects.requireNonNull(lsn, "lsn");
        if (numInstances < 0) {
            throw new IllegalArgumentException("numInstances must be >= 0, got " + numInstances);
        }
        this.numInstances = numInstances;
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
                && lsn.equals(that.lsn)
                && tables.equals(that.tables)
                && vectorIndexes.equals(that.vectorIndexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lsn, numInstances, tables, vectorIndexes);
    }
}
