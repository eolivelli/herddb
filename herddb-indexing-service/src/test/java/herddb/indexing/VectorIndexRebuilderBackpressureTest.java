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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.index.vector.VectorIndexManager;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Issue #471 — verifies that the rebuild's per-record
 * {@link herddb.index.vector.AbstractVectorStore#addVector} call respects
 * the {@link PersistentVectorStore} back-pressure layers and that a
 * heavily back-pressured rebuild completes without deadlocking.
 *
 * <p>The test configures a {@code PersistentVectorStore} with a tiny
 * {@code maxLiveGraphSize=4} and {@code compactionBackpressureThreshold=2},
 * so segment rotation fires every 4 inserts and the segment-count
 * back-pressure path must engage repeatedly during a 100-record
 * rebuild. The load-bearing assertion is that the rebuild
 * <strong>completes</strong> — without back-pressure-aware ingest the
 * test would either deadlock or fail with an unbounded segment-count
 * exception.
 *
 * <p>This is the smallest reproducer of the 20 B-row scenario the user
 * called out: at production scale, compaction cannot keep up with
 * ingest if the rebuilder bypasses the back-pressure path. Here we
 * compress the same dynamic into ~100 inserts so the test runs in
 * sub-second time on CI.
 *
 * @author enrico.olivelli
 */
public class VectorIndexRebuilderBackpressureTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    private static final LogSequenceNumber REBUILD_LSN = new LogSequenceNumber(7L, 13L);

    private int savedMinLive;
    private long savedDeferral;

    @Before
    public void disableMinLiveGate() {
        // PersistentVectorStore.checkpoint defers when the live shard
        // has fewer than minLiveVectorsForCheckpoint vectors — disable
        // the gate so the rebuild's post-scan store.checkpoint() call
        // actually flushes the small live shard for assertion purposes.
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        savedDeferral = PersistentVectorStore.maxCheckpointDeferralMs;
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreMinLiveGate() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
        PersistentVectorStore.maxCheckpointDeferralMs = savedDeferral;
    }

    private Table createTable() {
        return Table.builder()
                .name("vectable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private Index createIndex() {
        return Index.builder()
                .name("vidx")
                .table("vectable")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_REBUILD, "true")
                .property(VectorIndexManager.PROP_REBUILD_LSN,
                        VectorIndexManager.encodeRebuildLsn(REBUILD_LSN))
                .build();
    }

    private PersistentVectorStore buildBackpressuredStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore("vidx", "vectable",
                "tstblspace", "vec", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L,
                /* maxLiveGraphSize */ 4,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN);
        // Aggressive segment-count back-pressure: 2 sealed segments
        // before addVector blocks. Combined with maxLiveGraphSize=4,
        // the rebuild is forced to (a) rotate after every 4 inserts,
        // (b) hit the segment-count cap repeatedly, and (c) wait for
        // background compaction to drain. The same code path applies
        // at 20 B-row production scale.
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 2,
                Integer.MAX_VALUE, 0);
        return store;
    }

    @Test
    public void rebuild_completesUnderHeavyBackpressure_withoutDeadlock() throws Exception {
        Path tmpDir = folder.newFolder("vstore").toPath();

        Table table = createTable();
        Index index = createIndex();
        PersistentVectorStore store = buildBackpressuredStore(tmpDir);
        try {
            store.start();
            // 100 records — at maxLiveGraphSize=4 that is ~25
            // rotations, more than enough to hit the segment-count
            // back-pressure cap (which is 2) repeatedly. If the
            // rebuilder were not back-pressure-aware, this scan would
            // produce 25 sealed segments without bound and OOM the
            // segment map; if addVector did not block on the cap,
            // the test would deadlock instead.
            int numRecords = 100;
            List<Record> records = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                records.add(RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 0.01f, i * 0.02f, i * 0.03f}));
            }

            FakeFullScanDsm dsm = new FakeFullScanDsm(REBUILD_LSN, records);
            VectorIndexRebuildMetrics metrics = new VectorIndexRebuildMetrics();
            VectorIndexRebuilder rebuilder = new VectorIndexRebuilder(
                    dsm, "tstblspace", table, index, store,
                    k -> true, metrics);

            long startNanos = System.nanoTime();
            rebuilder.run();
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

            // Load-bearing: rebuild completed without deadlock.
            assertEquals("every record must reach the store",
                    (long) numRecords, metrics.recordsScanned.sum());
            assertEquals("every record must be indexed (predicate accepts all)",
                    (long) numRecords, metrics.recordsIndexed.sum());
            // Wall-clock sanity: a deadlocking rebuild would have hit
            // the @Rule Timeout(60s); a successful one finishes in
            // single-digit seconds even on a constrained CI runner.
            assertTrue("rebuild must finish in well under the test timeout (got "
                            + elapsedMs + " ms)",
                    elapsedMs < 30_000L);
            // The store must observably reflect the inserts.
            assertEquals("store size must equal records inserted",
                    numRecords, store.size());
        } finally {
            store.close();
        }
    }

    /**
     * Same record-store pair, smaller record count — sanity check that
     * the back-pressure-instrumented store does NOT regress the basic
     * "rebuild populates the store" contract under default conditions.
     */
    @Test
    public void rebuild_basicSmallScan_populatesStoreWithBackpressureStore() throws Exception {
        Path tmpDir = folder.newFolder("vstore").toPath();

        Table table = createTable();
        Index index = createIndex();
        PersistentVectorStore store = buildBackpressuredStore(tmpDir);
        try {
            store.start();

            int numRecords = 6;
            List<Record> records = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                records.add(RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 0.1f, 0f, 0f}));
            }

            FakeFullScanDsm dsm = new FakeFullScanDsm(REBUILD_LSN, records);
            VectorIndexRebuildMetrics metrics = new VectorIndexRebuildMetrics();
            VectorIndexRebuilder rebuilder = new VectorIndexRebuilder(
                    dsm, "tstblspace", table, index, store,
                    k -> true, metrics);
            rebuilder.run();

            assertEquals("indexed count must equal records",
                    (long) numRecords, metrics.recordsIndexed.sum());
            assertEquals("store size must equal records",
                    numRecords, store.size());
        } finally {
            store.close();
        }
    }

    /**
     * Minimal fake DSM that emits the staged record list as a single
     * page — the rebuilder's per-record back-pressure path is
     * exercised regardless of the per-page batching, so emitting
     * everything at once is sufficient.
     */
    private static final class FakeFullScanDsm extends MemoryDataStorageManager {
        private final LogSequenceNumber expectedLsn;
        private final List<Record> records;

        FakeFullScanDsm(LogSequenceNumber expectedLsn, List<Record> records) {
            this.expectedLsn = Objects.requireNonNull(expectedLsn);
            this.records = new ArrayList<>(records);
        }

        @Override
        public void fullTableScan(String tableSpace, String uuid,
                                  LogSequenceNumber sequenceNumber,
                                  FullTableScanConsumer consumer)
                throws DataStorageManagerException {
            TableStatus status = new TableStatus("vectable", expectedLsn,
                    Bytes.longToByteArray(0L), 1L, new HashMap<>());
            consumer.acceptTableStatus(status);
            consumer.acceptPage(0L, Arrays.asList(records.toArray(new Record[0])));
            consumer.endTable();
        }
    }
}
