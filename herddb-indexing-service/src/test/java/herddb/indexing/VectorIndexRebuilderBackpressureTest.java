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
import herddb.indexing.vector.PersistentVectorStore;
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
 * Issue #471 — verifies that the rebuild engages and survives the
 * {@link PersistentVectorStore} segment-count back-pressure path
 * (issue #354) when the rebuild scan must add records into a store
 * that already carries more sealed segments than the back-pressure
 * threshold.
 *
 * <p>Test setup:
 * <ul>
 *   <li>{@code maxLiveGraphSize=4}: small live-shard cap so each
 *       pre-seed cycle reliably produces a sealed segment.</li>
 *   <li>{@code compactionBackpressureThreshold=2}: tight threshold
 *       so the back-pressure path engages quickly.</li>
 *   <li>IS-local compaction loop disabled
 *       ({@code compactionIntervalMs=Long.MAX_VALUE}) so the
 *       pre-seeded segments do NOT get drained while we set up.</li>
 *   <li>{@code compactionBackpressureMaxWaitMs=500} ms: small safety
 *       timeout so the test does not deadlock — when compaction is
 *       disabled, the back-pressure wait force-releases and the
 *       rebuild proceeds. This is the same safety-net path that
 *       protects production from a hung compaction subsystem.</li>
 * </ul>
 *
 * <p>The load-bearing assertion is that the rebuild
 * <strong>completes</strong> AND that
 * {@code getSegmentCountBackpressureTotal()} reports at least one
 * back-pressure event. Without the rebuilder calling the public
 * {@code addVector} (i.e. if a future regression introduced a
 * fast-path that bypasses the back-pressure layers), the counter
 * would stay at zero and the test would fail.
 *
 * <p>This is the smallest reproducer of the 20 B-row scenario the
 * user called out: at production scale, compaction cannot keep up
 * with ingest if the rebuilder bypasses the back-pressure path.
 * Here we compress the same dynamic into ~10 records on a tightly
 * configured store, so the test runs in sub-second time on CI.
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

    @Before
    public void disableMinLiveGate() {
        // PersistentVectorStore.checkpoint defers when the live shard
        // has fewer than minLiveVectorsForCheckpoint vectors — disable
        // the gate so the rebuild's post-scan store.checkpoint() call
        // actually flushes the small live shard for assertion purposes.
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreMinLiveGate() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
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

    /**
     * Builds a store with a tiny live-shard cap, a tiny
     * segment-count back-pressure threshold, and the IS-local
     * compaction loop DISABLED so pre-seeded segments persist into
     * the rebuild. The back-pressure safety timeout is set to
     * 500 ms so the test does not deadlock when the wait
     * force-releases.
     */
    private PersistentVectorStore buildBackpressuredStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore("vidx", "vectable",
                "tstblspace", "vec", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L,
                /* maxLiveGraphSize */ 4,
                /* compactionIntervalMs */ Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN);
        // Disable the IS-local compaction loop so pre-seeded
        // segments persist into the rebuild. (configureCompaction's
        // 4th arg is `minCount` per its signature, NOT the
        // back-pressure threshold — that is set via the dedicated
        // setter below.)
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE,
                Integer.MAX_VALUE, Integer.MAX_VALUE, 0);
        // Tight back-pressure threshold: the rebuild's first
        // addVector observes segments.size() > 2 and enters
        // waitForSegmentCountRelief.
        store.setCompactionBackpressureThreshold(2);
        // 500 ms safety timeout — when the wait fails to drain
        // (because compaction is disabled), the safety path
        // force-releases, the rebuilder's addVector returns, and
        // the test completes instead of deadlocking. The same
        // safety path protects production against a hung compactor.
        store.setCompactionBackpressureMaxWaitMs(500L);
        return store;
    }

    /**
     * Pre-seeds the store with at least {@code targetSegments}
     * sealed on-disk segments, using primary keys disjoint from the
     * rebuild's keys ({@code "preseed-N"} vs. {@code "keyN"}). After
     * this returns, the store's {@code segments.size()} is &gt;=
     * {@code targetSegments}, so the very first {@code addVector}
     * call from the rebuild path will enter
     * {@code waitForSegmentCountRelief}.
     */
    private void preSeedSegments(PersistentVectorStore store, Table table,
                                 int targetSegments) throws Exception {
        // Each preSeed cycle: insert maxLiveGraphSize+1 vectors to
        // force a rotation, then call store.checkpoint() to seal
        // the rotated live shard into a sealed segment.
        int seedIdx = 0;
        for (int seg = 0; seg < targetSegments; seg++) {
            for (int i = 0; i <= 4; i++) {
                store.addVector(Bytes.from_string("preseed-" + (seedIdx++)),
                        new float[]{seg * 0.01f, i * 0.01f, 0f});
            }
            // Seal: convert live shards into on-disk segments.
            store.checkpoint();
        }
    }

    @Test
    public void rebuild_engagesAndSurvivesSegmentCountBackpressure() throws Exception {
        Path tmpDir = folder.newFolder("vstore").toPath();

        Table table = createTable();
        Index index = createIndex();
        PersistentVectorStore store = buildBackpressuredStore(tmpDir);
        try {
            store.start();

            // Pre-seed segments above the back-pressure threshold (=2).
            // The IS-local compaction loop is disabled in this test
            // (compactionIntervalMs=Long.MAX_VALUE), so segments
            // persist until the rebuild's addVector reads them.
            preSeedSegments(store, table, 4);
            int segmentsAfterPreSeed = store.getSegmentCount();
            assertTrue("preSeed must leave > 2 segments, got " + segmentsAfterPreSeed,
                    segmentsAfterPreSeed > 2);
            long backpressureBefore = store.getSegmentCountBackpressureTotal();

            // 5 records — small enough to keep the test under the
            // 60 s timeout when each addVector waits up to 500 ms in
            // the back-pressure safety path. Each addVector observes
            // segments.size() > 2 and enters waitForSegmentCountRelief;
            // compaction is disabled so the wait force-releases at
            // 500 ms; addVector returns and the next record is
            // attempted. Total back-pressure wait time per record:
            // up to 500 ms; total test runtime: ~2.5 s.
            int numRecords = 5;
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

            // Load-bearing: rebuild completed.
            assertEquals("every record must reach the store",
                    (long) numRecords, metrics.recordsScanned.sum());
            assertEquals("every record must be indexed (predicate accepts all)",
                    (long) numRecords, metrics.recordsIndexed.sum());
            // Sanity: must finish in less than the test timeout.
            assertTrue("rebuild must finish under the test timeout (got "
                            + elapsedMs + " ms)",
                    elapsedMs < 30_000L);
            // Load-bearing: the segment-count back-pressure path
            // must have engaged at least once. A regression that
            // bypasses addVector's back-pressure (e.g. by exposing a
            // raw insert path the rebuilder accidentally calls) would
            // skip waitForSegmentCountRelief and this counter would
            // stay at zero.
            long backpressureAfter = store.getSegmentCountBackpressureTotal();
            assertTrue("segment-count back-pressure must have engaged at least once "
                            + "(before=" + backpressureBefore + ", after=" + backpressureAfter + ")",
                    backpressureAfter > backpressureBefore);
        } finally {
            store.close();
        }
    }

    /**
     * Sanity check: with a fresh store (no pre-seed), the rebuild
     * still populates correctly. Locks down the basic correctness
     * path so a regression that broke the back-pressure interaction
     * does not also silently break the no-back-pressure path.
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
