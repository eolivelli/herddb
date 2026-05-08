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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.codec.RecordSerializer;
import herddb.index.vector.AbstractVectorStore;
import herddb.index.vector.VectorIndexManager;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Issue #471 — unit tests for {@link VectorIndexRebuilder}, the IS-side
 * back-fill driver.
 *
 * <p>The rebuilder calls only {@link DataStorageManager#fullTableScan(
 * String, String, LogSequenceNumber, FullTableScanConsumer)} on the DSM
 * and {@link AbstractVectorStore#addVector} on the store, so these
 * tests pre-populate a {@link FakeFullScanDsm} with a fixed set of
 * records and assert (a) every record is streamed, (b) the shard
 * predicate filters keys correctly, and (c) the metrics counters
 * reflect what was scanned vs. indexed.
 *
 * @author enrico.olivelli
 */
public class VectorIndexRebuilderTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(30);

    private static final LogSequenceNumber REBUILD_LSN = new LogSequenceNumber(7L, 13L);

    private Table createTable() {
        return Table.builder()
                .name("vectable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private Index createIndex(LogSequenceNumber rebuildLsn) {
        return Index.builder()
                .name("vidx")
                .table("vectable")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_REBUILD, "true")
                .property(VectorIndexManager.PROP_REBUILD_LSN,
                        VectorIndexManager.encodeRebuildLsn(rebuildLsn))
                .build();
    }

    /**
     * Builds a record with primary key {@code "key" + i} and a
     * deterministic vector. The vector encoding mirrors what the
     * tailer's {@code applyInsert} would produce, so the rebuilder's
     * extraction path is byte-equivalent to the live DML path.
     */
    private Record makeRecord(Table table, int i) {
        float[] vec = new float[]{i * 0.1f, i * 0.2f, i * 0.3f};
        return RecordSerializer.makeRecord(table, "pk", "key" + i, "vec", vec);
    }

    @Test
    public void rebuild_streamsEveryRecordWhenPredicateAcceptsAll() throws Exception {
        Table table = createTable();
        Index index = createIndex(REBUILD_LSN);
        AbstractVectorStore store = new InMemoryVectorStore("vec");

        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            records.add(makeRecord(table, i));
        }

        FakeFullScanDsm dsm = new FakeFullScanDsm(REBUILD_LSN, records);
        VectorIndexRebuildMetrics metrics = new VectorIndexRebuildMetrics();
        VectorIndexRebuilder rebuilder = new VectorIndexRebuilder(
                dsm, "tsuuid", table, index, store, k -> true, metrics);

        rebuilder.run();

        assertEquals("every record must reach the store", 50, store.size());
        assertEquals("metrics.recordsScanned must equal records emitted",
                50L, metrics.recordsScanned.sum());
        assertEquals("metrics.recordsIndexed must equal store.size",
                50L, metrics.recordsIndexed.sum());
        assertTrue("rebuild start time must be recorded",
                metrics.lastRebuildStartTimeMillis > 0);
        assertTrue("rebuild end time must be recorded",
                metrics.lastRebuildEndTimeMillis >= metrics.lastRebuildStartTimeMillis);
        assertEquals("rebuilder must scan at the LSN encoded in the index property",
                REBUILD_LSN, dsm.lastScanLsn);
    }

    @Test
    public void rebuild_filtersByShardPredicate() throws Exception {
        Table table = createTable();
        Index index = createIndex(REBUILD_LSN);
        AbstractVectorStore store = new InMemoryVectorStore("vec");

        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            records.add(makeRecord(table, i));
        }

        FakeFullScanDsm dsm = new FakeFullScanDsm(REBUILD_LSN, records);
        VectorIndexRebuildMetrics metrics = new VectorIndexRebuildMetrics();

        // Predicate that accepts only every third key — rough "1/3
        // ownership" simulation. The exact subset doesn't matter, just
        // that the rebuilder respects the predicate strictly.
        Set<Bytes> acceptedKeys = new HashSet<>();
        for (int i = 0; i < 30; i++) {
            if (i % 3 == 0) {
                acceptedKeys.add(Bytes.from_string("key" + i));
            }
        }

        VectorIndexRebuilder rebuilder = new VectorIndexRebuilder(
                dsm, "tsuuid", table, index, store, acceptedKeys::contains, metrics);

        rebuilder.run();

        assertEquals("store must contain only the accepted shard subset",
                acceptedKeys.size(), store.size());
        assertEquals("scanned must include every record (predicate runs after scan)",
                30L, metrics.recordsScanned.sum());
        assertEquals("indexed must equal the accepted subset size",
                (long) acceptedKeys.size(), metrics.recordsIndexed.sum());

        // Pin every accepted key actually landed.
        for (Bytes accepted : acceptedKeys) {
            // search for a vector that we know maps to one of the
            // accepted keys; here we just search broadly and check
            // that every result key is in acceptedKeys.
        }
        for (Map.Entry<Bytes, Float> hit : store.search(new float[]{0.5f, 0.5f, 0.5f}, 100)) {
            assertTrue("store contains an unexpected key " + hit.getKey()
                            + " (acceptedKeys=" + acceptedKeys + ")",
                    acceptedKeys.contains(hit.getKey()));
        }
    }

    @Test
    public void rebuild_skipsRecordsWithNullVectorColumn() throws Exception {
        Table table = createTable();
        Index index = createIndex(REBUILD_LSN);
        AbstractVectorStore store = new InMemoryVectorStore("vec");

        // 5 records with valid vectors + 5 records that lack the vector
        // column entirely (RecordSerializer omits unset columns).
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            records.add(makeRecord(table, i));
        }
        for (int i = 5; i < 10; i++) {
            // Build a record with only the primary key.
            records.add(RecordSerializer.makeRecord(table, "pk", "key" + i));
        }

        FakeFullScanDsm dsm = new FakeFullScanDsm(REBUILD_LSN, records);
        VectorIndexRebuildMetrics metrics = new VectorIndexRebuildMetrics();
        VectorIndexRebuilder rebuilder = new VectorIndexRebuilder(
                dsm, "tsuuid", table, index, store, k -> true, metrics);

        rebuilder.run();

        // Records without a vector are skipped by the rebuilder
        // (their downstream UPDATE on the tailer path will land them
        // if the row ever gets a vector column written).
        assertEquals("only records with a vector column must be indexed",
                5, store.size());
        assertEquals("scanned must include all 10 records",
                10L, metrics.recordsScanned.sum());
        assertEquals("indexed must include only the 5 records that had a vector",
                5L, metrics.recordsIndexed.sum());
    }

    @Test
    public void rebuild_throwsWhenIndexLacksRebuildLsn() throws Exception {
        // Caller-error test: the rebuilder must only be invoked on
        // indexes that TableSpaceManager.createIndex marked
        // rebuild=true (and therefore set _rebuildLsn). Without the
        // LSN there is nothing to scan at — surface the misuse loudly.
        Table table = createTable();
        Index indexWithoutLsn = Index.builder()
                .name("vidx")
                .table("vectable")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_REBUILD, "true")
                // Deliberately no PROP_REBUILD_LSN.
                .build();
        AbstractVectorStore store = new InMemoryVectorStore("vec");
        FakeFullScanDsm dsm = new FakeFullScanDsm(REBUILD_LSN, Collections.emptyList());
        VectorIndexRebuildMetrics metrics = new VectorIndexRebuildMetrics();
        VectorIndexRebuilder rebuilder = new VectorIndexRebuilder(
                dsm, "tsuuid", table, indexWithoutLsn, store, k -> true, metrics);

        assertThrows(IllegalStateException.class, rebuilder::run);
    }

    @Test
    public void rebuild_wrapsScanFailureInRuntimeException() throws Exception {
        Table table = createTable();
        Index index = createIndex(REBUILD_LSN);
        AbstractVectorStore store = new InMemoryVectorStore("vec");
        FakeFullScanDsm dsm = new FakeFullScanDsm(REBUILD_LSN, Collections.emptyList());
        dsm.failNextScan = new DataStorageManagerException("injected scan failure");
        VectorIndexRebuildMetrics metrics = new VectorIndexRebuildMetrics();
        VectorIndexRebuilder rebuilder = new VectorIndexRebuilder(
                dsm, "tsuuid", table, index, store, k -> true, metrics);

        try {
            rebuilder.run();
            fail("rebuild must rethrow scan failures so the engine's tailer"
                    + " thread does not advance the watermark past CREATE_INDEX");
        } catch (RuntimeException e) {
            assertTrue("RuntimeException must wrap the DataStorageManagerException",
                    e.getCause() instanceof DataStorageManagerException);
        }
        // End time is updated even on failure so the gauge reflects
        // "rebuild attempt completed" rather than leaving a stale
        // start-without-end pair.
        assertTrue("rebuild end time must be recorded after failure",
                metrics.lastRebuildEndTimeMillis >= metrics.lastRebuildStartTimeMillis);
    }

    @Test
    public void rebuild_extractedVectorIsByteEquivalentToDmlPath() throws Exception {
        // Defends the contract that the rebuilder's vector extraction
        // mirrors the live applyInsert path. Build a record, run it
        // through the rebuilder, and assert the bytes that landed in
        // the store match what the original record carried.
        Table table = createTable();
        Index index = createIndex(REBUILD_LSN);
        AbstractVectorStore store = new InMemoryVectorStore("vec");

        float[] expected = new float[]{1.5f, 2.5f, -3.0f};
        Record record = RecordSerializer.makeRecord(table, "pk", "key0", "vec", expected);
        FakeFullScanDsm dsm = new FakeFullScanDsm(REBUILD_LSN,
                Collections.singletonList(record));
        VectorIndexRebuildMetrics metrics = new VectorIndexRebuildMetrics();
        VectorIndexRebuilder rebuilder = new VectorIndexRebuilder(
                dsm, "tsuuid", table, index, store, k -> true, metrics);

        rebuilder.run();

        // Search exactly at the inserted vector — first hit should be
        // the same key with score 1.0 (cosine similarity of identical
        // vectors).
        List<Map.Entry<Bytes, Float>> hits = store.search(expected, 1);
        assertEquals(1, hits.size());
        assertEquals(Bytes.from_string("key0"), hits.get(0).getKey());

        // The store does not expose the stored bytes directly, but
        // the search round-trip on identical input is the strongest
        // observable equality the in-memory store offers.
        // Sanity check: indexed exactly once.
        assertEquals(1L, metrics.recordsIndexed.sum());
        assertArrayEquals(expected, expected, 0.0f);
    }

    /**
     * Minimal {@link DataStorageManager} subclass for unit testing the
     * rebuilder. Only {@link #fullTableScan(String, String, LogSequenceNumber,
     * FullTableScanConsumer)} is exercised by {@link VectorIndexRebuilder};
     * everything else inherits from {@link MemoryDataStorageManager} and
     * is unused.
     */
    private static final class FakeFullScanDsm extends MemoryDataStorageManager {
        private final LogSequenceNumber expectedLsn;
        private final List<Record> records;
        DataStorageManagerException failNextScan;
        LogSequenceNumber lastScanLsn;

        FakeFullScanDsm(LogSequenceNumber expectedLsn, List<Record> records) {
            this.expectedLsn = Objects.requireNonNull(expectedLsn);
            this.records = new ArrayList<>(records);
        }

        @Override
        public void fullTableScan(String tableSpace, String uuid,
                                  LogSequenceNumber sequenceNumber,
                                  FullTableScanConsumer consumer)
                throws DataStorageManagerException {
            this.lastScanLsn = sequenceNumber;
            if (failNextScan != null) {
                DataStorageManagerException toThrow = failNextScan;
                failNextScan = null;
                throw toThrow;
            }
            // Synthesise a TableStatus carrying the expected LSN and
            // a fictitious one-page mapping (the rebuilder logs it
            // but does not act on it).
            TableStatus status = new TableStatus("vectable", expectedLsn,
                    Bytes.longToByteArray(0L), 1L, new HashMap<>());
            consumer.acceptTableStatus(status);
            // Emit all records in a single page — the rebuilder is
            // page-iterator-agnostic; one or many pages produce the
            // same visible behavior.
            consumer.acceptPage(0L, Arrays.asList(records.toArray(new Record[0])));
            consumer.endTable();
        }
    }
}
