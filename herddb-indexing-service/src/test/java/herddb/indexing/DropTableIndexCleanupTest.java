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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@code DROP_INDEX} and {@code DROP_TABLE} log entries cause the
 * indexing-service engine to (a) close every affected vector store and
 * (b) remove every byte of on-storage data the store owned. Without this
 * cleanup, a workload that repeatedly creates and drops vector indexes would
 * leak segment files (graph, map, index pages, IndexStatus markers) on the
 * file server / S3 forever (issue #383).
 *
 * <p>The tests exercise the engine with a real {@link PersistentVectorStore} on
 * top of {@link MemoryDataStorageManager} so we can assert directly on the
 * stored-page set after the DROP. A spy {@link DataStorageManager} wrapper
 * records every {@code dropIndex} call so the test can verify both that the
 * engine called it and that the call carried the right
 * {@code (tableSpaceUUID, storeUUID)} pair.
 */
public class DropTableIndexCleanupTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private int savedMinLive;
    private long savedDeferral;

    @Before
    public void saveGateState() {
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        savedDeferral = PersistentVectorStore.maxCheckpointDeferralMs;
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreGateState() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
        PersistentVectorStore.maxCheckpointDeferralMs = savedDeferral;
    }

    private static final int DIM = 4;

    private static Table buildTable(String name) {
        return Table.builder()
                .name(name)
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static Index buildVectorIndex(String name, String tableName) {
        return Index.builder()
                .name(name)
                .table(tableName)
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    private static float[] randomVec(Random rng) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * {@link DataStorageManager} wrapper that delegates every call to a
     * backing instance and records each {@code dropIndex} invocation
     * <em>that targets a vector-store top-level UUID</em>. The
     * {@code _tmp_pkset_*} suffix is the {@link PersistentVectorStore}-internal
     * temp BLink storage used during checkpoint compaction; those are dropped
     * inside the regular checkpoint path and are not relevant to the
     * DROP_TABLE / DROP_INDEX cleanup contract under test.
     */
    private static final class RecordingDataStorageManager
            extends ForwardingDataStorageManager {
        final List<String[]> dropIndexCalls = new ArrayList<>();
        final List<String[]> dropIndexCallsAll = new ArrayList<>();

        RecordingDataStorageManager(DataStorageManager delegate) {
            super(delegate);
        }

        @Override
        public synchronized void dropIndex(String tableSpace, String name)
                throws DataStorageManagerException {
            dropIndexCallsAll.add(new String[]{tableSpace, name});
            // Skip the per-checkpoint temp BLink storage drops triggered by
            // PersistentVectorStore.createTempPagedPkSet — those happen on
            // every checkpoint and would mask the DROP-induced cleanup.
            if (!name.contains("_tmp_pkset_")) {
                dropIndexCalls.add(new String[]{tableSpace, name});
            }
            super.dropIndex(tableSpace, name);
        }

        synchronized List<String[]> dropIndexCallsCopy() {
            return new ArrayList<>(dropIndexCalls);
        }
    }

    /**
     * {@link PersistentVectorStore} subclass that flips a flag inside
     * {@code close()}, so a test can prove the engine actually invoked
     * {@code close()} on the store during DROP cleanup. Subclassing
     * (rather than wrapping) is required because {@link IndexingServiceEngine}
     * uses {@code instanceof PersistentVectorStore} to decide whether to drive
     * {@code checkpoint()} on each store — a wrapper would silently
     * de-activate the persistent path.
     */
    private static final class CloseTrackingPersistentVectorStore
            extends PersistentVectorStore {
        private final AtomicBoolean closed = new AtomicBoolean(false);

        CloseTrackingPersistentVectorStore(String indexName, String tableName,
                String tableSpaceUUID, String vectorColumnName,
                java.nio.file.Path tmpDirectory,
                DataStorageManager dataStorageManager,
                MemoryManager memoryManager,
                int m, int beamWidth, float neighborOverflow, float alpha,
                boolean fusedPQ, long maxSegmentSize, int maxLiveGraphSize,
                long compactionIntervalMs,
                VectorSimilarityFunction similarityFunction,
                long maxVectorMemoryBytes) {
            super(indexName, tableName, tableSpaceUUID, vectorColumnName,
                    tmpDirectory, dataStorageManager, memoryManager,
                    m, beamWidth, neighborOverflow, alpha,
                    fusedPQ, maxSegmentSize, maxLiveGraphSize,
                    compactionIntervalMs,
                    similarityFunction, maxVectorMemoryBytes);
        }

        @Override
        public void close() throws Exception {
            closed.set(true);
            super.close();
        }

        boolean wasClosed() {
            return closed.get();
        }
    }

    private IndexingServerConfiguration configuration() {
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        return new IndexingServerConfiguration(props);
    }

    private MemoryMetadataStorageManager newMeta() throws Exception {
        MemoryMetadataStorageManager m = new MemoryMetadataStorageManager();
        m.start();
        m.ensureDefaultTableSpace("local", "local", 0, 1);
        return m;
    }

    /**
     * After ingesting and checkpointing a vector index, applying
     * {@code DROP_INDEX} must (1) close the vector store, (2) call
     * {@code dataStorageManager.dropIndex(tableSpaceUUID, storeUUID)},
     * (3) clear every page/multipart/IndexStatus byte for that index, and
     * (4) leave a subsequent watermark snapshot free of the dropped index.
     *
     * <p>Before the issue #383 fix, only the in-memory map entry was removed
     * — pages, multipart files and IndexStatus markers stayed forever and
     * the store leaked its compaction/upload threads.
     */
    @Test
    public void dropIndexClosesStoreAndRemovesStoredData() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        IndexingServerConfiguration cfg = configuration();
        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, cfg);
        engine.setMetadataStorageManager(newMeta());

        MemoryDataStorageManager backing = new MemoryDataStorageManager();
        RecordingDataStorageManager dsm = new RecordingDataStorageManager(backing);
        engine.setDataStorageManager(dsm);

        MemoryManager mm = new MemoryManager(
                64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        AtomicReference<CloseTrackingPersistentVectorStore> spy = new AtomicReference<>();

        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName,
                                       dataDirectory, indexProperties) -> {
            // Resolve the engine's tablespace UUID lazily — the factory is
            // only invoked from CREATE_INDEX, well after engine.start() has
            // populated tableSpaceUUID via MetadataStorageManager.
            CloseTrackingPersistentVectorStore real = new CloseTrackingPersistentVectorStore(
                    indexName, tableName, engine.getTableSpaceUUID(), vectorColumnName,
                    dataDirectory, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE,
                    VectorSimilarityFunction.EUCLIDEAN, Long.MAX_VALUE);
            try {
                real.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            spy.set(real);
            return real;
        });

        AtomicReference<WatermarkSnapshot> latest = new AtomicReference<>();
        engine.setWatermarkStore(new WatermarkStore() {
            @Override
            public WatermarkSnapshot load() {
                return WatermarkSnapshot.START_OF_TIME;
            }

            @Override
            public void save(WatermarkSnapshot snapshot) throws IOException {
                latest.set(snapshot);
            }
        });

        Table table = buildTable("t1");
        Index ix = buildVectorIndex("vidx", "t1");
        Random rng = new Random(7);

        try {
            engine.start();

            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));

            // Capture the store UUID after CREATE_INDEX so we can verify the
            // engine passes the same UUID to dataStorageManager.dropIndex().
            String storeUUID = spy.get().getStoreUUID();
            assertNotNull("vector store factory must produce a UUID", storeUUID);

            // Ingest enough vectors to trigger at least one segment + IndexStatus.
            LogSequenceNumber last = null;
            for (int i = 0; i < 80; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "k" + i, "vec", randomVec(rng));
                LogEntry insert = LogEntryFactory.insert(table, rec.key, rec.value, null);
                last = new LogSequenceNumber(1, 100 + i);
                engine.applySingleEntryForTest(last, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(last);
            engine.forceCheckpointAndSaveWatermark();

            // Sanity: the watermark snapshot now lists the index, and the
            // backing store has at least one IndexStatus / multipart entry.
            assertNotNull("checkpoint must save a snapshot", latest.get());
            assertEquals("snapshot must list the live vector index",
                    1, latest.get().vectorIndexes.size());
            assertTrue(
                    "backing store must have data for the index after a checkpoint; got "
                            + countBackingEntries(backing, storeUUID),
                    countBackingEntries(backing, storeUUID) > 0);

            // ---- DROP_INDEX ----
            engine.applyEntry(new LogSequenceNumber(1, 999),
                    LogEntryFactory.dropIndex("vidx", null));
            engine.awaitPendingWorkForTest();
            engine.awaitPendingDeletionsForTest();

            // (1) The vector store wrapper must have been closed.
            assertTrue("DROP_INDEX must close the vector store (resource leak otherwise)",
                    spy.get().wasClosed());

            // (2) The engine must have called dataStorageManager.dropIndex
            //     with (tableSpaceUUID, storeUUID).
            List<String[]> calls = dsm.dropIndexCallsCopy();
            assertEquals("DROP_INDEX must invoke dataStorageManager.dropIndex exactly once",
                    1, calls.size());
            assertEquals("dropIndex tableSpace argument must be the engine's tablespace UUID",
                    engine.getTableSpaceUUID(), calls.get(0)[0]);
            assertEquals("dropIndex name argument must be the vector store UUID",
                    storeUUID, calls.get(0)[1]);

            // (3) No pages / multipart files / index statuses for the
            //     dropped index UUID may remain in the backing store.
            assertEquals(
                    "DROP_INDEX must erase all on-storage data for the index; survivors="
                            + listBackingEntries(backing, storeUUID),
                    0, countBackingEntries(backing, storeUUID));

            // (4) A subsequent checkpoint must not list the dropped index.
            engine.setLastProcessedLsnForTest(new LogSequenceNumber(1, 1000));
            latest.set(null);
            engine.forceCheckpointAndSaveWatermark();
            WatermarkSnapshot post = latest.get();
            assertNotNull("post-DROP checkpoint must publish a snapshot", post);
            assertEquals("post-DROP snapshot must NOT list the dropped index",
                    0, post.vectorIndexes.size());
            assertEquals("post-DROP snapshot must NOT list the dropped table either "
                            + "(table not dropped here, but the only vector index is gone)",
                    1, post.tables.size());

            // (5) Idempotency: applying DROP_INDEX again is a no-op (no NPE,
            //     no extra dataStorageManager.dropIndex call) — the engine
            //     no longer tracks the store.
            engine.applyEntry(new LogSequenceNumber(1, 1500),
                    LogEntryFactory.dropIndex("vidx", null));
            engine.awaitPendingWorkForTest();
            engine.awaitPendingDeletionsForTest();
            assertEquals("repeated DROP_INDEX must be a no-op",
                    1, dsm.dropIndexCallsCopy().size());
        } finally {
            engine.close();
        }
    }

    /**
     * {@code DROP_TABLE} must close every vector index defined on that table
     * and remove every byte of their on-storage data — not just the first
     * one, and not just the in-memory map entry.
     */
    @Test
    public void dropTableClosesAllIndexesAndRemovesData() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        IndexingServiceEngine engine = new IndexingServiceEngine(
                logDir, dataDir, configuration());
        engine.setMetadataStorageManager(newMeta());

        MemoryDataStorageManager backing = new MemoryDataStorageManager();
        RecordingDataStorageManager dsm = new RecordingDataStorageManager(backing);
        engine.setDataStorageManager(dsm);
        MemoryManager mm = new MemoryManager(
                64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        List<CloseTrackingPersistentVectorStore> spies = new ArrayList<>();
        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName,
                                       dataDirectory, indexProperties) -> {
            CloseTrackingPersistentVectorStore real = new CloseTrackingPersistentVectorStore(
                    indexName, tableName, engine.getTableSpaceUUID(), vectorColumnName,
                    dataDirectory, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE,
                    VectorSimilarityFunction.EUCLIDEAN, Long.MAX_VALUE);
            try {
                real.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            synchronized (spies) {
                spies.add(real);
            }
            return real;
        });

        engine.setWatermarkStore(new WatermarkStore() {
            @Override
            public WatermarkSnapshot load() {
                return WatermarkSnapshot.START_OF_TIME;
            }

            @Override
            public void save(WatermarkSnapshot snapshot) throws IOException {
            }
        });

        Table table = buildTable("t2");
        Index ix1 = buildVectorIndex("vidx_a", "t2");
        Index ix2 = buildVectorIndex("vidx_b", "t2");
        Random rng = new Random(11);

        try {
            engine.start();

            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix1, null));
            engine.applyEntry(new LogSequenceNumber(1, 3),
                    LogEntryFactory.createIndex(ix2, null));

            // Snapshot the two store UUIDs (stable across the rest of the test).
            Set<String> storeUUIDs = new HashSet<>();
            synchronized (spies) {
                assertEquals("two vector stores must have been created",
                        2, spies.size());
                for (CloseTrackingPersistentVectorStore s : spies) {
                    storeUUIDs.add(s.getStoreUUID());
                }
            }
            assertEquals(2, storeUUIDs.size());

            // Ingest into both indexes.
            LogSequenceNumber last = null;
            for (int i = 0; i < 60; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "k" + i, "vec", randomVec(rng));
                LogEntry insert = LogEntryFactory.insert(table, rec.key, rec.value, null);
                last = new LogSequenceNumber(1, 100 + i);
                engine.applySingleEntryForTest(last, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(last);
            engine.forceCheckpointAndSaveWatermark();

            for (String uuid : storeUUIDs) {
                assertTrue(
                        "each store must have data after the checkpoint; uuid=" + uuid
                                + " entries=" + countBackingEntries(backing, uuid),
                        countBackingEntries(backing, uuid) > 0);
            }

            // ---- DROP_TABLE ----
            engine.applyEntry(new LogSequenceNumber(1, 999),
                    LogEntryFactory.dropTable("t2", null));
            engine.awaitPendingWorkForTest();
            engine.awaitPendingDeletionsForTest();

            // Both stores must be closed.
            for (CloseTrackingPersistentVectorStore s : spies) {
                assertTrue("DROP_TABLE must close every vector store on that table",
                        s.wasClosed());
            }

            // dataStorageManager.dropIndex must have been called for both UUIDs.
            Set<String> droppedUuids = new HashSet<>();
            for (String[] call : dsm.dropIndexCallsCopy()) {
                assertEquals("dropIndex tableSpace must be the engine's tablespace UUID",
                        engine.getTableSpaceUUID(), call[0]);
                droppedUuids.add(call[1]);
            }
            assertEquals("DROP_TABLE must drop on-storage data for every vector index",
                    storeUUIDs, droppedUuids);

            // No pages / multipart / index status entries remain for either UUID.
            for (String uuid : storeUUIDs) {
                assertEquals(
                        "DROP_TABLE must erase all on-storage data for index UUID " + uuid,
                        0, countBackingEntries(backing, uuid));
            }
        } finally {
            engine.close();
        }
    }

    /**
     * Submits a forced checkpoint and a {@code DROP_INDEX} back-to-back to
     * exercise the serialisation between {@link
     * IndexingServiceEngine#submitVectorStoreDeletion} and
     * {@code checkpointAndSaveWatermark()} on the single-thread
     * {@code checkpointExecutor}. Closing the store while a concurrent
     * checkpoint cycle still held a reference to it would surface as an
     * {@link IllegalStateException} or NPE — none of which is expected to
     * happen with the executor-serialised drop path.
     */
    @Test
    public void dropIndexConcurrentWithCheckpointDoesNotCrash() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        IndexingServiceEngine engine = new IndexingServiceEngine(
                logDir, dataDir, configuration());
        engine.setMetadataStorageManager(newMeta());

        MemoryDataStorageManager backing = new MemoryDataStorageManager();
        RecordingDataStorageManager dsm = new RecordingDataStorageManager(backing);
        engine.setDataStorageManager(dsm);
        MemoryManager mm = new MemoryManager(
                64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        AtomicInteger created = new AtomicInteger(0);
        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName,
                                       dataDirectory, indexProperties) -> {
            PersistentVectorStore pvs = new PersistentVectorStore(
                    indexName, tableName, engine.getTableSpaceUUID(), vectorColumnName,
                    dataDirectory, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE,
                    VectorSimilarityFunction.EUCLIDEAN);
            try {
                pvs.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            created.incrementAndGet();
            return pvs;
        });

        engine.setWatermarkStore(new WatermarkStore() {
            @Override
            public WatermarkSnapshot load() {
                return WatermarkSnapshot.START_OF_TIME;
            }

            @Override
            public void save(WatermarkSnapshot snapshot) throws IOException {
            }
        });

        Table table = buildTable("t3");
        Index ix = buildVectorIndex("vidx", "t3");
        Random rng = new Random(13);

        try {
            engine.start();

            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));

            LogSequenceNumber last = null;
            for (int i = 0; i < 50; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "k" + i, "vec", randomVec(rng));
                LogEntry insert = LogEntryFactory.insert(table, rec.key, rec.value, null);
                last = new LogSequenceNumber(1, 100 + i);
                engine.applySingleEntryForTest(last, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(last);

            // Trigger the async checkpoint without waiting for it.
            engine.triggerCheckpointAsyncForTest();

            // Apply DROP_INDEX immediately — this submits the close+drop
            // task to the same single-thread checkpoint executor, so it
            // must run AFTER the in-flight checkpoint cycle.
            engine.applyEntry(new LogSequenceNumber(1, 999),
                    LogEntryFactory.dropIndex("vidx", null));
            engine.awaitPendingWorkForTest();
            engine.awaitPendingDeletionsForTest();

            // Wait for the async checkpoint future too (completed by now,
            // but not racy to await) so any swallowed exception is
            // surfaced as an ExecutionException.
            java.util.concurrent.Future<?> inflight =
                    engine.getInflightCheckpointFutureForTest();
            if (inflight != null) {
                inflight.get(30, java.util.concurrent.TimeUnit.SECONDS);
            }

            // Verify the engine completed both operations: store created,
            // dropIndex called once, no leak in the backing store.
            assertEquals("vector store factory was invoked exactly once",
                    1, created.get());
            assertEquals("dropIndex must run exactly once",
                    1, dsm.dropIndexCallsCopy().size());
            // After DROP, listIndexes must not include the dropped index.
            List<IndexingServiceEngine.IndexDescriptor> indexes = engine.listIndexes();
            for (IndexingServiceEngine.IndexDescriptor d : indexes) {
                assertFalse("dropped index must not be visible after DROP_INDEX",
                        "vidx".equals(d.getIndex()) && "t3".equals(d.getTable()));
            }
        } finally {
            engine.close();
        }
    }

    /**
     * Counts every on-storage entry (page, IndexStatus marker, multipart
     * blob) the backing {@link MemoryDataStorageManager} holds for a given
     * vector-store UUID. Used by the cleanup tests to assert "zero residual
     * data" after a DROP.
     *
     * <p>The keys observed in {@link MemoryDataStorageManager} are:
     * <ul>
     *   <li>{@code "<tableSpace>.<storeUUID>_<pageId>"} for index pages,
     *   <li>{@code "<tableSpace>.<storeUUID>_<ledgerId>.<offset>"} for
     *       checkpoint markers (IndexStatus),
     *   <li>{@code "<tableSpace>/<storeUUID>/..."} for multipart blob files.
     * </ul>
     */
    private static int countBackingEntries(MemoryDataStorageManager backing, String storeUUID) {
        return listBackingEntries(backing, storeUUID).size();
    }

    private static List<String> listBackingEntries(MemoryDataStorageManager backing, String storeUUID) {
        List<String> hits = new ArrayList<>();
        // Iterate via reflection to peek at the private maps so we can
        // verify the engine cleanup path actually erased the on-storage
        // bytes, not just the in-memory metadata list.
        for (java.lang.reflect.Field f : MemoryDataStorageManager.class.getDeclaredFields()) {
            if (!java.util.concurrent.ConcurrentHashMap.class.isAssignableFrom(f.getType())) {
                continue;
            }
            f.setAccessible(true);
            java.util.concurrent.ConcurrentHashMap<?, ?> map;
            try {
                map = (java.util.concurrent.ConcurrentHashMap<?, ?>) f.get(backing);
            } catch (IllegalAccessException e) {
                throw new AssertionError("reflection failed: " + f.getName(), e);
            }
            for (Object key : map.keySet()) {
                String s = key.toString();
                // page key:    "<tableSpace>.<storeUUID>_..."
                // status key:  "<tableSpace>.<storeUUID>_..."
                // multipart:   "<tableSpace>/<storeUUID>/..."
                if (s.endsWith(storeUUID)
                        || s.contains("." + storeUUID + "_")
                        || s.contains("/" + storeUUID + "/")) {
                    hits.add(f.getName() + ": " + s);
                }
            }
        }
        return hits;
    }
}
