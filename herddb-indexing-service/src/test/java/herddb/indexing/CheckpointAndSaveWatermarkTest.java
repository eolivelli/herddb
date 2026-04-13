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
import static org.junit.Assert.assertNull;
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
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end correctness test for
 * {@link IndexingServiceEngine#forceCheckpointAndSaveWatermark()}:
 *
 * <p>Verifies that when a {@link PersistentVectorStore} cannot complete a
 * checkpoint because another checkpoint is already in progress (the
 * {@code tryLock} skip path), the engine MUST NOT advance the watermark.
 * Advancing past LSNs that live only in the new live shard would lose those
 * entries on restart — this was the latent correctness bug fixed as Part 3
 * of issue #90.
 */
public class CheckpointAndSaveWatermarkTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private int savedMinLive;
    private long savedDeferral;

    @Before
    public void saveGateState() {
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        savedDeferral = PersistentVectorStore.maxCheckpointDeferralMs;
        // Ensure the min-live gate cannot interfere with the watermark
        // assertions — we want the store to reach Phase B.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreGateState() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
        PersistentVectorStore.maxCheckpointDeferralMs = savedDeferral;
    }

    /**
     * In-memory watermark store that records every save() call.
     */
    private static final class RecordingWatermarkStore implements WatermarkStore {
        private final AtomicReference<LogSequenceNumber> saved = new AtomicReference<>();
        private int saveCount;

        @Override
        public synchronized LogSequenceNumber load() {
            LogSequenceNumber v = saved.get();
            return v != null ? v : LogSequenceNumber.START_OF_TIME;
        }

        @Override
        public synchronized void save(LogSequenceNumber lsn) throws IOException {
            saved.set(lsn);
            saveCount++;
        }

        synchronized LogSequenceNumber current() {
            return saved.get();
        }

        synchronized int getSaveCount() {
            return saveCount;
        }
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

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private static MemoryMetadataStorageManager createTestMetadata() throws Exception {
        MemoryMetadataStorageManager m = new MemoryMetadataStorageManager();
        m.start();
        m.ensureDefaultTableSpace("local", "local", 0, 1);
        return m;
    }

    @Test
    public void testWatermarkNotAdvancedWhenCheckpointDeferred() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        // Use the persistent factory path — we need a real PersistentVectorStore.
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());

        RecordingWatermarkStore watermark = new RecordingWatermarkStore();
        engine.setWatermarkStore(watermark);

        // Install a factory that produces real PersistentVectorStore instances
        // backed by an in-memory DataStorageManager, exposing the underlying
        // store so the test can park it in Phase B.
        AtomicReference<PersistentVectorStore> storeRef = new AtomicReference<>();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(
                128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDirectory, indexProperties) -> {
            PersistentVectorStore pvs = new PersistentVectorStore(
                    indexName, tableName, "tstblspace", vectorColumnName,
                    dataDirectory, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE,
                    VectorSimilarityFunction.EUCLIDEAN);
            try {
                pvs.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            storeRef.set(pvs);
            return pvs;
        });

        try {
            engine.start();

            Table table = createTable();
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            Index index = Index.builder()
                    .name("vidx")
                    .table("vectable")
                    .type(Index.TYPE_VECTOR)
                    .column("vec", ColumnTypes.FLOATARRAY)
                    .build();
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(index, null));

            PersistentVectorStore store = storeRef.get();
            assertTrue("store should have been created by DDL", store != null);

            // Insert enough vectors to trip the FusedPQ threshold.
            Random rng = new Random(29);
            int numVectors = 512;
            int dim = 32;
            long baseLsn = 100;
            LogSequenceNumber lastLsn = null;
            for (int i = 0; i < numVectors; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "k" + i,
                        "vec", randomVector(rng, dim));
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                lastLsn = new LogSequenceNumber(1, baseLsn + i);
                engine.applySingleEntryForTest(lastLsn, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(lastLsn);

            // First call: should advance watermark because store.checkpoint()
            // will run Phase A→B→C cleanly (no concurrent checkpoint).
            engine.forceCheckpointAndSaveWatermark();
            assertEquals("first save should advance watermark",
                    1, watermark.getSaveCount());
            assertEquals("watermark at LSN of last processed entry",
                    new LogSequenceNumber(1, baseLsn + numVectors - 1),
                    watermark.current());

            // Add more vectors so the next checkpoint has real work to do.
            for (int i = 0; i < 200; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "k" + (numVectors + i),
                        "vec", randomVector(rng, dim));
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                lastLsn = new LogSequenceNumber(1, baseLsn + numVectors + i);
                engine.applySingleEntryForTest(lastLsn, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(lastLsn);

            // Park a background checkpoint inside Phase B so the next
            // forceCheckpointAndSaveWatermark() call hits the tryLock-skip.
            CountDownLatch phaseBEntered = new CountDownLatch(1);
            CountDownLatch releasePhaseB = new CountDownLatch(1);
            store.setCheckpointPhaseBHook(() -> {
                phaseBEntered.countDown();
                try {
                    releasePhaseB.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            AtomicBoolean parkedResult = new AtomicBoolean();
            AtomicReference<Exception> error = new AtomicReference<>();
            Thread parked = new Thread(() -> {
                try {
                    parkedResult.set(store.checkpoint());
                } catch (Exception e) {
                    error.compareAndSet(null, e);
                }
            }, "parked-cp");
            parked.start();

            assertTrue("parked checkpoint did not reach Phase B",
                    phaseBEntered.await(30, TimeUnit.SECONDS));

            LogSequenceNumber watermarkBefore = watermark.current();
            int saveCountBefore = watermark.getSaveCount();

            // Engine-level attempt: must return without advancing the watermark.
            engine.forceCheckpointAndSaveWatermark();

            assertEquals("watermark MUST NOT advance on tryLock-skip",
                    saveCountBefore, watermark.getSaveCount());
            assertEquals("watermark LSN MUST NOT change on tryLock-skip",
                    watermarkBefore, watermark.current());

            releasePhaseB.countDown();
            parked.join(30_000);
            if (error.get() != null) {
                throw error.get();
            }
            assertTrue("parked checkpoint should have returned true",
                    parkedResult.get());

            // Clear the hook and retry: now the next call should advance
            // the watermark to the latest processed LSN.
            store.setCheckpointPhaseBHook(null);
            engine.forceCheckpointAndSaveWatermark();
            assertEquals("second successful save", saveCountBefore + 1,
                    watermark.getSaveCount());
            assertEquals("watermark advanced to last processed LSN",
                    new LogSequenceNumber(1, baseLsn + numVectors + 200 - 1),
                    watermark.current());
        } finally {
            engine.close();
        }
    }

    /**
     * Sanity check: when {@link PersistentVectorStore.checkpoint()} returns
     * {@code false} via the min-live-vectors deferral gate, the engine must
     * also refuse to advance the watermark. This guarantees that the two
     * deferral paths share the same correctness guarantee.
     */
    @Test
    public void testWatermarkNotAdvancedWhenMinLiveGateDefers() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());

        RecordingWatermarkStore watermark = new RecordingWatermarkStore();
        engine.setWatermarkStore(watermark);

        AtomicReference<PersistentVectorStore> storeRef = new AtomicReference<>();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(
                128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDirectory, indexProperties) -> {
            PersistentVectorStore pvs = new PersistentVectorStore(
                    indexName, tableName, "tstblspace", vectorColumnName,
                    dataDirectory, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE,
                    VectorSimilarityFunction.EUCLIDEAN);
            try {
                pvs.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            storeRef.set(pvs);
            return pvs;
        });

        try {
            engine.start();

            Table table = createTable();
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            Index index = Index.builder()
                    .name("vidx")
                    .table("vectable")
                    .type(Index.TYPE_VECTOR)
                    .column("vec", ColumnTypes.FLOATARRAY)
                    .build();
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(index, null));

            Random rng = new Random(41);
            int dim = 32;
            // Bootstrap with 300 vectors (above FusedPQ threshold of 256).
            LogSequenceNumber lastLsn = null;
            for (int i = 0; i < 300; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "k" + i,
                        "vec", randomVector(rng, dim));
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                lastLsn = new LogSequenceNumber(1, 100 + i);
                engine.applySingleEntryForTest(lastLsn, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(lastLsn);

            // First save: bootstrap — segments.isEmpty(), gate bypassed.
            engine.forceCheckpointAndSaveWatermark();
            assertEquals(1, watermark.getSaveCount());
            LogSequenceNumber firstWatermark = watermark.current();
            assertEquals(new LogSequenceNumber(1, 100 + 300 - 1), firstWatermark);

            // Arm the gate with a very high threshold and a long deferral,
            // then add a tiny batch and re-invoke. The store must defer.
            PersistentVectorStore.minLiveVectorsForCheckpoint = 10_000;
            PersistentVectorStore.maxCheckpointDeferralMs = 600_000L;

            for (int i = 0; i < 50; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "k" + (300 + i),
                        "vec", randomVector(rng, dim));
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                lastLsn = new LogSequenceNumber(1, 500 + i);
                engine.applySingleEntryForTest(lastLsn, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(lastLsn);

            engine.forceCheckpointAndSaveWatermark();
            assertEquals("gate-deferred call MUST NOT advance watermark",
                    1, watermark.getSaveCount());
            assertEquals("watermark LSN unchanged",
                    firstWatermark, watermark.current());
            assertTrue("store should report a deferred cycle",
                    storeRef.get().getTotalCheckpointsDeferred() >= 1);
        } finally {
            engine.close();
        }
    }

    /** Guard against accidental assertNull import removal. */
    @Test
    public void testRecordingWatermarkStartsNull() {
        RecordingWatermarkStore s = new RecordingWatermarkStore();
        assertNull(s.current());
        assertEquals(0, s.getSaveCount());
        assertFalse(s.load() == null);
    }
}
