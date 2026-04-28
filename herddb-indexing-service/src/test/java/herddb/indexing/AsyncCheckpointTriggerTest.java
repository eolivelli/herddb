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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the async tailer-driven checkpoint trigger introduced for issue #213.
 *
 * <p>Before issue #213 the tailer thread executed
 * {@link IndexingServiceEngine#forceCheckpointAndSaveWatermark() checkpointAndSaveWatermark()}
 * inline; while Phase B of the FusedPQ checkpoint was running the tailer
 * could not dispatch new entries, and all striped apply-workers went idle.
 * The fix hands the checkpoint off to a single-thread background executor so
 * the tailer returns immediately and apply-workers keep receiving work.
 *
 * <p>These tests drive the engine directly (bypassing a real commit-log
 * tailer) and use {@link PersistentVectorStore#setCheckpointPhaseBHook(Runnable)}
 * to park the checkpoint inside Phase B, which is where the blocking
 * {@code Future.get()} lived.
 */
public class AsyncCheckpointTriggerTest {

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

    private static final class RecordingWatermarkStore implements WatermarkStore {
        private final AtomicReference<WatermarkSnapshot> saved = new AtomicReference<>();
        private volatile int saveCount;

        @Override
        public synchronized WatermarkSnapshot load() {
            WatermarkSnapshot v = saved.get();
            return v != null ? v : WatermarkSnapshot.START_OF_TIME;
        }

        @Override
        public synchronized void save(WatermarkSnapshot snapshot) throws IOException {
            saved.set(snapshot);
            saveCount++;
        }

        synchronized LogSequenceNumber current() {
            WatermarkSnapshot v = saved.get();
            return v != null ? v.lsn : null;
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

    /**
     * Context holding the engine, the live PersistentVectorStore, and the
     * recording watermark so every test can share the boot path.
     */
    private static final class EngineCtx implements AutoCloseable {
        final IndexingServiceEngine engine;
        final PersistentVectorStore store;
        final RecordingWatermarkStore watermark;
        final Table table;
        final LogSequenceNumber bootstrapLastLsn;

        EngineCtx(IndexingServiceEngine engine, PersistentVectorStore store,
                  RecordingWatermarkStore watermark, Table table,
                  LogSequenceNumber bootstrapLastLsn) {
            this.engine = engine;
            this.store = store;
            this.watermark = watermark;
            this.table = table;
            this.bootstrapLastLsn = bootstrapLastLsn;
        }

        @Override
        public void close() throws Exception {
            engine.close();
        }
    }

    /**
     * Boots an engine with a persistent vector-store factory, applies the
     * DDL, ingests enough vectors to clear the FusedPQ threshold, takes one
     * successful checkpoint to establish a baseline watermark, and returns
     * the context with the live store exposed.
     */
    private EngineCtx bootAndBaseline() throws Exception {
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

        engine.start();

        Table table = createTable();
        engine.applyEntry(new LogSequenceNumber(1, 1), LogEntryFactory.createTable(table, null));
        Index index = Index.builder()
                .name("vidx")
                .table("vectable")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
        engine.applyEntry(new LogSequenceNumber(1, 2), LogEntryFactory.createIndex(index, null));

        PersistentVectorStore store = storeRef.get();
        assertNotNull("store should have been created by DDL", store);

        Random rng = new Random(29);
        int numVectors = 300;
        int dim = 32;
        LogSequenceNumber lastLsn = null;
        for (int i = 0; i < numVectors; i++) {
            Record record = RecordSerializer.makeRecord(table,
                    "pk", "k" + i,
                    "vec", randomVector(rng, dim));
            LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
            lastLsn = new LogSequenceNumber(1, 100 + i);
            engine.applySingleEntryForTest(lastLsn, insert);
        }
        engine.awaitPendingWorkForTest();
        engine.setLastProcessedLsnForTest(lastLsn);

        // Establish a baseline watermark so later deltas are easy to reason about.
        engine.forceCheckpointAndSaveWatermark();
        assertEquals("baseline checkpoint should have saved once",
                1, watermark.getSaveCount());

        return new EngineCtx(engine, store, watermark, table, lastLsn);
    }

    private LogSequenceNumber ingestMore(EngineCtx ctx, long startLsn, int count, Random rng, int dim) {
        LogSequenceNumber lastLsn = null;
        for (int i = 0; i < count; i++) {
            Record record = RecordSerializer.makeRecord(ctx.table,
                    "pk", "k-extra-" + startLsn + "-" + i,
                    "vec", randomVector(rng, dim));
            LogEntry insert = LogEntryFactory.insert(ctx.table, record.key, record.value, null);
            lastLsn = new LogSequenceNumber(1, startLsn + i);
            ctx.engine.applySingleEntryForTest(lastLsn, insert);
        }
        ctx.engine.awaitPendingWorkForTest();
        ctx.engine.setLastProcessedLsnForTest(lastLsn);
        return lastLsn;
    }

    /**
     * When a Phase B hook parks the store's checkpoint, a tailer-driven
     * trigger must return quickly (no Future.get() on the caller thread).
     * After the hook releases, the in-flight Future must complete and the
     * watermark must advance to the LSN captured at trigger time.
     */
    @Test(timeout = 60_000)
    public void testTriggerIsNonBlockingAndWatermarkAdvancesAfterRelease() throws Exception {
        try (EngineCtx ctx = bootAndBaseline()) {
            Random rng = new Random(37);
            LogSequenceNumber lastLsn = ingestMore(ctx, 2_000, 200, rng, 32);

            CountDownLatch phaseBEntered = new CountDownLatch(1);
            CountDownLatch releasePhaseB = new CountDownLatch(1);
            ctx.store.setCheckpointPhaseBHook(() -> {
                phaseBEntered.countDown();
                try {
                    releasePhaseB.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            int saveCountBefore = ctx.watermark.getSaveCount();

            long t0 = System.nanoTime();
            ctx.engine.triggerCheckpointAsyncForTest();
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
            // Submitting to the executor should be effectively instant; allow
            // generous slack for a loaded CI machine.
            assertTrue("trigger must not block the caller; elapsed=" + elapsedMs + " ms",
                    elapsedMs < 1_000);

            Future<?> inflight = ctx.engine.getInflightCheckpointFutureForTest();
            assertNotNull("inflight future should be tracked", inflight);

            assertTrue("async checkpoint must have reached Phase B",
                    phaseBEntered.await(30, TimeUnit.SECONDS));

            // While the checkpoint is parked, the watermark must NOT have moved.
            assertEquals("watermark must not advance while checkpoint is parked",
                    saveCountBefore, ctx.watermark.getSaveCount());

            releasePhaseB.countDown();
            inflight.get(30, TimeUnit.SECONDS);

            assertEquals("watermark should advance once async checkpoint completes",
                    saveCountBefore + 1, ctx.watermark.getSaveCount());
            assertEquals("watermark advanced to the LSN captured at trigger time",
                    lastLsn, ctx.watermark.current());

            ctx.store.setCheckpointPhaseBHook(null);
        }
    }

    /**
     * A second trigger fired while the first async checkpoint is still
     * running must be coalesced (same Future) — the engine relies on the
     * {@code tryLock} skip inside PersistentVectorStore as a safety net, but
     * we want to avoid queueing redundant work in the first place.
     */
    @Test(timeout = 60_000)
    public void testOverlappingTriggersAreCoalesced() throws Exception {
        try (EngineCtx ctx = bootAndBaseline()) {
            Random rng = new Random(41);
            ingestMore(ctx, 3_000, 200, rng, 32);

            CountDownLatch phaseBEntered = new CountDownLatch(1);
            CountDownLatch releasePhaseB = new CountDownLatch(1);
            ctx.store.setCheckpointPhaseBHook(() -> {
                phaseBEntered.countDown();
                try {
                    releasePhaseB.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            ctx.engine.triggerCheckpointAsyncForTest();
            Future<?> first = ctx.engine.getInflightCheckpointFutureForTest();
            assertNotNull(first);
            assertTrue(phaseBEntered.await(30, TimeUnit.SECONDS));

            // Second trigger while first is parked — must be coalesced.
            ctx.engine.triggerCheckpointAsyncForTest();
            Future<?> second = ctx.engine.getInflightCheckpointFutureForTest();
            assertSame("second trigger must reuse the in-flight future", first, second);

            releasePhaseB.countDown();
            first.get(30, TimeUnit.SECONDS);

            // After completion a fresh trigger IS allowed (we use a no-op
            // hook now so the new checkpoint completes quickly).
            ctx.store.setCheckpointPhaseBHook(null);
            ctx.engine.triggerCheckpointAsyncForTest();
            Future<?> third = ctx.engine.getInflightCheckpointFutureForTest();
            assertNotNull(third);
            assertTrue("a new submission is expected after the previous one finished",
                    third != first);
            third.get(30, TimeUnit.SECONDS);
        }
    }

    /**
     * {@code close()} must wait for any in-flight async checkpoint so the
     * final watermark is persisted before the engine shuts down.
     */
    @Test(timeout = 60_000)
    public void testCloseAwaitsInflightCheckpoint() throws Exception {
        EngineCtx ctx = bootAndBaseline();
        try {
            Random rng = new Random(53);
            LogSequenceNumber lastLsn = ingestMore(ctx, 4_000, 200, rng, 32);

            // A long-but-bounded delay inside Phase B: close() must wait it
            // out instead of racing the watermark save.
            CountDownLatch phaseBEntered = new CountDownLatch(1);
            ctx.store.setCheckpointPhaseBHook(() -> {
                phaseBEntered.countDown();
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            int saveCountBefore = ctx.watermark.getSaveCount();
            ctx.engine.triggerCheckpointAsyncForTest();
            assertTrue("async checkpoint must have reached Phase B",
                    phaseBEntered.await(30, TimeUnit.SECONDS));

            // close() should block until the parked checkpoint above finishes,
            // then observe the watermark save.
            ctx.engine.close();

            assertEquals("close must wait for in-flight checkpoint to persist watermark",
                    saveCountBefore + 1, ctx.watermark.getSaveCount());
            assertEquals("watermark matches last processed LSN at trigger time",
                    lastLsn, ctx.watermark.current());
        } finally {
            // Safety: if the assertion before engine.close() failed, make
            // sure we still clean up.
            try {
                ctx.engine.close();
            } catch (Exception ignored) {
                // already closed
            }
        }
    }

    /**
     * {@code forceCheckpointAndSaveWatermark()} must first await any
     * in-flight async checkpoint, then run a fresh synchronous checkpoint,
     * so tests and admin callers observe a deterministic post-condition
     * regardless of what the tailer has been doing.
     */
    @Test(timeout = 60_000)
    public void testForceAwaitsInflightAndRunsFreshCheckpoint() throws Exception {
        try (EngineCtx ctx = bootAndBaseline()) {
            Random rng = new Random(61);
            LogSequenceNumber lastLsn = ingestMore(ctx, 5_000, 200, rng, 32);

            CountDownLatch phaseBEntered = new CountDownLatch(1);
            CountDownLatch releasePhaseB = new CountDownLatch(1);
            ctx.store.setCheckpointPhaseBHook(() -> {
                phaseBEntered.countDown();
                try {
                    releasePhaseB.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            int saveCountBefore = ctx.watermark.getSaveCount();
            ctx.engine.triggerCheckpointAsyncForTest();
            assertTrue(phaseBEntered.await(30, TimeUnit.SECONDS));

            // Run force() on a worker thread so we can unblock Phase B after
            // verifying it has started to wait.
            Thread forcer = new Thread(ctx.engine::forceCheckpointAndSaveWatermark, "test-forcer");
            forcer.start();

            // Give the forcer time to reach the inflight.get() wait. Then
            // release Phase B: the async task finishes, force() runs a second
            // checkpoint on the caller, which must save the watermark again.
            // (There is no deterministic way to observe the forcer blocking
            // without reflection; the timing here is generous.)
            Thread.sleep(500);
            assertEquals("async checkpoint must not have saved yet while Phase B is parked",
                    saveCountBefore, ctx.watermark.getSaveCount());
            releasePhaseB.countDown();

            // Clear the hook so the force()'s own synchronous checkpoint
            // does not park a second time.
            ctx.store.setCheckpointPhaseBHook(null);

            forcer.join(30_000);
            assertTrue("forcer thread should have terminated", !forcer.isAlive());

            // At least two saves: one from the async task, one from force().
            // There may be extra transient saves if the store advances the
            // LSN in between, but strictly more than the baseline.
            assertTrue("watermark must have advanced at least twice; saveCount="
                            + ctx.watermark.getSaveCount(),
                    ctx.watermark.getSaveCount() >= saveCountBefore + 2);
            assertEquals("watermark points at the last processed LSN",
                    lastLsn, ctx.watermark.current());
        }
    }

    /** Guard: if the engine is never started, close() must not NPE on checkpointExecutor. */
    @Test
    public void testCloseBeforeStartDoesNotFail() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.close();
        assertNull("no in-flight checkpoint before start",
                engine.getInflightCheckpointFutureForTest());
    }
}
