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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
import herddb.storage.DataStorageManagerException;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for the asynchronous post-Phase-C block-cache warmup introduced by
 * issue #472.
 *
 * <p>Issue #322 added a synchronous warmup that holds the
 * {@code indexing-checkpoint} thread until the entry-point neighbourhood of
 * every segment has been BFS-walked (~3 s for 21 × 33 MiB segments in the
 * gist1m k3s-bench profile). Issue #472 moves that warmup onto a dedicated
 * {@code indexing-warmup} executor so the watermark snapshot is published
 * immediately and the next checkpoint cycle is not blocked.
 *
 * <p>The tests in this class verify:
 * <ol>
 *   <li>Async mode (default): the engine submits the warmup to a separate
 *       executor and returns a non-null {@link Future}; the warmup completes
 *       and reads bytes.</li>
 *   <li>Sync mode (opt-in): the warmup runs inline on the calling thread and
 *       no async {@link Future} is published — backward-compatibility with
 *       the original issue #322 behaviour.</li>
 *   <li>Coalescing: while a warmup is in flight a second submit is dropped,
 *       preventing unbounded executor queueing.</li>
 *   <li>Shutdown: an in-flight warmup does not prevent {@link
 *       IndexingServiceEngine#close()} from returning within a sane bound.</li>
 *   <li>Non-blocking ordering: with a slow data-storage manager the watermark
 *       is saved <em>before</em> the warmup future completes, proving the
 *       checkpoint thread is not blocked on the warmup.</li>
 *   <li>System-property fallback: the JVM property
 *       {@link IndexingServerConfiguration#SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_ASYNC}
 *       is honoured when the properties-file key is absent.</li>
 * </ol>
 *
 * @see BlockCacheWarmupTest existing tests for the warmup byte-budget logic
 *      and the per-segment BFS, which are unchanged by issue #472.
 */
public class BlockCacheWarmupAsyncTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private int savedMinLive;

    @Before
    public void lowerMinLiveGate() {
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        // Allow checkpoints to run even with a small vector batch.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreMinLiveGate() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
    }

    // -------------------------------------------------------------------------
    // Helpers (intentionally duplicated from BlockCacheWarmupTest — keeping
    // this test self-contained so it can be run in isolation without the
    // other warmup tests being on the classpath).
    // -------------------------------------------------------------------------

    /** In-memory watermark store that records every {@code save()} call. */
    private static final class RecordingWatermarkStore implements WatermarkStore {

        private final AtomicReference<WatermarkSnapshot> saved = new AtomicReference<>();
        private int saveCount;

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

        synchronized int getSaveCount() {
            return saveCount;
        }
    }

    private static float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private static Table buildTable() {
        return Table.builder()
                .name("vectable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static MemoryMetadataStorageManager createTestMetadata() throws Exception {
        MemoryMetadataStorageManager m = new MemoryMetadataStorageManager();
        m.start();
        m.ensureDefaultTableSpace("local", "local", 0, 1);
        return m;
    }

    /**
     * A {@link MemoryDataStorageManager} that wraps every
     * {@link ReaderSupplier} in a counting + optionally-blocking shim so
     * tests can both observe how many bytes were read and gate the warmup on
     * a {@link CountDownLatch}.
     */
    private static final class ControllableMemoryDataStorageManager extends MemoryDataStorageManager {

        final AtomicLong totalBytesRead = new AtomicLong();
        /** Sleeps this many ms before each read when {@code slowReads} is on. */
        final AtomicLong perReadDelayMs = new AtomicLong(0);
        /**
         * If non-null, every read awaits this latch BEFORE issuing the
         * underlying read. Tests use this to "freeze" the warmup so they can
         * observe interim state (watermark already saved while warmup
         * remains in flight).
         */
        volatile CountDownLatch readGate = null;

        @Override
        public ReaderSupplier multipartIndexReaderSupplier(
                String tableSpace, String uuid, String fileType, long fileSize)
                throws DataStorageManagerException {
            ReaderSupplier original = super.multipartIndexReaderSupplier(
                    tableSpace, uuid, fileType, fileSize);
            return new CountingReaderSupplier(original, this);
        }
    }

    private static final class CountingReaderSupplier implements ReaderSupplier {

        private final ReaderSupplier delegate;
        private final ControllableMemoryDataStorageManager owner;

        CountingReaderSupplier(ReaderSupplier delegate, ControllableMemoryDataStorageManager owner) {
            this.delegate = delegate;
            this.owner = owner;
        }

        @Override
        public RandomAccessReader get() throws IOException {
            return new CountingReader(delegate.get(), owner);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class CountingReader implements RandomAccessReader {

        private final RandomAccessReader delegate;
        private final ControllableMemoryDataStorageManager owner;

        CountingReader(RandomAccessReader delegate, ControllableMemoryDataStorageManager owner) {
            this.delegate = delegate;
            this.owner = owner;
        }

        private void preReadGateAndDelay() throws IOException {
            CountDownLatch gate = owner.readGate;
            if (gate != null) {
                try {
                    if (!gate.await(60, TimeUnit.SECONDS)) {
                        throw new IOException("read gate timed out");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted at read gate", e);
                }
            }
            long delayMs = owner.perReadDelayMs.get();
            if (delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted in slow read", e);
                }
            }
        }

        @Override
        public void readFully(byte[] dest) throws IOException {
            preReadGateAndDelay();
            delegate.readFully(dest);
            owner.totalBytesRead.addAndGet(dest.length);
        }

        @Override
        public void readFully(ByteBuffer buffer) throws IOException {
            preReadGateAndDelay();
            int before = buffer.remaining();
            delegate.readFully(buffer);
            owner.totalBytesRead.addAndGet((long) (before - buffer.remaining()));
        }

        @Override
        public void readFully(long[] longs) throws IOException {
            preReadGateAndDelay();
            delegate.readFully(longs);
            owner.totalBytesRead.addAndGet((long) longs.length * Long.BYTES);
        }

        @Override
        public void read(int[] ints, int offset, int count) throws IOException {
            preReadGateAndDelay();
            delegate.read(ints, offset, count);
            owner.totalBytesRead.addAndGet((long) count * Integer.BYTES);
        }

        @Override
        public void read(float[] floats, int offset, int count) throws IOException {
            preReadGateAndDelay();
            delegate.read(floats, offset, count);
            owner.totalBytesRead.addAndGet((long) count * Float.BYTES);
        }

        @Override
        public void seek(long offset) throws IOException {
            delegate.seek(offset);
        }

        @Override
        public long getPosition() throws IOException {
            return delegate.getPosition();
        }

        @Override
        public int readInt() throws IOException {
            preReadGateAndDelay();
            int v = delegate.readInt();
            owner.totalBytesRead.addAndGet(Integer.BYTES);
            return v;
        }

        @Override
        public float readFloat() throws IOException {
            preReadGateAndDelay();
            float v = delegate.readFloat();
            owner.totalBytesRead.addAndGet(Float.BYTES);
            return v;
        }

        @Override
        public long readLong() throws IOException {
            preReadGateAndDelay();
            long v = delegate.readLong();
            owner.totalBytesRead.addAndGet(Long.BYTES);
            return v;
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /**
     * Builds a configured {@link IndexingServiceEngine} ready for the
     * checkpoint-and-warmup flow.
     *
     * @param props pre-populated properties (storage type, warmup byte
     *     budget, async mode flag) — the helper adds nothing more
     */
    private TestEngine newEngine(Properties props) throws Exception {
        Path logDir = folder.newFolder().toPath();
        Path dataDir = folder.newFolder().toPath();

        IndexingServerConfiguration config = new IndexingServerConfiguration(props);
        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());

        RecordingWatermarkStore watermark = new RecordingWatermarkStore();
        engine.setWatermarkStore(watermark);

        ControllableMemoryDataStorageManager dsm = new ControllableMemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        AtomicReference<PersistentVectorStore> storeRef = new AtomicReference<>();
        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDirectory, indexProperties) -> {
            PersistentVectorStore pvs = new PersistentVectorStore(
                    indexName, tableName, "tstblspace", vectorColumnName,
                    dataDirectory, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE, VectorSimilarityFunction.EUCLIDEAN);
            try {
                pvs.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            storeRef.set(pvs);
            return pvs;
        });

        return new TestEngine(engine, dsm, watermark, storeRef);
    }

    /** Bundle of references the tests need from a built engine. */
    private static final class TestEngine implements AutoCloseable {

        final IndexingServiceEngine engine;
        final ControllableMemoryDataStorageManager dsm;
        final RecordingWatermarkStore watermark;
        final AtomicReference<PersistentVectorStore> storeRef;

        TestEngine(IndexingServiceEngine engine,
                   ControllableMemoryDataStorageManager dsm,
                   RecordingWatermarkStore watermark,
                   AtomicReference<PersistentVectorStore> storeRef) {
            this.engine = engine;
            this.dsm = dsm;
            this.watermark = watermark;
            this.storeRef = storeRef;
        }

        @Override
        public void close() throws Exception {
            engine.close();
        }
    }

    /**
     * Brings the engine to the point where it has 1 PersistentVectorStore
     * containing 512 random vectors (one FusedPQ-eligible segment after the
     * upcoming checkpoint).
     */
    private LogSequenceNumber bootstrapWithVectors(TestEngine te, long seed) throws Exception {
        te.engine.start();
        Table table = buildTable();
        te.engine.applyEntry(new LogSequenceNumber(1, 1),
                LogEntryFactory.createTable(table, null));
        Index index = Index.builder()
                .name("vidx")
                .table("vectable")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
        te.engine.applyEntry(new LogSequenceNumber(1, 2),
                LogEntryFactory.createIndex(index, null));
        assertNotNull("store must have been created by DDL", te.storeRef.get());

        Random rng = new Random(seed);
        int dim = 32;
        long baseLsn = 100;
        LogSequenceNumber lastLsn = null;
        for (int i = 0; i < 512; i++) {
            Record record = RecordSerializer.makeRecord(table,
                    "pk", "k" + i,
                    "vec", randomVector(rng, dim));
            LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
            lastLsn = new LogSequenceNumber(1, baseLsn + i);
            te.engine.applySingleEntryForTest(lastLsn, insert);
        }
        te.engine.awaitPendingWorkForTest();
        te.engine.setLastProcessedLsnForTest(lastLsn);
        return lastLsn;
    }

    private Properties baseProps(boolean async, long warmupBytes) {
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_BYTES,
                String.valueOf(warmupBytes));
        props.setProperty(IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_ASYNC,
                String.valueOf(async));
        return props;
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * Async mode (default): after a force-checkpoint a non-null warmup
     * Future has been submitted, eventually completes, and bytes are read
     * through the storage manager.
     */
    @Test
    public void testAsyncWarmupSubmitsToSeparateExecutor() throws Exception {
        try (TestEngine te = newEngine(baseProps(true, 32L * 1024 * 1024))) {
            bootstrapWithVectors(te, 42);
            te.dsm.totalBytesRead.set(0);

            te.engine.forceCheckpointAndSaveWatermark();

            assertEquals("watermark must be saved", 1, te.watermark.getSaveCount());
            Future<?> warmup = te.engine.getLastWarmupFutureForTest();
            assertNotNull("async mode must publish a warmup Future", warmup);
            // forceCheckpointAndSaveWatermark awaits the warmup before
            // returning, so the Future must already be done.
            assertTrue("warmup must be done after forceCheckpointAndSaveWatermark",
                    warmup.isDone());
            assertTrue("warmup must have read bytes from the segment",
                    te.dsm.totalBytesRead.get() > 0);
        }
    }

    /**
     * Sync mode (opt-in): after a force-checkpoint no async warmup Future is
     * published — the warmup ran inline. Bytes are still read.
     */
    @Test
    public void testSyncWarmupRunsInline() throws Exception {
        try (TestEngine te = newEngine(baseProps(false, 32L * 1024 * 1024))) {
            bootstrapWithVectors(te, 43);
            te.dsm.totalBytesRead.set(0);

            te.engine.forceCheckpointAndSaveWatermark();

            assertEquals("watermark must be saved", 1, te.watermark.getSaveCount());
            assertNull("sync mode must NOT publish a warmup Future",
                    te.engine.getLastWarmupFutureForTest());
            assertTrue("inline warmup must have read bytes from the segment",
                    te.dsm.totalBytesRead.get() > 0);
        }
    }

    /**
     * Coalescing: while a warmup is paused at the test hook a second
     * checkpoint trigger does NOT pile up a second Future on the executor —
     * it sees the in-flight Future and skips the submit. Once the hook is
     * released the in-flight warmup completes; the next checkpoint after
     * that queues a fresh warmup with the latest segments.
     *
     * <p>Uses {@link IndexingServiceEngine#setWarmupPauseHookForTest} rather
     * than the read-gate so that Phase A / B / C reads (which run on the
     * checkpoint thread, not the warmup thread) are not blocked by the
     * pause — only the warmup itself is.
     */
    @Test
    public void testAsyncWarmupCoalescing() throws Exception {
        try (TestEngine te = newEngine(baseProps(true, 32L * 1024 * 1024))) {
            bootstrapWithVectors(te, 44);

            // Hook installed BEFORE the first checkpoint trigger so the
            // warmup task pauses at the very start of its run.
            CountDownLatch hookRelease = new CountDownLatch(1);
            te.engine.setWarmupPauseHookForTest(() -> {
                try {
                    if (!hookRelease.await(60, TimeUnit.SECONDS)) {
                        throw new RuntimeException("warmup pause hook timed out");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });

            // First checkpoint: Phase A → B → C runs unhindered, then the
            // warmup is dispatched and immediately blocks at the hook.
            te.engine.triggerCheckpointAsyncForTest();

            Future<?> firstCheckpoint = te.engine.getInflightCheckpointFutureForTest();
            assertNotNull(firstCheckpoint);
            firstCheckpoint.get(30, TimeUnit.SECONDS);
            assertEquals("first checkpoint must have saved the watermark",
                    1, te.watermark.getSaveCount());

            Future<?> firstWarmup = waitForWarmupFuture(te, 5_000);
            assertNotNull("first checkpoint must have submitted a warmup", firstWarmup);
            assertFalse("warmup is paused at the test hook and must still be in progress",
                    firstWarmup.isDone());

            // While the first warmup is still paused, fire another checkpoint.
            te.engine.triggerCheckpointAsyncForTest();
            Future<?> secondCheckpoint = te.engine.getInflightCheckpointFutureForTest();
            assertNotNull(secondCheckpoint);
            secondCheckpoint.get(30, TimeUnit.SECONDS);

            // Coalescing assertion: the warmup Future is the SAME instance as
            // before — the second submit was dropped because the previous
            // warmup was still running.
            Future<?> stillFirstWarmup = te.engine.getLastWarmupFutureForTest();
            assertTrue("second checkpoint must not pile up a new warmup while one is in progress",
                    stillFirstWarmup == firstWarmup);

            // Release the hook; the in-flight warmup now completes.
            hookRelease.countDown();
            firstWarmup.get(30, TimeUnit.SECONDS);
            assertTrue("first warmup must have completed after hook release",
                    firstWarmup.isDone());

            // Clear the hook so subsequent submits run uninterrupted.
            te.engine.setWarmupPauseHookForTest(null);
        }
    }

    /**
     * Shutdown: a paused-at-hook warmup does not prevent
     * {@link IndexingServiceEngine#close()} from completing within a sane
     * bound. The warmup executor is shut down cleanly via shutdownNow with
     * a 5 s awaitTermination cap; the daemon thread is then interrupted and
     * the close() returns.
     */
    @Test
    public void testAsyncWarmupCloseDoesNotHang() throws Exception {
        TestEngine te = newEngine(baseProps(true, 32L * 1024 * 1024));
        boolean closed = false;
        try {
            bootstrapWithVectors(te, 45);

            // Park the warmup indefinitely at the test hook. close() must
            // interrupt the daemon thread within the bounded window.
            CountDownLatch parkForever = new CountDownLatch(1);
            te.engine.setWarmupPauseHookForTest(() -> {
                try {
                    // Wait essentially forever; close() will interrupt this
                    // thread, propagating an InterruptedException that the
                    // hook converts to RuntimeException — which the engine
                    // catches and logs at WARNING.
                    parkForever.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });

            te.engine.triggerCheckpointAsyncForTest();
            // Wait until the warmup has been submitted (so close() actually
            // exercises the in-flight-shutdown path).
            assertNotNull(waitForWarmupFuture(te, 10_000));

            long t0 = System.currentTimeMillis();
            te.engine.close();
            closed = true;
            long elapsed = System.currentTimeMillis() - t0;
            // 30 s for the checkpoint executor + 5 s for the warmup executor
            // + a generous safety margin.
            assertTrue("engine.close() must complete in a bounded time, got " + elapsed + " ms",
                    elapsed < 45_000);
            // Sanity: cancellation flag gets set so the parked hook unblocks
            // (the daemon thread itself is gone after close() anyway).
            parkForever.countDown();
        } finally {
            if (!closed) {
                te.engine.close();
            }
        }
    }

    /**
     * Non-blocking ordering: with the warmup paused at the test hook, the
     * checkpoint Future completes (and the watermark is saved) while the
     * warmup is still in flight. Proves that the checkpoint thread is
     * decoupled from the warmup — the whole point of issue #472.
     */
    @Test
    public void testAsyncWarmupDoesNotBlockWatermarkSave() throws Exception {
        try (TestEngine te = newEngine(baseProps(true, 32L * 1024 * 1024))) {
            bootstrapWithVectors(te, 46);

            // Pause the warmup at its very first instruction. Phase A / B / C
            // run unhindered (they execute on the checkpoint thread, not the
            // warmup thread).
            CountDownLatch hookRelease = new CountDownLatch(1);
            te.engine.setWarmupPauseHookForTest(() -> {
                try {
                    if (!hookRelease.await(60, TimeUnit.SECONDS)) {
                        throw new RuntimeException("warmup pause hook timed out");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });

            te.engine.triggerCheckpointAsyncForTest();
            Future<?> checkpoint = te.engine.getInflightCheckpointFutureForTest();
            assertNotNull(checkpoint);
            // The checkpoint Future returns once the watermark is saved
            // (and the async warmup has been dispatched but is paused).
            checkpoint.get(30, TimeUnit.SECONDS);

            assertEquals("watermark must be saved as soon as checkpoint Future returns",
                    1, te.watermark.getSaveCount());

            Future<?> warmup = waitForWarmupFuture(te, 5_000);
            assertNotNull("warmup must have been submitted", warmup);
            // CRITICAL ASSERTION: the watermark is already saved (above) but
            // the warmup is NOT done — proving the watermark save did not
            // wait for the warmup to finish.
            assertFalse("warmup must still be running while watermark is saved",
                    warmup.isDone());

            // Release the hook; the warmup completes normally and reads
            // bytes through the storage manager.
            hookRelease.countDown();
            warmup.get(30, TimeUnit.SECONDS);
            assertTrue("warmup must have read bytes after release",
                    te.dsm.totalBytesRead.get() > 0);

            te.engine.setWarmupPauseHookForTest(null);
        }
    }

    /**
     * The system property
     * {@link IndexingServerConfiguration#SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_ASYNC}
     * is honoured as a default when the properties-file key is absent.
     * Setting it to {@code false} via system property must run the warmup
     * inline (no async Future published).
     */
    @Test
    public void testSystemPropertyDisablesAsyncMode() throws Exception {
        String originalValue = System.getProperty(
                IndexingServerConfiguration.SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_ASYNC);
        try {
            System.setProperty(
                    IndexingServerConfiguration.SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_ASYNC, "false");

            // Build properties WITHOUT the async key so the engine falls
            // back to the system property.
            Properties props = new Properties();
            props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
            props.setProperty(IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_BYTES,
                    String.valueOf(32L * 1024 * 1024));
            // Note: PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_ASYNC intentionally absent.

            try (TestEngine te = newEngine(props)) {
                bootstrapWithVectors(te, 47);
                te.dsm.totalBytesRead.set(0);
                te.engine.forceCheckpointAndSaveWatermark();
                assertEquals("watermark saved", 1, te.watermark.getSaveCount());
                assertNull("system property forced sync mode — no async Future expected",
                        te.engine.getLastWarmupFutureForTest());
                assertTrue("inline warmup must have read bytes",
                        te.dsm.totalBytesRead.get() > 0);
            }
        } finally {
            if (originalValue == null) {
                System.clearProperty(
                        IndexingServerConfiguration.SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_ASYNC);
            } else {
                System.setProperty(
                        IndexingServerConfiguration.SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_ASYNC,
                        originalValue);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Polling helpers
    // -------------------------------------------------------------------------

    /**
     * Polls {@link IndexingServiceEngine#getLastWarmupFutureForTest()} until
     * a non-null Future appears or {@code timeoutMs} elapses.
     */
    private static Future<?> waitForWarmupFuture(TestEngine te, long timeoutMs)
            throws InterruptedException, TimeoutException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Future<?> f = te.engine.getLastWarmupFutureForTest();
            if (f != null) {
                return f;
            }
            Thread.sleep(10);
        }
        fail("warmup Future never appeared within " + timeoutMs + " ms");
        // unreachable
        throw new TimeoutException();
    }
}
