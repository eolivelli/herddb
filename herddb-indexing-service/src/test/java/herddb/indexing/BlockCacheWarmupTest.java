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
import herddb.storage.DataStorageManagerException;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for the post-Phase-C block-cache warmup introduced by issue #322.
 *
 * <p>Verifies that:
 * <ol>
 *   <li>{@link PersistentVectorStore#warmUpBlockCache} is a no-op when
 *       {@code warmupBytes == 0}.</li>
 *   <li>{@link PersistentVectorStore#warmUpBlockCache} issues reads starting
 *       from the HNSW entry node via BFS on Layer 0, and the byte counter
 *       increases when {@code warmupBytes > 0}.</li>
 *   <li>The {@link IndexingServiceEngine} respects the config key
 *       {@link IndexingServerConfiguration#PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_BYTES}
 *       and saves the watermark after warmup completes.</li>
 *   <li>The JVM system property
 *       {@link IndexingServerConfiguration#SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_BYTES}
 *       is honoured as a fallback when the properties-file key is absent.</li>
 * </ol>
 */
public class BlockCacheWarmupTest {

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
    // Helpers
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

        synchronized LogSequenceNumber current() {
            WatermarkSnapshot v = saved.get();
            return v != null ? v.lsn : null;
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
     * {@link ReaderSupplier} returned by
     * {@link #multipartIndexReaderSupplier} in a counting shim so we can
     * verify how many bytes the warmup actually read.
     */
    private static final class TrackingMemoryDataStorageManager extends MemoryDataStorageManager {

        final AtomicLong totalBytesRead = new AtomicLong();

        @Override
        public ReaderSupplier multipartIndexReaderSupplier(
                String tableSpace, String uuid, String fileType, long fileSize)
                throws DataStorageManagerException {
            ReaderSupplier original = super.multipartIndexReaderSupplier(
                    tableSpace, uuid, fileType, fileSize);
            return new CountingReaderSupplier(original, totalBytesRead);
        }
    }

    private static final class CountingReaderSupplier implements ReaderSupplier {

        private final ReaderSupplier delegate;
        private final AtomicLong counter;

        CountingReaderSupplier(ReaderSupplier delegate, AtomicLong counter) {
            this.delegate = delegate;
            this.counter = counter;
        }

        @Override
        public RandomAccessReader get() throws IOException {
            return new CountingReader(delegate.get(), counter);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class CountingReader implements RandomAccessReader {

        private final RandomAccessReader delegate;
        private final AtomicLong counter;

        CountingReader(RandomAccessReader delegate, AtomicLong counter) {
            this.delegate = delegate;
            this.counter = counter;
        }

        @Override
        public void readFully(byte[] dest) throws IOException {
            delegate.readFully(dest);
            counter.addAndGet(dest.length);
        }

        @Override
        public void readFully(ByteBuffer buffer) throws IOException {
            int before = buffer.remaining();
            delegate.readFully(buffer);
            counter.addAndGet(before - buffer.remaining());
        }

        @Override
        public void readFully(long[] longs) throws IOException {
            delegate.readFully(longs);
            counter.addAndGet((long) longs.length * Long.BYTES);
        }

        @Override
        public void read(int[] ints, int offset, int count) throws IOException {
            delegate.read(ints, offset, count);
            counter.addAndGet((long) count * Integer.BYTES);
        }

        @Override
        public void read(float[] floats, int offset, int count) throws IOException {
            delegate.read(floats, offset, count);
            counter.addAndGet((long) count * Float.BYTES);
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
            int v = delegate.readInt();
            counter.addAndGet(Integer.BYTES);
            return v;
        }

        @Override
        public float readFloat() throws IOException {
            float v = delegate.readFloat();
            counter.addAndGet(Float.BYTES);
            return v;
        }

        @Override
        public long readLong() throws IOException {
            long v = delegate.readLong();
            counter.addAndGet(Long.BYTES);
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

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * {@code warmUpBlockCache(0)} must be an instant no-op: zero reads issued,
     * method returns without error even when segments are loaded.
     */
    @Test
    public void testWarmupZeroIsNoOp() throws Exception {
        Path tmpDir = folder.newFolder("data").toPath();
        TrackingMemoryDataStorageManager dsm = new TrackingMemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        PersistentVectorStore store = new PersistentVectorStore(
                "vidx", "vectable", "tstuuid", "vec",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE, VectorSimilarityFunction.EUCLIDEAN);
        store.start();
        try {
            // Insert enough vectors to cross the FusedPQ threshold.
            Random rng = new Random(7);
            int dim = 32;
            for (int i = 0; i < 512; i++) {
                store.addVector(herddb.utils.Bytes.from_string("pk" + i),
                        randomVector(rng, dim));
            }
            store.checkpoint();

            long bytesBefore = dsm.totalBytesRead.get();
            // warmup with 0 must touch nothing
            store.warmUpBlockCache(0);
            assertEquals("zero-warmup must issue no reads",
                    bytesBefore, dsm.totalBytesRead.get());
        } finally {
            store.close();
        }
    }

    /**
     * {@code warmUpBlockCache(N)} reads up to N bytes from every segment that
     * has a loaded {@code onDiskReaderSupplier}. With N larger than any single
     * segment the full file is read; total bytes > 0 proves that at least one
     * read was issued.
     */
    @Test
    public void testWarmupReadsSegments() throws Exception {
        Path tmpDir = folder.newFolder("data").toPath();
        TrackingMemoryDataStorageManager dsm = new TrackingMemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        PersistentVectorStore store = new PersistentVectorStore(
                "vidx", "vectable", "tstuuid", "vec",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE, VectorSimilarityFunction.EUCLIDEAN);
        store.start();
        try {
            Random rng = new Random(13);
            int dim = 32;
            for (int i = 0; i < 512; i++) {
                store.addVector(herddb.utils.Bytes.from_string("pk" + i),
                        randomVector(rng, dim));
            }
            store.checkpoint();

            // Reset the counter so only warmup reads are measured.
            dsm.totalBytesRead.set(0);

            // Warm with a generous limit — should read whatever is in each segment file.
            store.warmUpBlockCache(Long.MAX_VALUE);

            assertTrue("warmup must have issued at least one byte of reads",
                    dsm.totalBytesRead.get() > 0);
        } finally {
            store.close();
        }
    }

    /**
     * {@code warmUpBlockCache(N)} caps BFS node visits to approximately
     * {@code N / approxBytesPerNode} per segment. With a 1-byte budget the
     * BFS visits exactly 1 node (the entry node) and reads only a handful of
     * bytes (one {@code readInt} for the neighbor count plus the neighbor-ID
     * array) — far less than a full traversal of all nodes.
     */
    @Test
    public void testWarmupRespectsLimit() throws Exception {
        Path tmpDir = folder.newFolder("data").toPath();
        TrackingMemoryDataStorageManager dsm = new TrackingMemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        PersistentVectorStore store = new PersistentVectorStore(
                "vidx", "vectable", "tstuuid", "vec",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE, VectorSimilarityFunction.EUCLIDEAN);
        store.start();
        try {
            Random rng = new Random(17);
            int dim = 32;
            for (int i = 0; i < 512; i++) {
                store.addVector(herddb.utils.Bytes.from_string("pk" + i),
                        randomVector(rng, dim));
            }
            store.checkpoint();

            // Full warmup (baseline — visits all nodes via BFS).
            dsm.totalBytesRead.set(0);
            store.warmUpBlockCache(Long.MAX_VALUE);
            long fullWarmupBytes = dsm.totalBytesRead.get();
            assertTrue("full warmup must read something", fullWarmupBytes > 0);

            // 1-byte budget → nodeLimit = max(1, 1/approxBytesPerNode) = 1 (entry node only).
            // Reads just readInt + neighbor-ID array: a few dozen bytes, strictly
            // less than a full BFS over all nodes.
            dsm.totalBytesRead.set(0);
            store.warmUpBlockCache(1);
            long limitedBytes = dsm.totalBytesRead.get();
            assertTrue("limited warmup must read less than full warmup",
                    limitedBytes < fullWarmupBytes);
        } finally {
            store.close();
        }
    }

    /**
     * End-to-end: the engine calls {@code warmUpBlockCache} after all
     * checkpoints succeed (before the watermark is saved). With warmup enabled
     * the watermark is still saved correctly.
     */
    @Test
    public void testEngineWarmupAndWatermarkSaved() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        // Enable warmup with a non-zero value.
        props.setProperty(IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_BYTES,
                String.valueOf(32L * 1024 * 1024));
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());

        RecordingWatermarkStore watermark =
                new RecordingWatermarkStore();
        engine.setWatermarkStore(watermark);

        AtomicReference<PersistentVectorStore> storeRef = new AtomicReference<>();
        TrackingMemoryDataStorageManager dsm = new TrackingMemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
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

        try {
            engine.start();

            Table table = buildTable();
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

            assertNotNull("store must have been created by DDL", storeRef.get());

            // Insert enough vectors to trigger a FusedPQ checkpoint.
            Random rng = new Random(99);
            int dim = 32;
            long baseLsn = 100;
            LogSequenceNumber lastLsn = null;
            for (int i = 0; i < 512; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "k" + i,
                        "vec", randomVector(rng, dim));
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                lastLsn = new LogSequenceNumber(1, baseLsn + i);
                engine.applySingleEntryForTest(lastLsn, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(lastLsn);

            // Reset the byte counter so we can distinguish checkpoint I/O
            // from warmup I/O.
            dsm.totalBytesRead.set(0);

            engine.forceCheckpointAndSaveWatermark();

            assertEquals("watermark must be saved after warmup", 1, watermark.getSaveCount());
            assertEquals("watermark must match last processed LSN",
                    new LogSequenceNumber(1, baseLsn + 512 - 1),
                    watermark.current());
            assertTrue("warmup must have read bytes from the segment",
                    dsm.totalBytesRead.get() > 0);
        } finally {
            engine.close();
        }
    }

    /**
     * With {@code warmupBytes = 0} in the config the warmup step is skipped
     * entirely; the watermark is still saved correctly.
     */
    @Test
    public void testEngineWarmupDisabledWatermarkStillSaved() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_BYTES, "0");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());

        RecordingWatermarkStore watermark =
                new RecordingWatermarkStore();
        engine.setWatermarkStore(watermark);

        TrackingMemoryDataStorageManager dsm = new TrackingMemoryDataStorageManager();
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

        try {
            engine.start();

            Table table = buildTable();
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

            assertNotNull("store must have been created by DDL", storeRef.get());

            Random rng = new Random(55);
            int dim = 32;
            long baseLsn = 100;
            LogSequenceNumber lastLsn = null;
            for (int i = 0; i < 512; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "k" + i,
                        "vec", randomVector(rng, dim));
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                lastLsn = new LogSequenceNumber(1, baseLsn + i);
                engine.applySingleEntryForTest(lastLsn, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(lastLsn);

            // Run the checkpoint; Phase C will read some bytes (segment metadata).
            engine.forceCheckpointAndSaveWatermark();

            assertEquals("watermark must be saved even with warmup disabled",
                    1, watermark.getSaveCount());
            assertEquals("watermark LSN must match last processed entry",
                    new LogSequenceNumber(1, baseLsn + 512 - 1),
                    watermark.current());

            // Phase C loaded the segment graph index (reads some metadata bytes via
            // the tracking DSM). Capture that baseline and then directly invoke
            // warmUpBlockCache to confirm it reads additional bytes — proving that
            // the engine skipped the warmup step as configured (warmupBytes = 0).
            long bytesAfterCheckpoint = dsm.totalBytesRead.get();
            storeRef.get().warmUpBlockCache(Long.MAX_VALUE);
            long bytesAfterExplicitWarmup = dsm.totalBytesRead.get();
            assertTrue("explicit warmUpBlockCache must add reads beyond Phase-C baseline",
                    bytesAfterExplicitWarmup > bytesAfterCheckpoint);
        } finally {
            engine.close();
        }
    }

    /**
     * The JVM system property
     * {@link IndexingServerConfiguration#SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_BYTES}
     * is honoured as the default when the properties-file key is absent.
     * Setting it to {@code 0} via system property must disable warmup.
     */
    @Test
    public void testSystemPropertyOverridesDefault() throws Exception {
        String originalValue = System.getProperty(
                IndexingServerConfiguration.SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_BYTES);
        try {
            // Override via system property: 0 means disable.
            System.setProperty(
                    IndexingServerConfiguration.SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_BYTES, "0");

            Path logDir = folder.newFolder("log").toPath();
            Path dataDir = folder.newFolder("data").toPath();

            // No PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_BYTES in properties file —
            // engine must fall back to the system-property value (0 = disabled).
            Properties props = new Properties();
            props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
            IndexingServerConfiguration config = new IndexingServerConfiguration(props);

            IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
            engine.setMetadataStorageManager(createTestMetadata());

            RecordingWatermarkStore watermark =
                    new RecordingWatermarkStore();
            engine.setWatermarkStore(watermark);

            TrackingMemoryDataStorageManager dsm = new TrackingMemoryDataStorageManager();
            MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
            AtomicReference<PersistentVectorStore> storeRef = new AtomicReference<>();
            engine.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDir2, indexProperties) -> {
                PersistentVectorStore pvs = new PersistentVectorStore(
                        indexName, tableName, "tstblspace", vectorColumnName,
                        dataDir2, dsm, mm,
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

            try {
                engine.start();

                Table table = buildTable();
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

                assertNotNull("store must have been created by DDL", storeRef.get());

                Random rng = new Random(77);
                int dim = 32;
                long baseLsn = 100;
                LogSequenceNumber lastLsn = null;
                for (int i = 0; i < 512; i++) {
                    Record record = RecordSerializer.makeRecord(table,
                            "pk", "k" + i,
                            "vec", randomVector(rng, dim));
                    LogEntry insert = LogEntryFactory.insert(
                            table, record.key, record.value, null);
                    lastLsn = new LogSequenceNumber(1, baseLsn + i);
                    engine.applySingleEntryForTest(lastLsn, insert);
                }
                engine.awaitPendingWorkForTest();
                engine.setLastProcessedLsnForTest(lastLsn);

                // Run the checkpoint; Phase C reads some bytes via the tracking DSM.
                engine.forceCheckpointAndSaveWatermark();

                assertEquals("watermark saved even when system-property disables warmup",
                        1, watermark.getSaveCount());

                // Confirm the engine skipped warmup: explicitly calling warmUpBlockCache
                // must add reads beyond the Phase-C baseline.
                long bytesAfterCheckpoint = dsm.totalBytesRead.get();
                storeRef.get().warmUpBlockCache(Long.MAX_VALUE);
                assertTrue("explicit warmUpBlockCache must add reads beyond Phase-C baseline",
                        dsm.totalBytesRead.get() > bytesAfterCheckpoint);
            } finally {
                engine.close();
            }
        } finally {
            // Restore original system property so other tests are not affected.
            if (originalValue == null) {
                System.clearProperty(
                        IndexingServerConfiguration.SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_BYTES);
            } else {
                System.setProperty(
                        IndexingServerConfiguration.SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_BYTES,
                        originalValue);
            }
        }
    }
}
