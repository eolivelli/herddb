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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that multipart-mode segments (used with remote file storage) can be
 * checkpointed, recovered after restart, and re-checkpointed correctly.
 *
 * <p>Uses a {@link MultipartMemoryDSM} that stores multipart files in memory,
 * enabling full multipart code-path testing without a real remote file server.
 */
public class PersistentVectorStoreMultipartRecoveryTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static final String FIXED_UUID = "multipart_test_fixed";
    private static final int DIM = 32;

    /**
     * In-memory DSM that supports multipart writes/reads, forcing PersistentVectorStore
     * to use the multipart code path instead of the page-based fallback.
     */
    private static class MultipartMemoryDSM extends MemoryDataStorageManager {
        final ConcurrentHashMap<String, byte[]> multipartFiles = new ConcurrentHashMap<>();

        @Override
        public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                              Path tempFile)
                throws IOException, DataStorageManagerException {
            String logicalPath = tableSpace + "/" + uuid + "/" + fileType;
            byte[] data = Files.readAllBytes(tempFile);
            multipartFiles.put(logicalPath, data);
            return logicalPath;
        }

        @Override
        public ReaderSupplier multipartIndexReaderSupplier(
                String tableSpace, String uuid, String fileType, long fileSize)
                throws DataStorageManagerException {
            String logicalPath = tableSpace + "/" + uuid + "/" + fileType;
            byte[] data = multipartFiles.get(logicalPath);
            if (data == null) {
                throw new DataStorageManagerException("multipart file not found: " + logicalPath);
            }
            return new InMemoryReaderSupplier(data);
        }
    }

    /**
     * A {@link ReaderSupplier} backed by a byte array in memory.
     */
    private static final class InMemoryReaderSupplier implements ReaderSupplier {
        private final byte[] data;

        InMemoryReaderSupplier(byte[] data) {
            this.data = data;
        }

        @Override
        public RandomAccessReader get() {
            return new InMemoryRandomAccessReader(data);
        }

        @Override
        public void close() {
            // nothing to close
        }
    }

    /**
     * A simple {@link RandomAccessReader} over a byte array.
     */
    private static final class InMemoryRandomAccessReader implements RandomAccessReader {
        private final byte[] data;
        private int position;

        InMemoryRandomAccessReader(byte[] data) {
            this.data = data;
        }

        @Override
        public void seek(long offset) {
            this.position = (int) offset;
        }

        @Override
        public long getPosition() {
            return position;
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public int readInt() throws IOException {
            if (position + 4 > data.length) {
                throw new IOException("read past end");
            }
            int v = ((data[position] & 0xFF) << 24)
                    | ((data[position + 1] & 0xFF) << 16)
                    | ((data[position + 2] & 0xFF) << 8)
                    | (data[position + 3] & 0xFF);
            position += 4;
            return v;
        }

        @Override
        public float readFloat() throws IOException {
            return Float.intBitsToFloat(readInt());
        }

        @Override
        public long readLong() throws IOException {
            if (position + 8 > data.length) {
                throw new IOException("read past end");
            }
            long v = 0;
            for (int i = 0; i < 8; i++) {
                v = (v << 8) | (data[position + i] & 0xFF);
            }
            position += 8;
            return v;
        }

        @Override
        public void readFully(byte[] dest) throws IOException {
            if (position + dest.length > data.length) {
                throw new IOException("read past end: position=" + position
                        + " len=" + dest.length + " total=" + data.length);
            }
            System.arraycopy(data, position, dest, 0, dest.length);
            position += dest.length;
        }

        @Override
        public void readFully(ByteBuffer buffer) throws IOException {
            byte[] tmp = new byte[buffer.remaining()];
            readFully(tmp);
            buffer.put(tmp);
        }

        @Override
        public void readFully(long[] vector) throws IOException {
            for (int i = 0; i < vector.length; i++) {
                vector[i] = readLong();
            }
        }

        @Override
        public void read(int[] ints, int offset, int count) throws IOException {
            for (int i = 0; i < count; i++) {
                ints[offset + i] = readInt();
            }
        }

        @Override
        public void read(float[] floats, int offset, int count) throws IOException {
            byte[] buf = new byte[count * 4];
            readFully(buf);
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.BIG_ENDIAN);
            for (int i = 0; i < count; i++) {
                floats[offset + i] = bb.getFloat();
            }
        }

        @Override
        public void close() {
            // nothing
        }
    }

    private PersistentVectorStore createStore(Path tmpDir, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("mpidx", "mptable", "mpspace",
                "vector_col", FIXED_UUID, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE / 2);
    }

    private static float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private static void addVectors(PersistentVectorStore store, int count, int dim, int seed) {
        Random rng = new Random(seed);
        for (int i = 0; i < count; i++) {
            store.addVector(Bytes.from_int(seed * 100000 + i), randomVector(rng, dim));
        }
    }

    @Test
    public void multipartCheckpointAndSearchWorks() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MultipartMemoryDSM dsm = new MultipartMemoryDSM();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 500, DIM, 1);
            store.checkpoint();

            assertEquals(500, store.size());
            float[] query = randomVector(new Random(99), DIM);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 10);
            assertFalse("search should return results after multipart checkpoint",
                    results.isEmpty());
            assertEquals(10, results.size());
        }

        // Verify multipart files were actually written
        assertFalse("DSM should contain multipart files", dsm.multipartFiles.isEmpty());
    }

    @Test
    public void multipartRestartRecoveryWorks() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MultipartMemoryDSM dsm = new MultipartMemoryDSM();
        float[] query = randomVector(new Random(42), DIM);
        List<Map.Entry<Bytes, Float>> before;

        // First run: add vectors and checkpoint
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 500, DIM, 2);
            store.checkpoint();
            before = store.search(query, 5);
            assertEquals(500, store.size());
        }

        // Reopen with same DSM (simulates pod restart with same PVC)
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            assertEquals(500, store.size());
            List<Map.Entry<Bytes, Float>> after = store.search(query, 5);
            assertFalse(after.isEmpty());
            assertEquals("top-1 result must survive restart",
                    before.get(0).getKey(), after.get(0).getKey());
        }
    }

    @Test
    public void multipartCheckpointAfterRecoveryWorks() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MultipartMemoryDSM dsm = new MultipartMemoryDSM();

        // First run
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 500, DIM, 3);
            store.checkpoint();
        }

        // Second run: recover, add more vectors, checkpoint again
        float[] query = randomVector(new Random(77), DIM);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            assertEquals(500, store.size());

            addVectors(store, 300, DIM, 4);
            store.checkpoint();

            assertEquals(800, store.size());
            List<Map.Entry<Bytes, Float>> results = store.search(query, 10);
            assertFalse(results.isEmpty());
            assertEquals(10, results.size());
        }

        // Third run: verify everything persisted correctly
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            assertEquals(800, store.size());
        }
    }

    /**
     * A multipart DSM that can be armed to fail the next write.
     */
    private static final class FailableMultipartDSM extends MultipartMemoryDSM {
        final AtomicBoolean failNext = new AtomicBoolean(false);

        @Override
        public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                              Path tempFile)
                throws IOException, DataStorageManagerException {
            if (failNext.compareAndSet(true, false)) {
                throw new DataStorageManagerException("injected multipart write failure");
            }
            return super.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile);
        }
    }

    @Test
    public void multipartPhaseBFailureRecovery() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        FailableMultipartDSM dsm = new FailableMultipartDSM();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 500, DIM, 5);

            // Arm failure for next multipart write
            dsm.failNext.set(true);

            // Checkpoint should fail but recover gracefully
            try {
                store.checkpoint();
                // If it succeeds, the failure was on a non-critical path or was retried
            } catch (Exception e) {
                // Expected: Phase B failure
                assertTrue("should be storage exception",
                        e instanceof DataStorageManagerException
                        || e.getCause() instanceof DataStorageManagerException);
            }

            // Store should still be usable after failure recovery
            assertEquals(500, store.size());
            List<Map.Entry<Bytes, Float>> results = store.search(
                    randomVector(new Random(55), DIM), 5);
            assertFalse("search should work after failed checkpoint", results.isEmpty());

            // A subsequent checkpoint should succeed
            store.checkpoint();
            assertEquals(500, store.size());
        }
    }

    @Test
    public void pageBasedGuardStillWorks() throws Exception {
        // A DSM that returns null from multipart write (not supported) AND also
        // fails page writes, so both paths leave graphPageIds empty.
        MemoryDataStorageManager brokenDsm = new MemoryDataStorageManager() {
            @Override
            public void writeIndexPage(String tableSpace, String indexName,
                                       long pageId, DataWriter writer)
                    throws DataStorageManagerException {
                throw new DataStorageManagerException("injected page write failure");
            }
        };

        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        try (PersistentVectorStore store = new PersistentVectorStore(
                "brkidx", "brktable", "brkspace", "vector_col",
                "broken_fixed", tmpDir, brokenDsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE / 2)) {
            store.start();
            addVectors(store, 500, DIM, 6);

            try {
                store.checkpoint();
                fail("checkpoint should fail when both multipart and page-based writes fail");
            } catch (DataStorageManagerException e) {
                // Expected: page write failure (multipart returned null, fell back to pages, pages failed)
            }
        }
    }
}
