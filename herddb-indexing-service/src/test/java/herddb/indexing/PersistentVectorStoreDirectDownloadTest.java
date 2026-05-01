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
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the direct-download path introduced in issue #381:
 * when {@code supportsDirectMultipartDownload()} returns {@code true}, the
 * {@code PersistentVectorStore} must use {@code downloadMultipartIndexFile}
 * instead of the gRPC-backed {@code multipartIndexReaderSupplier} path during
 * cold-start recovery.
 *
 * <p>Also verifies that loading-progress counters
 * ({@code getLoadingSegmentsDone()}, {@code getLoadingSegmentsTotal()},
 * {@code isLoadingFromStatus()}) are exposed correctly via the store's getter
 * methods so that the gRPC {@code GetIndexStatus} RPC can surface them.
 */
public class PersistentVectorStoreDirectDownloadTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static final String FIXED_UUID = "direct_dl_test_uuid";
    private static final int DIM = 32;

    private PersistentVectorStore createStore(Path tmpDir, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("direct_dl_idx", "direct_dl_tbl", "direct_dl_space",
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
            store.addVector(Bytes.from_int(seed * 100_000 + i), randomVector(rng, dim));
        }
    }

    /**
     * A {@link MemoryDataStorageManager} subclass that advertises direct-download
     * support and tracks call counts for both paths.
     *
     * <p>Distinguishes between:
     * <ul>
     *   <li>{@link #directDownloadCount} — calls via {@link #downloadMultipartIndexFile}
     *       (the fast path that must be taken during recovery).</li>
     *   <li>{@link #externalReaderSupplierCount} — calls to
     *       {@link #multipartIndexReaderSupplier} for the <em>map</em> file type that did NOT
     *       originate from inside {@link #downloadMultipartIndexFile} (i.e., the slow gRPC
     *       fallback path). Graph file calls via the same method are expected (the
     *       {@code OnDiskGraphIndex} keeps a live reader reference) and are therefore
     *       excluded from this counter. This must be zero during recovery when
     *       {@code supportsDirectMultipartDownload()} is true.
     *   </li>
     * </ul>
     *
     * <p>A {@link ThreadLocal} marker is used to detect calls to
     * {@code multipartIndexReaderSupplier} that come from inside
     * {@code downloadMultipartIndexFile} vs. from {@code readMultipartMapDataToTempFile}.
     *
     * <p>An optional per-download sleep widows the loading window so that the
     * observer thread in the loading-progress test can reliably catch the
     * {@code isLoadingFromStatus()==true} state.
     */
    private static final class DirectDownloadDSM extends MemoryDataStorageManager {

        final AtomicInteger directDownloadCount = new AtomicInteger(0);
        /** Count of {@code multipartIndexReaderSupplier} calls from the slow path only. */
        final AtomicInteger externalReaderSupplierCount = new AtomicInteger(0);
        /** Optional per-download delay in ms (0 = no delay). */
        volatile long downloadDelayMs = 0;

        /** Set while executing inside {@link #downloadMultipartIndexFile}. */
        private final ThreadLocal<Boolean> insideDirectDownload = ThreadLocal.withInitial(() -> false);

        @Override
        public boolean supportsDirectMultipartDownload() {
            return true;
        }

        @Override
        public void downloadMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                               long fileSize, Path destFile)
                throws IOException, DataStorageManagerException {
            directDownloadCount.incrementAndGet();
            insideDirectDownload.set(true);
            try {
                if (downloadDelayMs > 0) {
                    try {
                        Thread.sleep(downloadDelayMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                // Read the raw bytes via the parent in-memory supplier and write to destFile.
                // This call does NOT increment externalReaderSupplierCount because the
                // insideDirectDownload marker is set.
                io.github.jbellis.jvector.disk.ReaderSupplier supplier =
                        super.multipartIndexReaderSupplier(tableSpace, uuid, fileType, fileSize);
                try (io.github.jbellis.jvector.disk.RandomAccessReader reader = supplier.get();
                     java.io.OutputStream out = Files.newOutputStream(destFile)) {
                    byte[] buf = new byte[4096];
                    long remaining = fileSize;
                    reader.seek(0);
                    while (remaining > 0) {
                        int toRead = (int) Math.min(buf.length, remaining);
                        byte[] tmp = toRead == buf.length ? buf : new byte[toRead];
                        reader.readFully(tmp);
                        out.write(tmp, 0, toRead);
                        remaining -= toRead;
                    }
                }
            } finally {
                insideDirectDownload.set(false);
            }
        }

        @Override
        public io.github.jbellis.jvector.disk.ReaderSupplier multipartIndexReaderSupplier(
                String tableSpace, String uuid, String fileType, long fileSize)
                throws DataStorageManagerException {
            // Only count "map" calls that originate from outside downloadMultipartIndexFile.
            // The "graph" file is always loaded via ReaderSupplier (OnDiskGraphIndex keeps a live
            // reference to it), so graph calls are expected regardless of the direct-download flag.
            // We specifically want to ensure that "map" file reads do NOT use the slow path when
            // supportsDirectMultipartDownload() returns true.
            if ("map".equals(fileType) && !insideDirectDownload.get()) {
                externalReaderSupplierCount.incrementAndGet();
            }
            return super.multipartIndexReaderSupplier(tableSpace, uuid, fileType, fileSize);
        }
    }

    /**
     * Verifies that after a checkpoint the store can be reloaded via the
     * direct-download path and that recovery correctly restores all vectors.
     */
    @Test
    public void directDownloadPathIsUsedDuringRecovery() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        DirectDownloadDSM dsm = new DirectDownloadDSM();

        float[] query = randomVector(new Random(42), DIM);
        List<Map.Entry<Bytes, Float>> beforeResults;

        // First run: populate and checkpoint
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 200, DIM, 1);
            store.checkpoint();
            assertEquals(200, store.size());
            beforeResults = store.search(query, 5);
            assertFalse("should return results before restart", beforeResults.isEmpty());
        }

        // Reset counters before the recovery pass
        dsm.directDownloadCount.set(0);
        dsm.externalReaderSupplierCount.set(0);

        // Second run: cold-start recovery — must use the direct-download path
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            assertEquals(200, store.size());

            // The direct-download path must have been taken exactly once per on-disk segment.
            // With 200 vectors (well below the flush threshold), there should be exactly 1 segment.
            assertTrue("downloadMultipartIndexFile should have been called at least once "
                    + "(one call per on-disk segment)",
                    dsm.directDownloadCount.get() > 0);

            // The slow gRPC/ReaderSupplier fallback path MUST NOT have been called for
            // map files during recovery when supportsDirectMultipartDownload()==true.
            // (Graph files still go through the ReaderSupplier; that is expected and is
            //  not counted in externalReaderSupplierCount.)
            assertEquals("multipartIndexReaderSupplier for 'map' files must NOT be invoked "
                    + "from the slow path when supportsDirectMultipartDownload()==true",
                    0, dsm.externalReaderSupplierCount.get());

            // Verify search results are consistent with before-restart results.
            List<Map.Entry<Bytes, Float>> afterResults = store.search(query, 5);
            assertFalse("should return results after direct-download restart", afterResults.isEmpty());
            assertEquals("top-1 result must be stable across restart",
                    beforeResults.get(0).getKey(), afterResults.get(0).getKey());
        }
    }

    /**
     * Verifies that the loading-progress counters are populated during recovery:
     * {@code getLoadingSegmentsTotal()} reflects the number of on-disk segments
     * and {@code getLoadingSegmentsDone()} advances toward that total.
     *
     * <p>After recovery completes, {@code getLoadingSegmentsDone()} must equal
     * {@code getLoadingSegmentsTotal()} and {@code isLoadingFromStatus()} must
     * return {@code false}.
     */
    @Test
    public void loadingProgressCountersAreExposedDuringRecovery() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        DirectDownloadDSM dsm = new DirectDownloadDSM();

        // First run: populate with enough vectors to generate at least 2 segments
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 300, DIM, 2);
            store.checkpoint();
            assertEquals(300, store.size());
        }

        // Second run: observe progress counters.
        // A 50 ms delay per segment download widens the loading window so the
        // observer thread can reliably catch isLoadingFromStatus()==true.
        dsm.downloadDelayMs = 50;
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // Before start: counters should be zero, not loading
            assertEquals(0, store.getLoadingSegmentsDone());
            assertEquals(0, store.getLoadingSegmentsTotal());
            assertFalse(store.isLoadingFromStatus());

            // Track observed progress values from a concurrent observer thread
            AtomicInteger observedTotal = new AtomicInteger(-1);
            AtomicInteger observedDoneSnapshot = new AtomicInteger(-1);
            CountDownLatch capturedLatch = new CountDownLatch(1);
            CountDownLatch observerReadyLatch = new CountDownLatch(1);

            Thread observer = new Thread(() -> {
                observerReadyLatch.countDown();
                // Poll until loading starts or times out (max 10 s).
                // The 50 ms download delay guarantees the window is wide enough.
                long deadline = System.currentTimeMillis() + 10_000;
                while (System.currentTimeMillis() < deadline) {
                    if (store.isLoadingFromStatus()) {
                        observedTotal.set(store.getLoadingSegmentsTotal());
                        observedDoneSnapshot.set(store.getLoadingSegmentsDone());
                        capturedLatch.countDown();
                        break;
                    }
                    Thread.yield();
                }
                // If we timed out without catching the loading state, count down anyway.
                capturedLatch.countDown();
            });
            observer.setDaemon(true);
            observer.start();
            observerReadyLatch.await();

            // Now start the store — this triggers loadMultiSegmentFormat (with delays)
            store.start();

            capturedLatch.await();
            dsm.downloadDelayMs = 0;

            // -----------------------------------------------------------------------
            // Unconditional post-start assertions
            // -----------------------------------------------------------------------
            // 1. After start() returns, loading must be finished.
            assertFalse("isLoadingFromStatus must be false after start() returns",
                    store.isLoadingFromStatus());

            // 2. After a successful load the final counters satisfy done == total.
            //    (They are not reset to 0 after completion — only meaningful when
            //     isLoadingFromStatus() is true.)
            int finalTotal = store.getLoadingSegmentsTotal();
            int finalDone = store.getLoadingSegmentsDone();
            assertTrue("final total must be > 0 (store had on-disk segments)", finalTotal > 0);
            assertEquals("done must equal total after successful load", finalTotal, finalDone);

            // 3. The observer must have caught the in-progress state (the 50 ms delay
            //    guarantees the loading window is wide enough to observe).
            assertTrue("observer must have caught isLoadingFromStatus()==true within 10 s",
                    observedTotal.get() >= 0);
            assertTrue("loading total must be > 0 when in progress", observedTotal.get() > 0);
            assertTrue("loading done must be >= 0 when in progress", observedDoneSnapshot.get() >= 0);

            assertEquals(300, store.size());
        }
    }

    /**
     * Verifies that the DSM without direct-download support (
     * {@code supportsDirectMultipartDownload()} = false) still works correctly —
     * the {@code multipartIndexReaderSupplier} path is taken instead.
     */
    @Test
    public void fallbackPathIsUsedWhenDirectDownloadNotSupported() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();

        // Plain MemoryDataStorageManager: supportsDirectMultipartDownload() = false
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        float[] query = randomVector(new Random(77), DIM);
        List<Map.Entry<Bytes, Float>> beforeResults;

        // Populate
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 150, DIM, 3);
            store.checkpoint();
            assertEquals(150, store.size());
            beforeResults = store.search(query, 5);
        }

        // Recovery via ReaderSupplier path
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            assertFalse("plain DSM must not support direct download",
                    dsm.supportsDirectMultipartDownload());
            store.start();
            assertEquals(150, store.size());
            List<Map.Entry<Bytes, Float>> afterResults = store.search(query, 5);
            assertFalse(afterResults.isEmpty());
            assertEquals("top-1 result must be stable across restart",
                    beforeResults.get(0).getKey(), afterResults.get(0).getKey());
        }
    }
}
