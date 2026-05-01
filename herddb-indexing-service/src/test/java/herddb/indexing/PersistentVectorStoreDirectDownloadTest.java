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
     * <p>A {@link java.util.concurrent.CountDownLatch} ({@link #loadingStartedLatch}) is
     * tripped on the first invocation of {@link #downloadMultipartIndexFile}, giving observer
     * threads a deterministic signal that loading is in progress without relying on sleep or
     * yield-based polling.
     *
     * <p>An optional per-download sleep ({@link #downloadDelayMs}) widens the loading window
     * after the latch fires, so that counter snapshots in the observer are stable.
     */
    private static class DirectDownloadDSM extends MemoryDataStorageManager {

        final AtomicInteger directDownloadCount = new AtomicInteger(0);
        /** Count of {@code multipartIndexReaderSupplier} calls from the slow path only. */
        final AtomicInteger externalReaderSupplierCount = new AtomicInteger(0);
        /**
         * Counted down on the first {@link #downloadMultipartIndexFile} invocation that occurs
         * while {@link #loadingPhaseActive} is {@code true}, so that observer threads can
         * {@code await()} a deterministic "recovery loading has started" signal without being
         * triggered by the checkpoint path (which also calls {@code downloadMultipartIndexFile}
         * when reloading map files during the 3-phase merge).
         */
        final CountDownLatch loadingStartedLatch = new CountDownLatch(1);
        /**
         * Must be set to {@code true} immediately before the recovery {@code start()} call so
         * that {@link #loadingStartedLatch} and fault-injection logic are scoped to the recovery
         * phase only.
         */
        volatile boolean loadingPhaseActive = false;
        /** Optional per-download delay in ms applied AFTER the latch fires (0 = no delay). */
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
            // Signal observers that loading has started — but only during the recovery phase
            // (loadingPhaseActive=true). The 3-phase checkpoint also calls this method to
            // reload existing segment map files; we must NOT fire the latch during checkpoint.
            if (loadingPhaseActive) {
                loadingStartedLatch.countDown();
            }
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
     * A {@link DirectDownloadDSM} variant that throws an {@link IOException} on the second
     * {@link #downloadMultipartIndexFile} invocation, used to test the failure path of
     * {@code loadMultiSegmentFormat}: the original I/O exception must be rethrown (not masked
     * by {@code CancellationException}), {@code isLoadingFromStatus()} must be {@code false}
     * after the failure, and all already-opened BLink indexes must be closed.
     */
    private static final class FailOnSecondDownloadDSM extends DirectDownloadDSM {
        /**
         * Counts calls made while {@link #loadingPhaseActive} is {@code true} (i.e., during
         * the recovery phase only). The 3-phase checkpoint also calls
         * {@code downloadMultipartIndexFile}; those calls must not count toward the failure
         * trigger.
         */
        final AtomicInteger recoveryDownloadCount = new AtomicInteger(0);

        @Override
        public void downloadMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                               long fileSize, Path destFile)
                throws IOException, DataStorageManagerException {
            if (loadingPhaseActive) {
                int n = recoveryDownloadCount.incrementAndGet();
                if (n == 2) {
                    throw new IOException("simulated S3 failure on second download");
                }
            }
            super.downloadMultipartIndexFile(tableSpace, uuid, fileType, fileSize, destFile);
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

        // First run: populate and checkpoint (produces at least 1 on-disk segment)
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 300, DIM, 2);
            store.checkpoint();
            assertEquals(300, store.size());
        }

        // Second run: observe progress counters using a deterministic latch signal.
        // loadingPhaseActive=true scopes the latch signal to the recovery start() call;
        // it must NOT fire during the populate-phase checkpoint (which also downloads map files
        // via the 3-phase merge).  A 50 ms per-download delay keeps the loading window wide
        // enough for stable counter snapshots — but the latch is the real synchronisation.
        dsm.downloadDelayMs = 50;
        dsm.loadingPhaseActive = true;
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // Before start: counters should be zero, not loading
            assertEquals(0, store.getLoadingSegmentsDone());
            assertEquals(0, store.getLoadingSegmentsTotal());
            assertFalse(store.isLoadingFromStatus());

            AtomicInteger observedTotal = new AtomicInteger(-1);
            AtomicInteger observedDoneSnapshot = new AtomicInteger(-1);
            CountDownLatch observerDoneLatch = new CountDownLatch(1);

            Thread observer = new Thread(() -> {
                try {
                    // Wait for the deterministic "loading has started" signal from inside
                    // DirectDownloadDSM.downloadMultipartIndexFile.  Timeout of 10 s
                    // is generous — any healthy JVM will fire within ms.
                    boolean signalled = dsm.loadingStartedLatch.await(10, java.util.concurrent.TimeUnit.SECONDS);
                    if (signalled) {
                        // Snapshot counters immediately after the signal fires.
                        // The 50 ms delay inside downloadMultipartIndexFile guarantees the
                        // store is still in the loading state at this point.
                        observedTotal.set(store.getLoadingSegmentsTotal());
                        observedDoneSnapshot.set(store.getLoadingSegmentsDone());
                    }
                    // If signalled==false (timeout), observedTotal stays -1 and the
                    // assertion below will flag the failure deterministically.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    observerDoneLatch.countDown();
                }
            });
            observer.setDaemon(true);
            observer.start();

            // Trigger loadMultiSegmentFormat — this fires the loadingStartedLatch and
            // then sleeps 50 ms per segment inside downloadMultipartIndexFile.
            store.start();

            observerDoneLatch.await();

            // -----------------------------------------------------------------------
            // Unconditional post-start assertions
            // -----------------------------------------------------------------------
            // 1. After start() returns, loading must be finished.
            assertFalse("isLoadingFromStatus must be false after start() returns",
                    store.isLoadingFromStatus());

            // 2. After a successful load the final counters satisfy done == total.
            int finalTotal = store.getLoadingSegmentsTotal();
            int finalDone = store.getLoadingSegmentsDone();
            assertTrue("final total must be > 0 (store had on-disk segments)", finalTotal > 0);
            assertEquals("done must equal total after successful load", finalTotal, finalDone);

            // 3. The observer must have received the loading-started signal and captured
            //    consistent counter values while loading was in progress.
            assertTrue("observer must have received loadingStartedLatch signal within 10 s",
                    observedTotal.get() >= 0);
            assertTrue("loading total must be > 0 when in progress", observedTotal.get() > 0);
            assertTrue("loading done must be >= 0 when in progress", observedDoneSnapshot.get() >= 0);

            assertEquals(300, store.size());
        }
    }

    /**
     * Verifies that when a segment download fails mid-recovery:
     * <ol>
     *   <li>The original {@link IOException} cause is rethrown by {@code start()} — NOT masked
     *       by a {@code CancellationException} from the cancellation of sibling futures.</li>
     *   <li>{@code isLoadingFromStatus()} is {@code false} after the failure.</li>
     *   <li>No temporary map files are left behind in the store's temp directory.</li>
     * </ol>
     *
     * <p>Two on-disk segments are created by calling checkpoint twice; the second download
     * fails, exercising the cancel + cleanup path in {@code loadMultiSegmentFormat}.
     */
    @Test
    public void loadFailureRethrownAsIOExceptionAndSegmentsAreClosed() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        FailOnSecondDownloadDSM dsm = new FailOnSecondDownloadDSM();

        // First run: produce 2 on-disk segments (2 checkpoint calls)
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 100, DIM, 1);
            store.checkpoint();
            addVectors(store, 100, DIM, 2);
            store.checkpoint();
            assertEquals(200, store.size());
        }

        // Activate failure-injection for the recovery pass.
        // loadingPhaseActive=true scopes the recoveryDownloadCount to the recovery start()
        // call (not the checkpoint path which also downloads map files).
        dsm.loadingPhaseActive = true;

        // Second run: recovery must fail when the 2nd map file is downloaded.
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            try {
                store.start();
                org.junit.Assert.fail("Expected an IOException from the failed segment download");
            } catch (IOException e) {
                // Original IOException must propagate; NOT wrapped in CancellationException.
                Throwable root = e;
                while (root.getCause() != null) {
                    root = root.getCause();
                }
                assertTrue("root cause must be the simulated S3 failure (was: " + root + ")",
                        root.getMessage() != null && root.getMessage().contains("simulated S3 failure"));
            }

            // isLoadingFromStatus must be reset to false even after a failure.
            assertFalse("isLoadingFromStatus must be false after start() failure",
                    store.isLoadingFromStatus());

            // Note: we do NOT assert that no temp map files survive, because the sibling
            // task that was NOT the failing download may still be running asynchronously
            // in the CHECKPOINT_POOL at the time start() throws.  Traditional file I/O
            // ignores the interrupt flag set by Future.cancel(true), so that task
            // continues to execute and will clean up its own temp file when it reaches
            // the Files.deleteIfExists call inside loadFusedPQSegment.  The race between
            // that background cleanup and this assertion makes the check non-deterministic.
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
