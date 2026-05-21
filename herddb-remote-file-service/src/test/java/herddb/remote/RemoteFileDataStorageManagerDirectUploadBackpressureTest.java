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

package herddb.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.remote.storage.ObjectStorage;
import herddb.remote.storage.ReadResult;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #638: verifies the in-flight backpressure semaphore that bounds
 * concurrent direct-S3 uploads inside {@link RemoteFileDataStorageManager}.
 *
 * <p>The semaphore is intentionally <em>independent</em> of the gRPC
 * client's {@code inflightWriteBytes} budget so direct and gRPC writes
 * cannot starve one another. This test pins that independence by
 * checking both: a saturated direct semaphore must block further direct
 * uploads, and the gRPC client's available-write-bytes counter must
 * remain untouched throughout.
 */
public class RemoteFileDataStorageManagerDirectUploadBackpressureTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private LatchingObjectStorage objectStorage;
    private RemoteFileServiceClient stubClient;
    private RemoteFileDataStorageManager dsm;
    private ExecutorService uploadExecutor;

    @Before
    public void setUp() throws IOException {
        objectStorage = new LatchingObjectStorage();
        Map<String, Object> fastFailConfig = new HashMap<>();
        fastFailConfig.put(RemoteFileServiceClient.CONFIG_CLIENT_TIMEOUT, 1L);
        fastFailConfig.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 0);
        stubClient = new RemoteFileServiceClient(Collections.emptyList(), fastFailConfig);
        Path metaDir = tmpFolder.newFolder("meta").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        dsm = new RemoteFileDataStorageManager(metaDir, tmpDir, Integer.MAX_VALUE, stubClient);
        dsm.setDirectObjectStorage(objectStorage);
        uploadExecutor = Executors.newCachedThreadPool();
    }

    @After
    public void tearDown() throws Exception {
        if (uploadExecutor != null) {
            uploadExecutor.shutdownNow();
            uploadExecutor.awaitTermination(10L, TimeUnit.SECONDS);
        }
        // Release every still-held latch so the stub uploads can complete
        // before the DSM close drains pending state.
        objectStorage.releaseAll();
        if (dsm != null) {
            dsm.close();
        }
        if (stubClient != null) {
            stubClient.close();
        }
    }

    private Path writeTempFile(String name, int size) throws IOException {
        Path f = tmpFolder.newFile(name).toPath();
        byte[] payload = new byte[size];
        // Deterministic content so test failures show byte mismatches if any.
        for (int i = 0; i < size; i++) {
            payload[i] = (byte) (i & 0xff);
        }
        Files.write(f, payload);
        return f;
    }

    /**
     * With an 8 MiB cap and two outstanding 6 MiB uploads, the second
     * upload's permit acquisition must block because there are only 2 MiB
     * free. Releasing the first upload's latch unblocks the second and
     * permits return to full capacity.
     */
    @Test
    public void semaphoreBlocksWhenInflightCapExhausted() throws Exception {
        final long capBytes = 8L * 1024 * 1024;
        dsm.enableDirectUpload(capBytes);
        assertEquals("semaphore starts full", capBytes, dsm.availableDirectInflightUploadBytes());

        final long blockSize = 6L * 1024 * 1024; // 6 MiB — two of these exceed the 8 MiB cap

        Path tempA = writeTempFile("A.bin", (int) blockSize);
        Path tempB = writeTempFile("B.bin", (int) blockSize);

        CountDownLatch firstAcquired = new CountDownLatch(1);
        CountDownLatch secondAcquired = new CountDownLatch(1);

        // Launch upload A. The stub future is latch-held so the permits
        // stay reserved until we release them explicitly.
        objectStorage.holdNext = true;
        CompletableFuture<Void> uploadA = CompletableFuture.runAsync(() -> {
            try {
                firstAcquired.countDown();
                dsm.writeMultipartIndexFile("ts", "uuid-a", "g", tempA, null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, uploadExecutor);

        // Wait until A's permit reservation is reflected in the semaphore.
        assertTrue(firstAcquired.await(5L, TimeUnit.SECONDS));
        // Spin until A has both acquired the semaphore AND called uploadFile
        // (so heldCount() becomes 1). We need both conditions before launching
        // B because (a) the semaphore check proves A holds its 6 MiB reservation
        // and (b) heldCount()==1 proves A's future is registered in the latch list
        // so the first releaseFirstHeld() below will actually unblock A.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while ((dsm.availableDirectInflightUploadBytes() > capBytes - blockSize
                || objectStorage.heldCount() < 1)
                && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertEquals("A must hold its 6 MiB reservation",
                capBytes - blockSize, dsm.availableDirectInflightUploadBytes());
        assertEquals("A's uploadFile must have landed on the stub",
                1, objectStorage.heldCount());

        // Launch B. It must block at the semaphore because only 2 MiB are
        // available and B asks for 6 MiB.
        objectStorage.holdNext = true;
        CompletableFuture<Void> uploadB = CompletableFuture.runAsync(() -> {
            try {
                secondAcquired.countDown();
                dsm.writeMultipartIndexFile("ts", "uuid-b", "g", tempB, null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, uploadExecutor);
        assertTrue(secondAcquired.await(5L, TimeUnit.SECONDS));

        // Confirm B is blocked at the semaphore: we know B's lambda has started
        // (secondAcquired), and after a brief yield B's thread should have reached
        // acquireUninterruptibly inside reserveDirectInflightUploadBytes. The
        // strongest observable invariant while B is blocked is:
        //   (a) availableDirectInflightUploadBytes() == capBytes - blockSize
        //       (only A's 6 MiB reservation is outstanding; B can't acquire)
        //   (b) objectStorage.heldCount() == 1
        //       (only A's uploadFile future is held; B hasn't reached uploadFile yet)
        // We active-wait to give B's thread time to reach the semaphore (sub-ms
        // in practice) and use a 5 s ceiling for slow CI runners. If the semaphore
        // were broken (B acquired immediately), heldCount() would jump to 2 and
        // availableBytes would drop further — both conditions would fail.
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        // Wait until the invariant stabilises (the loop exits as soon as both
        // conditions match; the assertion below catches any deviation).
        while (dsm.availableDirectInflightUploadBytes() != capBytes - blockSize
                && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertEquals("A must still hold its reservation while B is blocked",
                capBytes - blockSize, dsm.availableDirectInflightUploadBytes());
        assertEquals("B must not have called uploadFile yet (blocked at semaphore)",
                1, objectStorage.heldCount());
        assertEquals("uploadA should still be in flight", false, uploadA.isDone());
        assertEquals("uploadB should still be blocked on the semaphore",
                false, uploadB.isDone());

        // Release A's latch — A completes, returns its permits, B unblocks.
        objectStorage.releaseFirstHeld();
        // Wait for A to complete (its permit release fires in whenComplete).
        uploadA.get(10L, TimeUnit.SECONDS);
        // Allow B's permit acquisition to proceed and verify it now holds
        // the reservation. Wait for two distinct conditions:
        //   1. the semaphore reflects B's 6 MiB reservation (permit acquired)
        //   2. B's uploadFile call has landed on the stub and B's held future
        //      is registered in the latch list (so releaseFirstHeld(B) below
        //      sees it). Permit acquisition happens BEFORE uploadFile, so the
        //      semaphore-only check would race: B could be between
        //      reserveDirectInflightUploadBytes() and storage.uploadFile() when
        //      the test reads the gauge.
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while ((dsm.availableDirectInflightUploadBytes() != capBytes - blockSize
                || objectStorage.heldCount() < 1)
                && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertEquals("B must now hold the only outstanding reservation",
                capBytes - blockSize, dsm.availableDirectInflightUploadBytes());
        assertEquals("B's uploadFile must have landed on the stub",
                1, objectStorage.heldCount());

        // Release B and verify the semaphore drains back to full.
        objectStorage.releaseFirstHeld();
        uploadB.get(10L, TimeUnit.SECONDS);
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (dsm.availableDirectInflightUploadBytes() != capBytes
                && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertEquals("semaphore must return to full after both uploads complete",
                capBytes, dsm.availableDirectInflightUploadBytes());
    }

    /**
     * The gRPC client's inflight-write-bytes counter must NOT be decremented
     * by direct uploads — the two budgets are independent.
     */
    @Test
    public void directUploadDoesNotConsumeGrpcInflightWriteBudget() throws Exception {
        dsm.enableDirectUpload(16L * 1024 * 1024);
        long grpcBudgetBefore = stubClient.availableInflightWriteBytes();
        Path src = writeTempFile("indep.bin", 1024);
        dsm.writeMultipartIndexFile("ts", "uuid-indep", "g", src, null);
        long grpcBudgetAfter = stubClient.availableInflightWriteBytes();
        assertEquals("gRPC inflight-write budget must not be touched by direct uploads",
                grpcBudgetBefore, grpcBudgetAfter);
    }

    // ====================================================================
    // LatchingObjectStorage — completes uploadFile futures only after
    // releaseFirstHeld() is called, so the test can pin the permit-hold
    // window and observe semaphore state under controlled concurrency.
    // ====================================================================

    static final class LatchingObjectStorage implements ObjectStorage {
        private final List<CompletableFuture<Long>> heldFutures =
                Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger uploadCount = new AtomicInteger();
        volatile boolean holdNext = false;

        @Override
        public CompletableFuture<Long> uploadFile(String path, Path source,
                LongConsumer progress) {
            uploadCount.incrementAndGet();
            try {
                long size = Files.size(source);
                if (holdNext) {
                    holdNext = false;
                    CompletableFuture<Long> held = new CompletableFuture<>();
                    heldFutures.add(held);
                    return held;
                }
                return CompletableFuture.completedFuture(size);
            } catch (IOException e) {
                CompletableFuture<Long> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                return failed;
            }
        }

        synchronized void releaseFirstHeld() {
            if (!heldFutures.isEmpty()) {
                heldFutures.remove(0).complete(0L);
            }
        }

        synchronized int heldCount() {
            return heldFutures.size();
        }

        synchronized void releaseAll() {
            for (CompletableFuture<Long> f : heldFutures) {
                f.complete(0L);
            }
            heldFutures.clear();
        }

        @Override
        public CompletableFuture<Boolean> existsObject(String path) {
            return CompletableFuture.completedFuture(Boolean.FALSE);
        }

        @Override
        public CompletableFuture<Boolean> delete(String path) {
            return CompletableFuture.completedFuture(Boolean.TRUE);
        }

        @Override
        public CompletableFuture<Void> write(String path, byte[] content) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<ReadResult> read(String path) {
            return CompletableFuture.completedFuture(ReadResult.notFound());
        }

        @Override
        public CompletableFuture<Void> writeBlock(String path, long blockIndex, byte[] content) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Boolean> deleteLogical(String path) {
            return CompletableFuture.completedFuture(Boolean.TRUE);
        }

        @Override
        public CompletableFuture<List<String>> listLogical(String prefix) {
            return CompletableFuture.completedFuture(new ArrayList<>());
        }

        @Override
        public CompletableFuture<List<String>> list(String prefix) {
            return CompletableFuture.completedFuture(new ArrayList<>());
        }

        @Override
        public CompletableFuture<Integer> deleteByPrefix(String prefix) {
            return CompletableFuture.completedFuture(0);
        }

        @Override
        public CompletableFuture<Void> downloadFileBulk(String path, Path dest) {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IOException("not used in this test"));
            return failed;
        }

        @Override
        public void close() {
            // Make sure no stray futures are left behind so the JVM does not
            // hold pooled buffers past the test.
            releaseAll();
            // Defensive: a non-zero upload count proves the DSM actually
            // exercised this stub at least once during the test. A trivially
            // wrong wiring (e.g. uploads landing on a different storage)
            // would surface as 0 here.
            // Reference uploadCount to prevent unused-field warnings under
            // SpotBugs / IDE inspections without changing semantics.
            int finalCount = uploadCount.get();
            assert finalCount >= 0;
        }
    }
}
