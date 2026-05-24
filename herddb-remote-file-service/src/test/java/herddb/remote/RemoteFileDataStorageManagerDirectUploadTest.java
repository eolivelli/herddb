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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import herddb.remote.storage.ObjectStorage;
import herddb.remote.storage.ReadResult;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #638: unit-level coverage of the direct-S3 upload path on
 * {@link RemoteFileDataStorageManager}. Uses a hand-rolled in-memory
 * {@link ObjectStorage} stub that records every {@code uploadFile} /
 * {@code existsObject} / {@code delete} call so the test can assert on the
 * exact storage operations issued by the DSM.
 *
 * <p>The {@link RemoteFileServiceClient} is created with no servers — only
 * {@code client.getBlockSize()} is touched on the gRPC fallback path, and we
 * explicitly verify that the direct path does <em>not</em> exercise it. No
 * real gRPC channel is opened.
 */
public class RemoteFileDataStorageManagerDirectUploadTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private RecordingObjectStorage objectStorage;
    private RemoteFileServiceClient stubClient;
    private RemoteFileDataStorageManager dsm;

    @Before
    public void setUp() throws IOException {
        objectStorage = new RecordingObjectStorage();
        // Configure the stub gRPC client with a 1 s timeout and zero retries so
        // any call that *should* go through the file-server (the gRPC fallback
        // for delete or for the write path when direct upload is disabled)
        // fails fast instead of blocking on the default 30 min × 10 retries.
        // Direct uploads bypass this client entirely.
        Map<String, Object> fastFailConfig = new HashMap<>();
        fastFailConfig.put(RemoteFileServiceClient.CONFIG_CLIENT_TIMEOUT, 1L);
        fastFailConfig.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 0);
        stubClient = new RemoteFileServiceClient(Collections.emptyList(), fastFailConfig);
        Path metaDir = tmpFolder.newFolder("meta").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        dsm = new RemoteFileDataStorageManager(metaDir, tmpDir, Integer.MAX_VALUE, stubClient);
        dsm.setDirectObjectStorage(objectStorage);
    }

    @After
    public void tearDown() throws Exception {
        if (dsm != null) {
            dsm.close();
        }
        if (stubClient != null) {
            stubClient.close();
        }
    }

    /**
     * Helper: write the given bytes to a temp file under tmpFolder and
     * return its Path. Mirrors what a real PersistentVectorStore Phase-B
     * shard write would hand off to {@code writeMultipartIndexFile}.
     */
    private Path writeTempFile(String name, byte[] content) throws IOException {
        Path f = tmpFolder.newFile(name).toPath();
        Files.write(f, content);
        return f;
    }

    // ----------------------------------------------------------------
    // Test cases
    // ----------------------------------------------------------------

    /**
     * When direct upload is enabled, {@code writeMultipartIndexFile} routes
     * the upload to {@code ObjectStorage.uploadFile(...)} at the bulk key
     * {@code {logicalPath}.bulk}. The gRPC client must NOT be invoked.
     */
    @Test
    public void directUploadUsesBulkLayoutAndBypassesGrpc() throws Exception {
        dsm.enableDirectUpload(16L * 1024 * 1024);
        assertTrue("supportsDirectMultipartUpload must be true after enableDirectUpload",
                dsm.supportsDirectMultipartUpload());

        byte[] payload = new byte[7000];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i & 0xff);
        }
        Path tempFile = writeTempFile("phaseB-graph.bin", payload);

        String logicalPath = dsm.writeMultipartIndexFile("ts", "uuid-1", "graph", tempFile, null);
        assertEquals("logical path must match the per-block path convention",
                "ts/uuid-1/multipart/graph", logicalPath);

        // Single-object layout (issue #650): the upload lands at logicalPath
        // — no .bulk suffix, no .multipart/{N} per-block keys.
        assertEquals("exactly one upload expected",
                1, objectStorage.uploadCount.get());
        assertTrue("logical-path key must be present",
                objectStorage.objects.containsKey("ts/uuid-1/multipart/graph"));
        assertFalse("no .bulk key must be written",
                objectStorage.objects.keySet().stream().anyMatch(k -> k.endsWith(".bulk")));
        assertFalse("no per-block keys must be written",
                objectStorage.objects.keySet().stream().anyMatch(k -> k.contains(".multipart/")));
        assertArrayEquals("uploaded bytes must match source",
                payload, objectStorage.objects.get("ts/uuid-1/multipart/graph"));
    }

    /**
     * Progress callback must observe a delta total matching the file size.
     */
    @Test
    public void progressCallbackReportsFullByteCount() throws Exception {
        dsm.enableDirectUpload(16L * 1024 * 1024);
        byte[] payload = new byte[12345];
        Path tempFile = writeTempFile("progress.bin", payload);

        AtomicLong reported = new AtomicLong();
        LongConsumer progress = bytes -> reported.addAndGet(bytes);
        dsm.writeMultipartIndexFile("ts", "uuid-2", "map", tempFile, progress);

        assertEquals("progress must total the whole file",
                payload.length, reported.get());
    }

    /**
     * When direct upload is NOT enabled, {@code writeMultipartIndexFile}
     * falls back to the gRPC path. With an empty server list, the gRPC
     * client raises an {@link IllegalStateException} on dispatch — we
     * accept that as proof that the direct path was NOT taken. The
     * recording storage must record zero uploads.
     */
    @Test
    public void grpcFallbackWhenDirectUploadDisabled() throws Exception {
        // intentionally do not call enableDirectUpload
        assertFalse(dsm.supportsDirectMultipartUpload());
        byte[] payload = new byte[100];
        Path tempFile = writeTempFile("fallback.bin", payload);

        assertThrows(Exception.class, () ->
                dsm.writeMultipartIndexFile("ts", "uuid-3", "map", tempFile, null));
        assertEquals("no direct uploads must have been recorded",
                0, objectStorage.uploadCount.get());
    }

    /**
     * A failed upload must propagate as IOException, must invoke best-effort
     * cleanup (delete on the .bulk key), and must release the in-flight
     * permits — verified by reading {@code availableDirectInflightUploadBytes}
     * before and after.
     */
    @Test
    public void uploadFailureCleansUpAndReleasesPermits() throws Exception {
        long capacity = 32L * 1024 * 1024;
        dsm.enableDirectUpload(capacity);
        assertEquals("semaphore starts full",
                capacity, dsm.availableDirectInflightUploadBytes());

        // Wire a failing upload future.
        objectStorage.failNextUpload = new IOException("synthetic upload failure");

        byte[] payload = new byte[1024];
        Path tempFile = writeTempFile("fail.bin", payload);

        IOException ioe = assertThrows(IOException.class, () ->
                dsm.writeMultipartIndexFile("ts", "uuid-fail", "graph", tempFile, null));
        assertNotNull(ioe.getMessage());

        // Single-object layout (issue #650): on upload failure the DSM no
        // longer issues a best-effort cleanup delete on the logical key —
        // the S3 Multipart Upload's abort step handles part cleanup, and a
        // failed PUT leaves no object behind. The only invariant the test
        // pins now is permit release.
        assertEquals("permits must be released back to full after the failure",
                capacity, dsm.availableDirectInflightUploadBytes());
    }

    /**
     * After a direct upload, {@code multipartIndexFileExists} must return true.
     * Single-object layout (issue #650): the now-removed layout-probe cache is
     * gone — every probe issues exactly one HEAD against the direct backend.
     */
    @Test
    public void multipartIndexFileExistsAfterUpload() throws Exception {
        dsm.enableDirectUpload(16L * 1024 * 1024);
        Path tempFile = writeTempFile("exists.bin", new byte[50]);
        dsm.writeMultipartIndexFile("ts", "uuid-exists", "graph", tempFile, null);

        long beforeProbes = objectStorage.existsCount.get();
        assertTrue("file must exist after upload",
                dsm.multipartIndexFileExists("ts", "uuid-exists", "graph"));
        assertEquals("exactly one HEAD probe per call",
                beforeProbes + 1, objectStorage.existsCount.get());
    }

    /**
     * Round-trip: a file uploaded via the direct path must be readable via
     * {@link RemoteFileDataStorageManager#downloadMultipartIndexFile}: it
     * must take the bulk download branch (downloadFileBulk on the storage),
     * and the reconstructed bytes must match the original.
     */
    @Test
    public void roundTripBulkUploadAndDownload() throws Exception {
        dsm.enableDirectUpload(16L * 1024 * 1024);
        byte[] payload = new byte[8 * 1024 + 17];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) ((i * 31 + 7) & 0xff);
        }
        Path src = writeTempFile("rt.bin", payload);
        dsm.writeMultipartIndexFile("ts", "uuid-rt", "map", src, null);

        Path dest = tmpFolder.newFile("rt-dest.bin").toPath();
        // The download path is contractually allowed to replace the file
        // when the bulk layout is present; we still pass a fileSize hint
        // matching the source for the log line.
        dsm.downloadMultipartIndexFile("ts", "uuid-rt", "map", payload.length, dest);

        assertEquals("download must take the bulk branch",
                1, objectStorage.bulkDownloadCount.get());
        assertArrayEquals("downloaded bytes must equal the original",
                payload, Files.readAllBytes(dest));
    }

    /**
     * Single-object layout (issue #650): {@code deleteMultipartIndexFile}
     * dispatches the delete via the file-server gRPC client (not the direct
     * object storage), so the file-server's caching tier also invalidates.
     * This unit test runs against an empty stub client, so we only verify
     * that delete() does not throw and that subsequent existence probes go
     * out to the storage (no probe-cache to short-circuit them).
     */
    @Test
    public void deleteIsIdempotentAndProbeIsRequeried() throws Exception {
        dsm.enableDirectUpload(16L * 1024 * 1024);
        Path src = writeTempFile("del.bin", new byte[256]);
        dsm.writeMultipartIndexFile("ts", "uuid-del", "graph", src, null);
        assertTrue(dsm.multipartIndexFileExists("ts", "uuid-del", "graph"));

        // delete() must not throw — the gRPC client has no server but
        // RuntimeExceptions are logged and swallowed by the DSM.
        dsm.deleteMultipartIndexFile("ts", "uuid-del", "graph");
        dsm.deleteMultipartIndexFile("ts", "uuid-del", "graph"); // idempotent

        // The single-object layout has no probe-cache; manually remove the
        // backing object and verify a fresh probe re-queries the storage.
        objectStorage.objects.remove("ts/uuid-del/multipart/graph");
        long beforeProbes = objectStorage.existsCount.get();
        assertFalse(dsm.multipartIndexFileExists("ts", "uuid-del", "graph"));
        assertTrue("probe must re-query the storage after delete",
                objectStorage.existsCount.get() > beforeProbes);
    }

    /**
     * Calling {@code enableDirectUpload(0)} or a negative value must reject
     * with IllegalArgumentException — defensive check on operator input.
     */
    @Test
    public void enableDirectUploadRejectsBadCap() {
        assertThrows(IllegalArgumentException.class, () -> dsm.enableDirectUpload(0L));
        assertThrows(IllegalArgumentException.class, () -> dsm.enableDirectUpload(-1L));
    }

    /**
     * Calling {@code enableDirectUpload} twice with the same cap is idempotent:
     * the second call must be a no-op (must not reset the existing semaphore or
     * change any state). Calling with a different cap is a startup-only operation
     * and causes the semaphore to be recreated.
     */
    @Test
    public void enableDirectUploadIsIdempotentForSameCap() {
        long cap = 16L * 1024 * 1024;
        dsm.enableDirectUpload(cap);
        assertEquals(cap, dsm.availableDirectInflightUploadBytes());
        assertEquals(cap, dsm.maxDirectInflightUploadBytes());
        assertTrue(dsm.supportsDirectMultipartUpload());

        // Second call with the same cap — must be a no-op.
        dsm.enableDirectUpload(cap);
        assertEquals("idempotent: cap unchanged", cap, dsm.availableDirectInflightUploadBytes());
        assertEquals("idempotent: max unchanged", cap, dsm.maxDirectInflightUploadBytes());
        assertTrue("idempotent: supportsDirectMultipartUpload still true",
                dsm.supportsDirectMultipartUpload());

        // Different cap — must be accepted at startup time (replaces the semaphore).
        long newCap = 32L * 1024 * 1024;
        dsm.enableDirectUpload(newCap);
        assertEquals("new cap replaces old semaphore", newCap,
                dsm.availableDirectInflightUploadBytes());
        assertEquals(newCap, dsm.maxDirectInflightUploadBytes());
    }

    // cacheRaceWriteThenProbeDoesNotPinNegativeResult was removed in issue #650:
    // the layout-probe cache it pinned (bulkLayoutCache + positive-only caching)
    // no longer exists. With single-object layout the probe issues a HEAD against
    // the single object at logicalPath on every call — there is no negative cache
    // entry that could be incorrectly pinned.

    /**
     * {@code disableDirectUpload} flips support off and zeros the gauges.
     */
    @Test
    public void disableDirectUploadFlipsStateOff() {
        dsm.enableDirectUpload(16L * 1024 * 1024);
        assertTrue(dsm.supportsDirectMultipartUpload());
        dsm.disableDirectUpload();
        assertFalse(dsm.supportsDirectMultipartUpload());
        assertEquals(0L, dsm.availableDirectInflightUploadBytes());
        assertEquals(0L, dsm.maxDirectInflightUploadBytes());
    }

    // ====================================================================
    // RecordingObjectStorage — in-memory ObjectStorage that records every
    // call so the DSM's storage interactions can be asserted on.
    // ====================================================================

    /**
     * Thread-safe in-memory storage used purely for unit testing the DSM's
     * direct-upload path. Implements only the surface that
     * {@link RemoteFileDataStorageManager} touches in the direct branch:
     * {@code uploadFile}, {@code existsObject}, {@code downloadFileBulk},
     * {@code delete}. All other methods throw
     * {@link UnsupportedOperationException} so we catch any unintended
     * fallback to the per-block API.
     */
    static final class RecordingObjectStorage implements ObjectStorage {
        final Map<String, byte[]> objects = new HashMap<>();
        final Set<String> deletedKeys = Collections.synchronizedSet(new LinkedHashSet<>());
        final AtomicLong uploadCount = new AtomicLong();
        final AtomicLong existsCount = new AtomicLong();
        final AtomicLong bulkDownloadCount = new AtomicLong();
        volatile IOException failNextUpload;

        @Override
        public synchronized CompletableFuture<Long> uploadFile(String path, Path source,
                LongConsumer progress) {
            uploadCount.incrementAndGet();
            if (failNextUpload != null) {
                IOException toThrow = failNextUpload;
                failNextUpload = null;
                CompletableFuture<Long> failed = new CompletableFuture<>();
                failed.completeExceptionally(toThrow);
                return failed;
            }
            try {
                byte[] content = Files.readAllBytes(source);
                objects.put(path, content);
                if (progress != null && content.length > 0) {
                    progress.accept((long) content.length);
                }
                return CompletableFuture.completedFuture((long) content.length);
            } catch (IOException e) {
                CompletableFuture<Long> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                return failed;
            }
        }

        @Override
        public synchronized CompletableFuture<Boolean> existsObject(String path) {
            existsCount.incrementAndGet();
            return CompletableFuture.completedFuture(objects.containsKey(path));
        }

        @Override
        public synchronized CompletableFuture<Void> downloadFileBulk(String path, Path dest) {
            bulkDownloadCount.incrementAndGet();
            byte[] content = objects.get(path);
            if (content == null) {
                CompletableFuture<Void> failed = new CompletableFuture<>();
                failed.completeExceptionally(new IOException("not found: " + path));
                return failed;
            }
            try {
                Files.write(dest, content);
                return CompletableFuture.completedFuture(null);
            } catch (IOException e) {
                CompletableFuture<Void> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                return failed;
            }
        }

        @Override
        public synchronized CompletableFuture<Boolean> delete(String path) {
            deletedKeys.add(path);
            objects.remove(path);
            return CompletableFuture.completedFuture(Boolean.TRUE);
        }

        @Override
        public synchronized CompletableFuture<Void> write(String path, byte[] content) {
            objects.put(path, content);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public synchronized CompletableFuture<ReadResult> read(String path) {
            byte[] content = objects.get(path);
            if (content == null) {
                return CompletableFuture.completedFuture(ReadResult.notFound());
            }
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(content.length);
            buf.writeBytes(content);
            return CompletableFuture.completedFuture(ReadResult.found(buf));
        }

        @Override
        public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
            throw new UnsupportedOperationException("readRange must not be called on direct-upload path");
        }

        @Override
        public CompletableFuture<List<String>> list(String prefix) {
            return CompletableFuture.completedFuture(new ArrayList<>(objects.keySet()));
        }

        @Override
        public CompletableFuture<Integer> deleteByPrefix(String prefix) {
            int[] count = {0};
            objects.keySet().removeIf(k -> {
                if (k.startsWith(prefix)) {
                    count[0]++;
                    return true;
                }
                return false;
            });
            return CompletableFuture.completedFuture(count[0]);
        }

        @Override
        public void close() {
            // nothing to release; assert that any held resources are gone.
            assertNull("recording storage must have no pending failNextUpload at close",
                    failNextUpload);
        }
    }
}
