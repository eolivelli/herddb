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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.remote.storage.LocalObjectStorage;
import herddb.remote.storage.ObjectStorage;
import herddb.remote.storage.ReadResult;
import herddb.storage.DataStorageManagerException;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #645: failure-injection coverage for the bulk-layout probe on the
 * read paths. The bench failure (Run 9 BIGANN 200M) crashed the IS with
 * confusing {@code Block not found} errors because the bulk-layout probe
 * returned {@code false} on transient HEAD failures and the read path
 * silently fell back to the gRPC file-server, which has no record of
 * direct-S3-uploaded blocks.
 *
 * <p>After the issue #645 fix, a transient probe failure on the
 * <em>read</em> paths ({@code multipartIndexReaderSupplier} and
 * {@code downloadMultipartIndexFile}) MUST surface as a clear
 * {@link DataStorageManagerException} or {@link IOException}, not silently
 * fall through to gRPC. The lenient {@code multipartIndexFileExists}
 * keeps its boolean contract.
 *
 * <p>The test simulates two failure modes:
 * <ol>
 *   <li>HEAD always completes exceptionally (e.g. MinIO returning 503 or
 *       a connect timeout to S3).</li>
 *   <li>HEAD recovers after N attempts — verifies that a subsequent read
 *       succeeds once the storage is healthy again.</li>
 * </ol>
 */
public class RemoteFileDataStorageManagerDirectUploadProbeFailureTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private ExecutorService metadataExecutor;
    private RemoteFileServiceClient stubClient;

    @Before
    public void setUp() throws IOException {
        metadataExecutor = Executors.newSingleThreadExecutor();
        Map<String, Object> fastFailConfig = new HashMap<>();
        fastFailConfig.put(RemoteFileServiceClient.CONFIG_CLIENT_TIMEOUT, 1L);
        fastFailConfig.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 0);
        stubClient = new RemoteFileServiceClient(Collections.emptyList(), fastFailConfig);
    }

    @After
    public void tearDown() throws Exception {
        if (stubClient != null) {
            stubClient.close();
        }
        if (metadataExecutor != null) {
            metadataExecutor.shutdown();
        }
    }

    private Path writeTempFile(String name, byte[] content) throws IOException {
        Path f = tmpFolder.newFile(name).toPath();
        Files.write(f, content);
        return f;
    }

    private RemoteFileDataStorageManager newDsm(ObjectStorage storage) throws IOException {
        Path metaDir = tmpFolder.newFolder("meta-" + System.nanoTime()).toPath();
        Path tmpDir = tmpFolder.newFolder("tmp-" + System.nanoTime()).toPath();
        RemoteFileDataStorageManager dsm = new RemoteFileDataStorageManager(
                metaDir, tmpDir, Integer.MAX_VALUE, stubClient);
        dsm.setDirectObjectStorage(storage);
        dsm.enableDirectUpload(16L * 1024 * 1024);
        return dsm;
    }

    /**
     * Issue #645 — primary regression: when the HEAD probe always fails
     * (transient MinIO outage, network partition), the
     * {@code downloadMultipartIndexFile} read path MUST throw rather
     * than silently fall back to the gRPC per-block path. The latter
     * is the broken behaviour that produced the
     * "Block not found" crash-loop in Run 9.
     */
    @Test
    public void downloadThrowsWhenProbeAlwaysFails() throws Exception {
        // First, write the file via a healthy backing storage so the
        // .bulk object truly exists on disk.
        Path objectsDir = tmpFolder.newFolder("objects").toPath();
        LocalObjectStorage healthyStorage =
                new LocalObjectStorage(objectsDir, metadataExecutor);
        try (RemoteFileDataStorageManager writerDsm = newDsm(healthyStorage)) {
            byte[] payload = "payload-content".getBytes();
            Path src = writeTempFile("flaky-write.bin", payload);
            writerDsm.writeMultipartIndexFile("ts", "uuid-flaky", "graph", src, null);
        } finally {
            healthyStorage.close();
        }

        // Now spawn a "restart" DSM with a probe-failing storage wrapping
        // the same on-disk directory. existsObject always completes
        // exceptionally; uploadFile / downloadFileBulk pass through to
        // the healthy backing storage. This mirrors a real MinIO outage
        // that affects only HEAD requests.
        AtomicInteger headAttempts = new AtomicInteger();
        AlwaysFailingHeadStorage flakyStorage = new AlwaysFailingHeadStorage(
                new LocalObjectStorage(objectsDir, metadataExecutor),
                () -> new IOException("simulated MinIO HEAD 503"),
                headAttempts);
        try (RemoteFileDataStorageManager readerDsm = newDsm(flakyStorage)) {
            Path dest = tmpFolder.newFile("flaky-out.bin").toPath();
            try {
                readerDsm.downloadMultipartIndexFile(
                        "ts", "uuid-flaky", "graph", 15L, dest);
                fail("downloadMultipartIndexFile must throw when the HEAD"
                        + " probe fails transiently");
            } catch (IOException expected) {
                // Issue #645: the failure must mention either the bulk-layout
                // probe or the synthetic underlying cause, NOT "Block not
                // found" (which would indicate a silent gRPC fallback).
                assertNotNull(expected.getMessage());
                String msg = expected.getMessage();
                assertTrue("error message must mention the bulk-layout probe;"
                                + " got: " + msg,
                        msg.toLowerCase().contains("bulk")
                                || msg.toLowerCase().contains("probe")
                                || msg.contains("simulated MinIO HEAD 503"));
                assertTrue("error message must NOT misroute via gRPC"
                                + " ('Block not found' is the buggy path); got: "
                                + msg,
                        !msg.contains("Block not found"));
            }
            assertEquals("HEAD must have been attempted exactly once for the read",
                    1, headAttempts.get());
        } finally {
            flakyStorage.close();
        }
    }

    /**
     * Issue #645: {@code multipartIndexReaderSupplier} must also throw
     * (as a {@link DataStorageManagerException}) when the probe fails.
     * This is the load-path used by {@code OnDiskGraphIndex.load} — the
     * exact call chain that exploded in the bench logs:
     * {@code PersistentVectorStore.loadFusedPQSegment} →
     * {@code OnDiskGraphIndex.load} →
     * {@code RemoteRandomAccessReader.fetchBlockFromRemote} →
     * "Block not found".
     */
    @Test
    public void readerSupplierThrowsWhenProbeAlwaysFails() throws Exception {
        Path objectsDir = tmpFolder.newFolder("objects").toPath();
        LocalObjectStorage healthyStorage =
                new LocalObjectStorage(objectsDir, metadataExecutor);
        try (RemoteFileDataStorageManager writerDsm = newDsm(healthyStorage)) {
            byte[] payload = "ondisk-graph".getBytes();
            Path src = writeTempFile("supplier-write.bin", payload);
            writerDsm.writeMultipartIndexFile("ts", "uuid-supplier", "graph", src, null);
        } finally {
            healthyStorage.close();
        }

        AlwaysFailingHeadStorage flakyStorage = new AlwaysFailingHeadStorage(
                new LocalObjectStorage(objectsDir, metadataExecutor),
                () -> new IOException("synthetic transient HEAD failure"),
                new AtomicInteger());
        try (RemoteFileDataStorageManager readerDsm = newDsm(flakyStorage)) {
            try {
                readerDsm.multipartIndexReaderSupplier(
                        "ts", "uuid-supplier", "graph", 12L);
                fail("multipartIndexReaderSupplier must throw on probe failure");
            } catch (DataStorageManagerException expected) {
                String msg = expected.getMessage();
                assertNotNull(msg);
                assertTrue("error must reference the bulk-layout probe; got: " + msg,
                        msg.toLowerCase().contains("bulk")
                                || msg.toLowerCase().contains("probe"));
                // The IOException carrying the original synthetic cause
                // must be in the cause chain.
                Throwable cause = expected.getCause();
                while (cause != null && !(cause.getMessage() != null
                        && cause.getMessage().contains("synthetic transient HEAD failure"))) {
                    cause = cause.getCause();
                }
                assertNotNull("original synthetic cause must be in the chain", cause);
            }
        } finally {
            flakyStorage.close();
        }
    }

    /**
     * Issue #645: the lenient {@code multipartIndexFileExists} contract
     * is preserved — it must NOT throw on probe failure. Instead, it
     * returns {@code false} (consistent with its long-standing
     * best-effort presence-check contract). This keeps any caller of
     * {@code multipartIndexFileExists} that relies on the boolean
     * contract from regressing.
     *
     * <p>Note: the strict-throw behaviour is reserved for the read paths
     * ({@code downloadMultipartIndexFile} and
     * {@code multipartIndexReaderSupplier}) where falling through to
     * gRPC would corrupt the load. The presence check is allowed to be
     * lenient because its callers ({@code deleteMultipartIndexFile},
     * observability) tolerate a transient false-negative.
     */
    @Test
    public void existsCheckDoesNotThrowOnProbeFailure() throws Exception {
        Path objectsDir = tmpFolder.newFolder("objects").toPath();
        AlwaysFailingHeadStorage flakyStorage = new AlwaysFailingHeadStorage(
                new LocalObjectStorage(objectsDir, metadataExecutor),
                () -> new IOException("HEAD blip"),
                new AtomicInteger());
        try (RemoteFileDataStorageManager dsm = newDsm(flakyStorage)) {
            // Must not throw; must return false (lenient contract).
            boolean exists = dsm.multipartIndexFileExists(
                    "ts", "uuid-lenient", "graph");
            // The gRPC fallback also fails fast (empty server list), so
            // the answer is {@code false}. The KEY behaviour is that the
            // call returned without throwing.
            assertEquals("lenient probe must return false on HEAD failure"
                            + " (and NOT throw)",
                    false, exists);
        } finally {
            flakyStorage.close();
        }
    }

    /**
     * Issue #645: HEAD probes that recover (transient outage clears) must
     * allow subsequent reads to succeed. This guards against an over-eager
     * cache or sticky-error state.
     */
    @Test
    public void readSucceedsAfterTransientHeadOutageClears() throws Exception {
        Path objectsDir = tmpFolder.newFolder("objects").toPath();
        LocalObjectStorage healthyStorage =
                new LocalObjectStorage(objectsDir, metadataExecutor);
        byte[] payload = "recovery-payload".getBytes();
        try (RemoteFileDataStorageManager writerDsm = newDsm(healthyStorage)) {
            Path src = writeTempFile("recovery-write.bin", payload);
            writerDsm.writeMultipartIndexFile("ts", "uuid-recover", "graph", src, null);
        } finally {
            healthyStorage.close();
        }

        AtomicInteger headAttempts = new AtomicInteger();
        TransientFailingHeadStorage flakyStorage = new TransientFailingHeadStorage(
                new LocalObjectStorage(objectsDir, metadataExecutor),
                /* failTimes = */ 2,
                () -> new IOException("transient HEAD blip"),
                headAttempts);
        try (RemoteFileDataStorageManager readerDsm = newDsm(flakyStorage)) {
            // Attempt 1: probe fails → read throws.
            try {
                Path dest1 = tmpFolder.newFile("recover-out-1.bin").toPath();
                readerDsm.downloadMultipartIndexFile(
                        "ts", "uuid-recover", "graph", payload.length, dest1);
                fail("read 1 must fail while HEAD is broken");
            } catch (IOException expected) {
                // expected
            }

            // Attempt 2: probe still failing → read still throws.
            try {
                Path dest2 = tmpFolder.newFile("recover-out-2.bin").toPath();
                readerDsm.downloadMultipartIndexFile(
                        "ts", "uuid-recover", "graph", payload.length, dest2);
                fail("read 2 must fail while HEAD is broken");
            } catch (IOException expected) {
                // expected
            }

            // Attempt 3: probe recovers (failTimes=2) → read succeeds and
            // caches the positive result for any subsequent reads.
            Path dest3 = tmpFolder.newFile("recover-out-3.bin").toPath();
            readerDsm.downloadMultipartIndexFile(
                    "ts", "uuid-recover", "graph", payload.length, dest3);
            byte[] read = Files.readAllBytes(dest3);
            assertEquals("recovered read must reconstruct the original",
                    new String(payload), new String(read));

            // Attempt 4: cache hit, no further HEAD round-trip.
            int headsAfterRecovery = headAttempts.get();
            Path dest4 = tmpFolder.newFile("recover-out-4.bin").toPath();
            readerDsm.downloadMultipartIndexFile(
                    "ts", "uuid-recover", "graph", payload.length, dest4);
            assertEquals("subsequent read after recovery must hit the probe cache",
                    headsAfterRecovery, headAttempts.get());
        } finally {
            flakyStorage.close();
        }
    }

    // ====================================================================
    // Failure-injection ObjectStorage wrappers.
    // ====================================================================

    /**
     * Delegating storage that intercepts {@code existsObject} (the HEAD
     * probe) and ALWAYS returns an exceptionally-completed future via
     * the supplied factory. Other methods pass through to the wrapped
     * storage so legitimate uploads/downloads can be staged before the
     * failure injection takes effect.
     */
    private static final class AlwaysFailingHeadStorage implements ObjectStorage {

        private final ObjectStorage delegate;
        private final java.util.function.Supplier<IOException> errorFactory;
        private final AtomicInteger headAttempts;

        AlwaysFailingHeadStorage(ObjectStorage delegate,
                                 java.util.function.Supplier<IOException> errorFactory,
                                 AtomicInteger headAttempts) {
            this.delegate = delegate;
            this.errorFactory = errorFactory;
            this.headAttempts = headAttempts;
        }

        @Override
        public CompletableFuture<Boolean> existsObject(String path) {
            headAttempts.incrementAndGet();
            CompletableFuture<Boolean> failed = new CompletableFuture<>();
            failed.completeExceptionally(errorFactory.get());
            return failed;
        }

        @Override
        public CompletableFuture<Void> write(String path, byte[] content) {
            return delegate.write(path, content);
        }

        @Override
        public CompletableFuture<ReadResult> read(String path) {
            return delegate.read(path);
        }

        @Override
        public CompletableFuture<Void> writeBlock(String path, long blockIndex, byte[] content) {
            return delegate.writeBlock(path, blockIndex, content);
        }

        @Override
        public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
            return delegate.readRange(path, offset, length, blockSize);
        }

        @Override
        public CompletableFuture<Boolean> deleteLogical(String path) {
            return delegate.deleteLogical(path);
        }

        @Override
        public CompletableFuture<List<String>> listLogical(String prefix) {
            return delegate.listLogical(prefix);
        }

        @Override
        public CompletableFuture<Boolean> delete(String path) {
            return delegate.delete(path);
        }

        @Override
        public CompletableFuture<List<String>> list(String prefix) {
            return delegate.list(prefix);
        }

        @Override
        public CompletableFuture<Integer> deleteByPrefix(String prefix) {
            return delegate.deleteByPrefix(prefix);
        }

        @Override
        public CompletableFuture<Long> uploadFile(String path, Path source,
                                                  java.util.function.LongConsumer progress) {
            return delegate.uploadFile(path, source, progress);
        }

        @Override
        public CompletableFuture<Void> downloadFileBulk(String path, Path dest) {
            return delegate.downloadFileBulk(path, dest);
        }

        @Override
        public CompletableFuture<Void> downloadToFile(String path, Path dest, boolean append) {
            return delegate.downloadToFile(path, dest, append);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }

    /**
     * Delegating storage that fails {@code existsObject} for the first
     * {@code failTimes} invocations, then succeeds (delegates to the
     * wrapped storage). Used to verify recovery after a transient HEAD
     * outage clears.
     */
    private static final class TransientFailingHeadStorage implements ObjectStorage {

        private final ObjectStorage delegate;
        private final int failTimes;
        private final java.util.function.Supplier<IOException> errorFactory;
        private final AtomicInteger headAttempts;

        TransientFailingHeadStorage(ObjectStorage delegate,
                                     int failTimes,
                                     java.util.function.Supplier<IOException> errorFactory,
                                     AtomicInteger headAttempts) {
            this.delegate = delegate;
            this.failTimes = failTimes;
            this.errorFactory = errorFactory;
            this.headAttempts = headAttempts;
        }

        @Override
        public CompletableFuture<Boolean> existsObject(String path) {
            int attempt = headAttempts.incrementAndGet();
            if (attempt <= failTimes) {
                CompletableFuture<Boolean> failed = new CompletableFuture<>();
                failed.completeExceptionally(errorFactory.get());
                return failed;
            }
            return delegate.existsObject(path);
        }

        @Override
        public CompletableFuture<Void> write(String path, byte[] content) {
            return delegate.write(path, content);
        }

        @Override
        public CompletableFuture<ReadResult> read(String path) {
            return delegate.read(path);
        }

        @Override
        public CompletableFuture<Void> writeBlock(String path, long blockIndex, byte[] content) {
            return delegate.writeBlock(path, blockIndex, content);
        }

        @Override
        public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
            return delegate.readRange(path, offset, length, blockSize);
        }

        @Override
        public CompletableFuture<Boolean> deleteLogical(String path) {
            return delegate.deleteLogical(path);
        }

        @Override
        public CompletableFuture<List<String>> listLogical(String prefix) {
            return delegate.listLogical(prefix);
        }

        @Override
        public CompletableFuture<Boolean> delete(String path) {
            return delegate.delete(path);
        }

        @Override
        public CompletableFuture<List<String>> list(String prefix) {
            return delegate.list(prefix);
        }

        @Override
        public CompletableFuture<Integer> deleteByPrefix(String prefix) {
            return delegate.deleteByPrefix(prefix);
        }

        @Override
        public CompletableFuture<Long> uploadFile(String path, Path source,
                                                  java.util.function.LongConsumer progress) {
            return delegate.uploadFile(path, source, progress);
        }

        @Override
        public CompletableFuture<Void> downloadFileBulk(String path, Path dest) {
            return delegate.downloadFileBulk(path, dest);
        }

        @Override
        public CompletableFuture<Void> downloadToFile(String path, Path dest, boolean append) {
            return delegate.downloadToFile(path, dest, append);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }

    @SuppressWarnings("unused")
    private static byte[] readByteBuf(ByteBuf buf) {
        byte[] out = new byte[buf.readableBytes()];
        buf.readBytes(out);
        return out;
    }
}
