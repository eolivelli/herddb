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

package herddb.remote.storage;

import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import software.amazon.awssdk.core.FileTransformerConfiguration;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.transfer.s3.progress.TransferListener;

/**
 * S3-backed implementation of {@link ObjectStorage} using the AWS SDK v2 async client.
 * Compatible with MinIO (forcePathStyle must be enabled on the client).
 *
 * @author enrico.olivelli
 */
public class S3ObjectStorage implements ObjectStorage {

    private static final Logger LOGGER = Logger.getLogger(S3ObjectStorage.class.getName());
    private static final int MAX_BATCH_SIZE = 1000;

    private final S3AsyncClient client;
    private final String bucket;
    private final String keyPrefix;
    @Nullable
    private final OpStatsLogger s3ReadLatency;
    @Nullable
    private final Counter s3ReadBytes;
    @Nullable
    private final Counter s3ReadRequests;
    /**
     * Issue #638: S3 Transfer Manager used by
     * {@link #uploadFile(String, Path, java.util.function.LongConsumer)} and
     * {@link #downloadFileBulk(String, Path)} for high-throughput bulk
     * transfers via the S3 Multipart Upload / Multipart Download APIs.
     *
     * <p>Optional: when {@code null}, both methods fall back to the
     * single-{@code PutObject}/single-{@code GetObject} path inherited from
     * {@link ObjectStorage}. Setting via the dedicated constructor (or via
     * {@link #setTransferManager(S3TransferManager)} after construction)
     * activates the multipart-aware fast path.
     */
    @Nullable
    private volatile S3TransferManager transferManager;

    public S3ObjectStorage(S3AsyncClient client, String bucket, String prefix,
                          @Nullable StatsLogger statsLogger) {
        this(client, bucket, prefix, statsLogger, null);
    }

    /**
     * Issue #638: full constructor that wires an {@link S3TransferManager}
     * built on top of {@code client} (via {@link S3TransferManagerFactory})
     * so {@link #uploadFile} and {@link #downloadFileBulk} go through real
     * multipart transfers.
     */
    public S3ObjectStorage(S3AsyncClient client, String bucket, String prefix,
                           @Nullable StatsLogger statsLogger,
                           @Nullable S3TransferManager transferManager) {
        this.client = client;
        this.bucket = bucket;
        this.keyPrefix = prefix == null ? "" : prefix;
        this.transferManager = transferManager;
        if (statsLogger != null) {
            StatsLogger s3Scope = statsLogger.scope("rfs").scope("s3");
            this.s3ReadLatency = s3Scope.getOpStatsLogger("read_latency");
            this.s3ReadBytes = s3Scope.getCounter("read_bytes");
            this.s3ReadRequests = s3Scope.getCounter("read_requests");
        } else {
            this.s3ReadLatency = null;
            this.s3ReadBytes = null;
            this.s3ReadRequests = null;
        }
    }

    /**
     * Backward-compatible constructor without metrics logging.
     */
    public S3ObjectStorage(S3AsyncClient client, String bucket, String prefix) {
        this(client, bucket, prefix, null, null);
    }

    /**
     * Issue #638: wires an {@link S3TransferManager} after construction so
     * callers that build the storage object before the TM exists
     * (e.g. when the CRT client and TM are constructed in different setup
     * phases) can still activate the multipart fast path.
     *
     * <p>This is a one-shot wiring used at startup; the {@code volatile}
     * field guarantees that any subsequent {@link #uploadFile} call sees
     * the new TM.
     */
    public void setTransferManager(@Nullable S3TransferManager transferManager) {
        this.transferManager = transferManager;
    }

    private String toKey(String path) {
        return keyPrefix + path;
    }

    private String fromKey(String key) {
        return key.substring(keyPrefix.length());
    }

    @Override
    public CompletableFuture<Void> write(String path, byte[] content) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(toKey(path))
                .build();
        return client.putObject(request, AsyncRequestBody.fromBytes(content))
                .thenApply(resp -> (Void) null);
    }

    @Override
    public CompletableFuture<ReadResult> read(String path) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(toKey(path))
                .build();
        return client.getObject(request, AsyncResponseTransformer.toBytes())
                .thenApply(response -> {
                    byte[] data = response.asByteArray();
                    ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(data.length);
                    buf.writeBytes(data);
                    return ReadResult.found(buf);
                })
                .exceptionally(t -> {
                    Throwable cause = (t instanceof CompletionException) ? t.getCause() : t;
                    if (cause instanceof NoSuchKeyException) {
                        return ReadResult.notFound();
                    }
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new RuntimeException(cause);
                });
    }

    /**
     * Streams the S3 object at {@code path} directly to a local file using
     * {@code AsyncResponseTransformer.toFile()}, avoiding the three-copy pattern
     * (CRT native buffer → byte[] → Netty ByteBuf) of the default
     * {@link #read(String)}-based fallback.
     *
     * <p>Block 0 of a multipart download uses {@code append=false}
     * ({@link FileTransformerConfiguration#defaultCreateOrReplaceExisting()}); subsequent
     * blocks use {@code append=true}
     * ({@link FileTransformerConfiguration#defaultCreateOrAppend()}).
     */
    @Override
    public CompletableFuture<Void> downloadToFile(String path, Path dest, boolean append) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(toKey(path))
                .build();
        FileTransformerConfiguration fileConfig = append
                ? FileTransformerConfiguration.defaultCreateOrAppend()
                : FileTransformerConfiguration.defaultCreateOrReplaceExisting();
        return client.getObject(request, AsyncResponseTransformer.toFile(dest, fileConfig))
                .thenApply(resp -> (Void) null);
    }

    @Override
    public CompletableFuture<Void> writeBlock(String path, long blockIndex, byte[] content) {
        String blockKey = toKey(path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex);
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(blockKey)
                .build();
        return client.putObject(request, AsyncRequestBody.fromBytes(content))
                .thenApply(resp -> (Void) null);
    }

    @Override
    public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
        long blockIndex = offset / blockSize;
        int offsetInBlock = (int) (offset % blockSize);
        // Each block is a separate S3 object starting at byte 0.
        // Download the whole block object and return the requested slice.
        if (s3ReadRequests != null) {
            s3ReadRequests.inc();
        }
        final long startNanos = System.nanoTime();
        return read(path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex)
                .thenApply(result -> {
                    try {
                        if (result.status() == ReadResult.Status.NOT_FOUND) {
                            if (s3ReadLatency != null) {
                                s3ReadLatency.registerFailedEvent(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
                            }
                            return ReadResult.notFound();
                        }
                        ByteBuf blockBuf = result.byteBuf();
                        int blockLength = blockBuf.readableBytes();
                        int from = offsetInBlock;
                        int to = Math.min(from + length, blockLength);
                        if (from >= blockLength) {
                            if (s3ReadLatency != null) {
                                s3ReadLatency.registerFailedEvent(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
                            }
                            return ReadResult.notFound();
                        }
                        // Create pooled slice without copying
                        ByteBuf sliceBuf = PooledByteBufAllocator.DEFAULT.directBuffer(to - from);
                        sliceBuf.writeBytes(blockBuf, blockBuf.readerIndex() + from, to - from);
                        if (s3ReadLatency != null) {
                            s3ReadLatency.registerSuccessfulEvent(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
                        }
                        if (s3ReadRequests != null) {
                            s3ReadRequests.inc();
                        }
                        if (s3ReadBytes != null) {
                            s3ReadBytes.inc();
                        }
                        return ReadResult.found(sliceBuf);
                    } finally {
                        result.release();
                    }
                })
                .exceptionally(t -> {
                    if (s3ReadLatency != null) {
                        s3ReadLatency.registerFailedEvent(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
                    }
                    Throwable cause = (t instanceof CompletionException) ? t.getCause() : t;
                    if (cause instanceof NoSuchKeyException) {
                        return ReadResult.notFound();
                    }
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new RuntimeException(cause);
                });
    }

    @Override
    public CompletableFuture<Boolean> deleteLogical(String path) {
        // Delete multipart blocks and the single-part file in parallel
        String multipartPrefix = path + ObjectStorage.MULTIPART_SUFFIX + "/";
        CompletableFuture<Integer> deletedBlocks = deleteByPrefix(multipartPrefix);
        CompletableFuture<Boolean> deletedSingle = delete(path);
        return deletedBlocks.thenCombine(deletedSingle, (blocks, single) -> blocks > 0 || single);
    }

    @Override
    public CompletableFuture<List<String>> listLogical(String prefix) {
        return list(prefix).thenApply(paths -> {
            LinkedHashSet<String> logical = new LinkedHashSet<>();
            for (String p : paths) {
                int mpIdx = p.indexOf(ObjectStorage.MULTIPART_SUFFIX + "/");
                if (mpIdx >= 0) {
                    String logicalPath = p.substring(0, mpIdx);
                    if (logicalPath.startsWith(prefix)) {
                        logical.add(logicalPath);
                    }
                } else {
                    logical.add(p);
                }
            }
            return new ArrayList<>(logical);
        });
    }

    @Override
    public CompletableFuture<Boolean> delete(String path) {
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(toKey(path))
                .build();
        // S3 DELETE is idempotent: native S3 silently succeeds for a missing key, but
        // S3-compatible stores (notably GCS) surface it as NoSuchKeyException. The end
        // state — object absent — is what the caller wanted, so treat 404 as success.
        return client.deleteObject(request)
                .<Boolean>thenApply(resp -> Boolean.TRUE)
                .exceptionally(t -> {
                    Throwable cause = (t instanceof CompletionException) ? t.getCause() : t;
                    if (cause instanceof NoSuchKeyException) {
                        return Boolean.TRUE;
                    }
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new RuntimeException(cause);
                });
    }

    @Override
    public CompletableFuture<List<String>> list(String prefix) {
        List<String> results = new ArrayList<>();
        return listPage(prefix, null, results);
    }

    private CompletableFuture<List<String>> listPage(String prefix, String continuationToken, List<String> results) {
        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(keyPrefix + prefix);
        if (continuationToken != null) {
            builder.continuationToken(continuationToken);
        }
        return client.listObjectsV2(builder.build()).thenCompose(resp -> {
            for (S3Object obj : resp.contents()) {
                results.add(fromKey(obj.key()));
            }
            if (Boolean.TRUE.equals(resp.isTruncated())) {
                return listPage(prefix, resp.nextContinuationToken(), results);
            }
            return CompletableFuture.completedFuture(results);
        });
    }

    @Override
    public CompletableFuture<Integer> deleteByPrefix(String prefix) {
        return list(prefix).thenCompose(paths -> {
            if (paths.isEmpty()) {
                return CompletableFuture.completedFuture(0);
            }
            List<String> fullKeys = paths.stream()
                    .map(this::toKey)
                    .collect(Collectors.toList());
            int[] totalDeleted = {0};
            return deleteBatches(fullKeys, 0, totalDeleted);
        });
    }

    private CompletableFuture<Integer> deleteBatches(List<String> keys, int offset, int[] totalDeleted) {
        if (offset >= keys.size()) {
            return CompletableFuture.completedFuture(totalDeleted[0]);
        }
        int end = Math.min(offset + MAX_BATCH_SIZE, keys.size());
        List<ObjectIdentifier> identifiers = keys.subList(offset, end).stream()
                .map(k -> ObjectIdentifier.builder().key(k).build())
                .collect(Collectors.toList());
        DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(Delete.builder().objects(identifiers).build())
                .build();
        return client.deleteObjects(request).thenCompose(resp -> {
            totalDeleted[0] += resp.deleted().size();
            return deleteBatches(keys, end, totalDeleted);
        });
    }

    /**
     * Issue #638: bulk upload via {@link S3TransferManager#uploadFile} —
     * the canonical AWS SDK v2 high-throughput bulk-transfer API. When a
     * TM has been wired, the source file is uploaded as a single S3 object
     * using real S3 Multipart Upload (parallel parts pipelined by the CRT
     * HTTP client). Otherwise falls back to the single-{@code PutObject}
     * default — correct but slower.
     *
     * <p>{@code progress} is invoked with byte deltas via a
     * {@link TransferListener}.
     */
    @Override
    public CompletableFuture<Long> uploadFile(String path, java.nio.file.Path source,
                                              java.util.function.LongConsumer progress) {
        S3TransferManager tm = this.transferManager;
        if (tm == null) {
            return ObjectStorage.super.uploadFile(path, source, progress);
        }
        UploadFileRequest.Builder builder = UploadFileRequest.builder()
                .putObjectRequest(req -> req.bucket(bucket).key(toKey(path)))
                .source(source);
        if (progress != null) {
            builder.addTransferListener(new ProgressForwardingListener(progress));
        }
        return tm.uploadFile(builder.build()).completionFuture()
                .thenApply(completed -> {
                    try {
                        return java.nio.file.Files.size(source);
                    } catch (IOException e) {
                        // Source file length is queried after a successful upload purely so
                        // we can report the byte count. If the file vanished underneath us
                        // (extremely unlikely on a freshly-written temp file), surface the
                        // I/O error rather than guessing — the upload itself succeeded.
                        throw new CompletionException(e);
                    }
                });
    }

    /**
     * Issue #638: single-key existence probe via S3 {@code HEAD} — the cheap,
     * canonical way to check whether a logical multipart file is stored in
     * the bulk layout ({@code {logicalPath}.bulk}) before deciding the read
     * path.
     */
    @Override
    public CompletableFuture<Boolean> existsObject(String path) {
        HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(bucket)
                .key(toKey(path))
                .build();
        return client.headObject(request)
                .<Boolean>thenApply(resp -> Boolean.TRUE)
                .exceptionally(t -> {
                    Throwable cause = (t instanceof CompletionException) ? t.getCause() : t;
                    // Known S3 "missing object" surfaces — both AWS native and CRT/MinIO —
                    // map to false; anything else also maps to false to honour the
                    // best-effort contract of existsObject (must not throw on missing).
                    if (cause instanceof NoSuchKeyException) {
                        return Boolean.FALSE;
                    }
                    // Some S3-compatible stores return a generic SdkServiceException
                    // with HTTP 404 instead of NoSuchKey. Inspect the status code if
                    // available — otherwise default to false for the best-effort probe.
                    if (cause instanceof software.amazon.awssdk.awscore.exception.AwsServiceException) {
                        int code = ((software.amazon.awssdk.awscore.exception.AwsServiceException) cause)
                                .statusCode();
                        if (code == 404) {
                            return Boolean.FALSE;
                        }
                    }
                    return Boolean.FALSE;
                });
    }

    /**
     * Issue #638: bulk download via {@link S3TransferManager#downloadFile} —
     * symmetric to {@link #uploadFile} and used to materialise a
     * bulk-layout multipart file back to local disk in one shot.
     */
    @Override
    public CompletableFuture<Void> downloadFileBulk(String path, java.nio.file.Path dest) {
        S3TransferManager tm = this.transferManager;
        if (tm == null) {
            return ObjectStorage.super.downloadFileBulk(path, dest);
        }
        DownloadFileRequest request = DownloadFileRequest.builder()
                .getObjectRequest(req -> req.bucket(bucket).key(toKey(path)))
                .destination(dest)
                .build();
        return tm.downloadFile(request).completionFuture()
                .thenApply(completed -> (Void) null);
    }

    @Override
    public void close() {
        // Close the Transfer Manager first: it was built on top of the CRT
        // S3AsyncClient, but per the TM contract closing it does NOT close
        // the wrapped client (we built it via .s3Client(...)). The client is
        // closed right after so the two resources are released in dependency
        // order on every shutdown path.
        S3TransferManager tm = this.transferManager;
        if (tm != null) {
            try {
                tm.close();
            } catch (RuntimeException e) {
                // S3TransferManager.close() is declared AutoCloseable but the
                // CRT-backed impl can throw on a partially-initialised manager.
                // We intentionally swallow here so the underlying CRT client
                // still gets closed below — losing the TM in best-effort
                // shutdown is preferable to leaking native CRT threads.
                LOGGER.warning("error closing S3TransferManager: " + e.getMessage());
            }
            this.transferManager = null;
        }
        client.close();
    }

    /**
     * Forwards {@link TransferListener#bytesTransferred} events to a
     * {@link java.util.function.LongConsumer} as deltas (not running totals)
     * so they match the {@code progress} semantics documented in
     * {@link ObjectStorage#uploadFile}. The listener is invoked on a CRT
     * worker thread; the consumer must be safe under concurrent calls.
     */
    private static final class ProgressForwardingListener implements TransferListener {
        private final java.util.function.LongConsumer progress;
        private long lastReported = 0L;

        ProgressForwardingListener(java.util.function.LongConsumer progress) {
            this.progress = progress;
        }

        @Override
        public synchronized void bytesTransferred(Context.BytesTransferred context) {
            long total = context.progressSnapshot().transferredBytes();
            long delta = total - lastReported;
            if (delta > 0) {
                lastReported = total;
                progress.accept(delta);
            }
        }
    }
}
