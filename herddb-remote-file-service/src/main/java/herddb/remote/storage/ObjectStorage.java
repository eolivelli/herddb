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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Abstraction over an object/blob storage backend.
 * Enables local filesystem, S3, or other providers.
 *
 * <p>Single-object layout (issue #650): every logical file is stored as
 * exactly one object at {@code path}. Large files are still served in
 * cache-block-sized slices through {@link #readRange(String, long, int, int)},
 * but the on-storage layout is one key per logical file — no
 * {@code .multipart/{N}} blocks, no {@code .bulk} suffix. The {@code blockSize}
 * parameter to {@link #readRange} is a cache-granularity hint only: backends
 * that wrap a caching tier (e.g. {@code CachingObjectStorage}) align cache
 * entries to {@code blockSize} boundaries; backends without a cache simply
 * honour {@code (offset, length)} as raw bytes against the single object.
 *
 * @author enrico.olivelli
 */
public interface ObjectStorage extends AutoCloseable {

    CompletableFuture<Void> write(String path, byte[] content);

    CompletableFuture<ReadResult> read(String path);

    /**
     * Reads a range of bytes from the single object stored at {@code path}.
     *
     * <p>Single-object layout: {@code (offset, length)} are absolute byte
     * coordinates within the one object that backs {@code path}. The
     * {@code blockSize} parameter is the cache-block granularity used by
     * caching backends; the contract guarantees that the returned slice
     * lies entirely within one {@code blockSize}-aligned window
     * {@code [floor(offset/blockSize)*blockSize, floor(offset/blockSize)*blockSize + blockSize)}.
     * Callers that need cross-block reads must issue multiple
     * {@code readRange} calls — this matches the existing
     * {@link herddb.remote.RemoteFileServiceClient#readFileRangeAsByteBufAsync}
     * splitting behaviour.
     *
     * @param path      logical file path
     * @param offset    absolute byte offset in the object
     * @param length    number of bytes to read
     * @param blockSize cache-block granularity (must be {@code > 0})
     */
    CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize);

    /**
     * Prefetches a range of bytes into the storage's cache (if any) without
     * returning the bytes to the caller.
     *
     * <p>The returned future has three terminal states:
     * <ul>
     *   <li>completes with {@code true} — the range was admitted into the
     *       cache (or it was already resident);</li>
     *   <li>completes with {@code false} — the underlying object was
     *       {@code NOT_FOUND}; nothing was admitted. Backends without a
     *       cache that nevertheless can cheaply distinguish present vs
     *       absent SHOULD report this; backends that cannot SHOULD return
     *       {@code true} (the prefetch contract is best-effort);</li>
     *   <li>completes exceptionally — a transient I/O / transport error.
     *       Callers MUST treat this as best-effort and continue.</li>
     * </ul>
     *
     * <p>The default implementation forwards to {@link #readRange} and
     * immediately releases the returned {@link ByteBuf}, mapping the
     * {@link ReadResult.Status} to the tri-state above.
     */
    default CompletableFuture<Boolean> prefetchRange(String path, long offset, int length, int blockSize) {
        return readRange(path, offset, length, blockSize).thenApply(result -> {
            ReadResult.Status status = result.status();
            result.release();
            return status == ReadResult.Status.FOUND ? Boolean.TRUE : Boolean.FALSE;
        });
    }

    CompletableFuture<Boolean> delete(String path);

    CompletableFuture<List<String>> list(String prefix);

    CompletableFuture<Integer> deleteByPrefix(String prefix);

    /**
     * Downloads the object at {@code path} and writes its content directly to
     * a local file, without materialising the full object in JVM heap or Netty
     * direct memory.
     *
     * <p>When {@code append} is {@code false} the destination file is created
     * (or replaced if it already exists). When {@code append} is {@code true}
     * the content is appended to the existing file (or the file is created if
     * absent).
     *
     * <p>The default implementation falls back to {@link #read(String)} and
     * copies the returned {@link ByteBuf} via a {@link FileChannel}. Override
     * in storage backends that can stream natively (e.g.
     * {@link S3ObjectStorage} uses {@code AsyncResponseTransformer.toFile()}).
     *
     * @param path   object path (same key space as {@link #read(String)})
     * @param dest   destination file path
     * @param append when {@code true} content is appended; when {@code false}
     *               the file is created or replaced
     * @return a future that completes when the write is done; completes
     *         exceptionally if the object does not exist or an I/O error occurs
     */
    default CompletableFuture<Void> downloadToFile(String path, Path dest, boolean append) {
        return read(path).thenApply(result -> {
            // Guard: do NOT open or truncate the destination file when the object is absent.
            // Checking status before opening preserves dest integrity on NOT_FOUND.
            if (result.status() == ReadResult.Status.NOT_FOUND) {
                throw new UncheckedIOException(
                        new IOException("object not found: " + path));
            }
            ByteBuf buf = result.byteBuf();
            try {
                StandardOpenOption[] opts = append
                        ? new StandardOpenOption[]{
                                StandardOpenOption.WRITE,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.APPEND}
                        : new StandardOpenOption[]{
                                StandardOpenOption.WRITE,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.TRUNCATE_EXISTING};
                try (FileChannel ch = FileChannel.open(dest, opts)) {
                    // nioBuffer() returns a ByteBuffer view without heap copying
                    // for both direct and array-backed Netty buffers.
                    ByteBuffer nioBuf = buf.nioBuffer(buf.readerIndex(), buf.readableBytes());
                    while (nioBuf.hasRemaining()) {
                        ch.write(nioBuf);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } finally {
                result.release();
            }
            return (Void) null;
        });
    }

    /**
     * Uploads {@code source} as the single object stored at {@code path},
     * returning the number of bytes written. Backends that support the S3
     * Multipart Upload API (e.g. {@link S3ObjectStorage} via
     * {@code S3TransferManager.uploadFile(...)}) override this to pipeline
     * parts in parallel and stream the file zero-copy from disk.
     *
     * <p>Single-object layout (issue #650): the resulting storage object
     * lives at {@code path} itself — no {@code .multipart/{N}} suffix, no
     * {@code .bulk} suffix. {@code uploadFile} is the only multipart-file
     * write path; the per-block {@code writeBlock} API was removed in #650.
     *
     * <p>The default implementation reads the whole file into a heap byte
     * array and calls {@link #write(String, byte[])} — fine for unit tests
     * and small files, but a real backend should override.
     *
     * @param path     logical object path (this is the final key, no suffix is added)
     * @param source   local file to upload; must exist and be readable for the
     *                 entire duration of the upload
     * @param progress invoked with the number of bytes uploaded since the
     *                 last invocation (a delta, not a running total). May be
     *                 {@code null}.
     * @return a future that completes with the total bytes uploaded
     */
    default CompletableFuture<Long> uploadFile(String path, Path source,
                                               java.util.function.LongConsumer progress) {
        try {
            byte[] content = java.nio.file.Files.readAllBytes(source);
            return write(path, content).thenApply(v -> {
                if (progress != null && content.length > 0) {
                    progress.accept(content.length);
                }
                return (long) content.length;
            });
        } catch (IOException e) {
            CompletableFuture<Long> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    /**
     * Tri-state single-key existence probe.
     *
     * <p>Backends with a cheap presence probe (S3 {@code HEAD}, local file
     * {@code exists()}) should override; the default falls back to
     * {@link #read(String)} which materialises the whole object — correct,
     * but inefficient.
     *
     * <p><b>Contract (issue #645):</b> the returned future has exactly three
     * terminal states, and callers MUST honour the distinction:
     * <ul>
     *   <li>completes with {@code true} — the object is present;</li>
     *   <li>completes with {@code false} — the object is <em>definitively</em>
     *       absent (e.g. S3 {@code NoSuchKey} / HTTP 404, local file does not
     *       exist);</li>
     *   <li>completes exceptionally — the existence could not be determined
     *       (transient I/O error, network timeout, HTTP 5xx, generic SDK
     *       error).</li>
     * </ul>
     */
    default CompletableFuture<Boolean> existsObject(String path) {
        return read(path).thenApply(result -> {
            ReadResult.Status status = result.status();
            result.release();
            if (status == ReadResult.Status.FOUND) {
                return Boolean.TRUE;
            }
            if (status == ReadResult.Status.NOT_FOUND) {
                return Boolean.FALSE;
            }
            // Defensive: any future ReadResult.Status that is neither FOUND
            // nor NOT_FOUND is a transient/unknown condition. Surface as an
            // exceptional completion so the caller can disambiguate (see
            // issue #645 contract above).
            throw new java.util.concurrent.CompletionException(
                    new java.io.IOException(
                            "existsObject probe returned unrecognised status "
                                    + status + " for path=" + path));
        });
    }

    /**
     * Downloads the single object at {@code path} directly to {@code dest},
     * replacing any existing file. Symmetric to
     * {@link #uploadFile(String, Path, java.util.function.LongConsumer)}.
     *
     * <p>The default implementation delegates to
     * {@link #downloadToFile(String, Path, boolean)} with {@code append=false};
     * S3 overrides with {@code S3TransferManager.downloadFile(...)} so large
     * objects stream zero-copy via CRT.
     */
    default CompletableFuture<Void> downloadFileBulk(String path, Path dest) {
        return downloadToFile(path, dest, false);
    }

    @Override
    void close() throws Exception;
}
