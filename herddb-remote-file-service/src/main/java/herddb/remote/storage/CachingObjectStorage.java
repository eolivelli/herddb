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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.util.concurrent.Striped;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Decorator that wraps an inner {@link ObjectStorage} (e.g. S3) with a local disk cache.
 * <p>
 * The cache is a pure on-disk LRU: byte[] values are NOT retained on the JVM heap.
 * Hot-data speed comes from the OS page cache, which manages its own memory pressure.
 * A Caffeine {@link Cache} indexes the disk tier: keys are object paths, values are the
 * on-disk file size in bytes. The cache is weight-bounded by {@code maxCacheBytes}
 * (the <b>disk</b> budget), and its eviction listener deletes the backing file.
 * <p>
 * Concurrent readers for the same missing key are deduplicated via an in-flight map so
 * only one {@code inner.read()} fires. Stripe-based read/write locks serialise disk
 * reads against evictions so that a deletion never races a concurrent reader.
 * <p>
 * The cache directory is cleared on construction. Cache files are stored flat in
 * {@code cacheDir} with {@code /} replaced by {@code _} in filenames.
 * <p>
 * Metrics (issue #336): Caffeine {@code recordStats()} is enabled so callers can query
 * hit/miss/eviction counts via {@link #diskLruStats()}. Byte-level hit/miss counters are
 * maintained via {@link AtomicLong} fields incremented in {@link #readRange}. The maximum
 * cache weight can be changed at runtime via {@link #setMaxCacheBytes(long)}.
 *
 * @author enrico.olivelli
 */
public class CachingObjectStorage implements ObjectStorage {

    private static final Logger LOGGER = Logger.getLogger(CachingObjectStorage.class.getName());
    private static final int STRIPES = 256;

    private final ObjectStorage inner;
    private final Path cacheDir;
    private final ExecutorService executor;
    private final Cache<String, Long> diskLru;
    private final Striped<ReadWriteLock> locks = Striped.lazyWeakReadWriteLock(STRIPES);
    private final ConcurrentHashMap<String, CompletableFuture<ByteBuf>> inFlightReads = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Void>> inFlightWrites = new ConcurrentHashMap<>();

    /**
     * Bytes served from the on-disk cache in {@link #readRange} calls.
     * Incremented on a cache hit in the readRange hot path (issue #336).
     */
    private final AtomicLong hitBytes = new AtomicLong();

    /**
     * Bytes fetched from inner storage in {@link #readRange} calls (cache miss path).
     * Incremented when readRange falls through to the inner storage (issue #336).
     */
    private final AtomicLong missBytes = new AtomicLong();

    public CachingObjectStorage(ObjectStorage inner, Path cacheDir, ExecutorService executor,
                                long maxCacheBytes) throws IOException {
        this.inner = inner;
        this.cacheDir = cacheDir;
        this.executor = executor;
        clearCacheDir();
        Files.createDirectories(cacheDir);

        this.diskLru = Caffeine.newBuilder()
                .maximumWeight(maxCacheBytes)
                .weigher((String key, Long size) -> {
                    long s = size == null ? 0L : size;
                    return s > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) s;
                })
                // recordStats() is required for hit/miss/eviction counters exposed via
                // diskLruStats(). The cost is negligible (a few AtomicLong increments
                // per cache operation).
                .recordStats()
                .evictionListener((RemovalListener<String, Long>) (key, value, cause) -> {
                    // Synchronous, best-effort delete. Runs on Caffeine's maintenance thread
                    // (or inline on a put() that triggers eviction) — does NOT take the stripe
                    // lock, because the stripe lock might already be held by the same thread
                    // inside admitToDisk. Concurrent readers that lose the race get
                    // NoSuchFileException and fall through to inner.read() via the
                    // tryReadFromDisk / tryReadSliceFromDisk null-return contract.
                    if (key != null) {
                        deleteCacheFile(key);
                    }
                })
                .build();
    }

    // -----------------------------------------------------------------------
    // Stats and dynamic resize (issue #336)
    // -----------------------------------------------------------------------

    /**
     * Returns a snapshot of Caffeine's stats for the disk-LRU index cache.
     * Provides cumulative hit, miss, eviction and eviction-weight counts since pod start.
     * Safe to call from gauge samplers (lock-free read of Caffeine's internal counters).
     */
    public CacheStats diskLruStats() {
        return diskLru.stats();
    }

    /**
     * Bytes served from the on-disk cache across all {@link #readRange} calls since pod start.
     * Incremented once per successful disk-cache hit, before the I/O completes, so the counter
     * counts <em>intended</em> hit bytes (matching the {@code length} parameter of readRange).
     */
    public long getHitBytes() {
        return hitBytes.get();
    }

    /**
     * Bytes fetched from the inner storage (GCS / S3 fallback) across all {@link #readRange}
     * calls since pod start. Incremented on every cache miss.
     */
    public long getMissBytes() {
        return missBytes.get();
    }

    /**
     * Approximate number of entries currently tracked in the disk-LRU index.
     * Safe to call from gauge samplers (Caffeine's {@code estimatedSize()} is thread-safe).
     */
    public long estimatedEntries() {
        return diskLru.estimatedSize();
    }

    /**
     * Returns the current maximum weight (bytes) of the disk-cache LRU as reported
     * by Caffeine's eviction policy. Returns 0 if the policy is not weight-based.
     */
    public long getMaxCacheBytes() {
        return diskLru.policy().eviction()
                .map(e -> e.getMaximum())
                .orElse(0L);
    }

    /**
     * Dynamically resizes the disk-cache LRU to {@code newMaxBytes}.
     * <p>
     * The change is applied immediately via Caffeine's live policy API.
     * If {@code newMaxBytes} is smaller than the current total weight, Caffeine will
     * evict excess entries lazily on the next cache access or maintenance cycle.
     * <p>
     * <b>This change is NOT persistent</b>: a pod restart will revert to the value
     * configured in {@code fileserver.properties} / Helm values (issue #336).
     *
     * @param newMaxBytes new maximum weight in bytes; must be &gt; 0
     * @return the previous maximum, or 0 if the policy is not available
     */
    public long setMaxCacheBytes(long newMaxBytes) {
        long[] prev = {0L};
        diskLru.policy().eviction().ifPresent(e -> {
            prev[0] = e.getMaximum();
            e.setMaximum(newMaxBytes);
        });
        LOGGER.log(Level.INFO,
                "Disk cache resized: previousMax={0} bytes, newMax={1} bytes",
                new Object[]{prev[0], newMaxBytes});
        return prev[0];
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private void clearCacheDir() throws IOException {
        if (Files.exists(cacheDir)) {
            try (Stream<Path> stream = Files.walk(cacheDir)) {
                stream.sorted((a, b) -> -a.compareTo(b))
                        .filter(p -> !p.equals(cacheDir))
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (IOException e) {
                                LOGGER.log(Level.WARNING, "Failed to delete cache file: " + p, e);
                            }
                        });
            }
        }
    }

    Path cacheFilePath(String path) {
        String safeName = path.replace('/', '_').replace('\\', '_');
        return cacheDir.resolve(safeName);
    }

    /**
     * Asynchronously writes {@code bytes} to a cache file using {@link AsynchronousFileChannel}.
     * Writes to a temp file then atomically renames it.
     */
    private CompletableFuture<Void> writeCacheFileAsync(String path, byte[] bytes) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Path target = cacheFilePath(path);
        Path tmp = cacheDir.resolve(target.getFileName() + ".tmp");

        try {
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(
                    tmp, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            channel.write(buffer, 0, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer bytesWritten, Void attachment) {
                    try {
                        channel.close();
                        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                        result.complete(null);
                    } catch (Throwable t) {
                        deleteCacheFile(path);
                        result.completeExceptionally(t);
                    }
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                    }
                    deleteCacheFile(path);
                    result.completeExceptionally(exc);
                }
            });
        } catch (Throwable t) {
            deleteCacheFile(path);
            result.completeExceptionally(t);
        }

        return result;
    }

    /**
     * Asynchronously writes {@code buf}'s readable bytes to a cache file using
     * {@link AsynchronousFileChannel}. The caller must hold a refcount on
     * {@code buf}; this method releases that refcount when the write completes
     * (success or failure). The buffer's reader/writer indices are not modified.
     */
    private CompletableFuture<Void> writeCacheFileAsync(String path, ByteBuf buf) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Path target = cacheFilePath(path);
        Path tmp = cacheDir.resolve(target.getFileName() + ".tmp");
        int len = buf.readableBytes();

        try {
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(
                    tmp, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            // nioBuffer on a direct pooled ByteBuf returns a view, no copy.
            ByteBuffer nio = buf.nioBuffer(buf.readerIndex(), len);
            channel.write(nio, 0, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer bytesWritten, Void attachment) {
                    try {
                        channel.close();
                        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                        result.complete(null);
                    } catch (Throwable t) {
                        deleteCacheFile(path);
                        result.completeExceptionally(t);
                    } finally {
                        buf.release();
                    }
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                    }
                    deleteCacheFile(path);
                    buf.release();
                    result.completeExceptionally(exc);
                }
            });
        } catch (Throwable t) {
            deleteCacheFile(path);
            buf.release();
            result.completeExceptionally(t);
        }

        return result;
    }

    private void deleteCacheFile(String path) {
        try {
            Files.deleteIfExists(cacheFilePath(path));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to delete cache file for path: " + path, e);
        }
    }

    /**
     * Asynchronously stores {@code content} on disk and registers it in the disk LRU.
     * Returns a CompletableFuture that completes when the write is done.
     * Deduplicates concurrent writes to the same path via an in-flight map.
     */
    private CompletableFuture<Void> admitToDisk(String path, byte[] content) {
        CompletableFuture<Void> pending = new CompletableFuture<>();
        CompletableFuture<Void> existing = inFlightWrites.putIfAbsent(path, pending);
        if (existing != null) {
            // Another thread is already writing this path; attach to their future
            return existing;
        }

        writeCacheFileAsync(path, content).whenComplete((v, err) -> {
            try {
                if (err != null) {
                    pending.completeExceptionally(err);
                    return;
                }
                diskLru.put(path, (long) content.length);
                pending.complete(null);
            } finally {
                inFlightWrites.remove(path, pending);
            }
        });

        return pending;
    }

    /**
     * ByteBuf overload of {@link #admitToDisk(String, byte[])}. The caller must hold
     * a refcount on {@code buf}; this method releases that refcount when the disk
     * write completes (success or failure). The buffer's reader/writer indices are
     * not modified.
     */
    private CompletableFuture<Void> admitToDisk(String path, ByteBuf buf) {
        CompletableFuture<Void> pending = new CompletableFuture<>();
        CompletableFuture<Void> existing = inFlightWrites.putIfAbsent(path, pending);
        if (existing != null) {
            // Another thread is already writing this path; release our retain and attach.
            buf.release();
            return existing;
        }

        int size = buf.readableBytes();
        writeCacheFileAsync(path, buf).whenComplete((v, err) -> {
            try {
                if (err != null) {
                    pending.completeExceptionally(err);
                    return;
                }
                diskLru.put(path, (long) size);
                pending.complete(null);
            } finally {
                inFlightWrites.remove(path, pending);
            }
        });

        return pending;
    }

    // -----------------------------------------------------------------------
    // ObjectStorage implementation
    // -----------------------------------------------------------------------

    @Override
    public CompletableFuture<Void> write(String path, byte[] content) {
        return inner.write(path, content).thenCompose(v -> admitToDisk(path, content));
    }

    @Override
    public CompletableFuture<Void> writeBlock(String path, long blockIndex, byte[] content) {
        String blockPath = path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex;
        return inner.writeBlock(path, blockIndex, content).thenCompose(v -> admitToDisk(blockPath, content));
    }

    @Override
    public CompletableFuture<ReadResult> read(String path) {
        return tryReadFromDiskAsync(path)
                .thenCompose(cached -> {
                    if (cached != null) {
                        return CompletableFuture.completedFuture(cached);
                    }
                    return loadAndCache(path);
                });
    }

    /**
     * Asynchronously attempts to read the full file from the on-disk cache using
     * {@link AsynchronousFileChannel}. Returns a future that completes with {@code null}
     * on miss (including {@link NoSuchFileException} from a concurrent eviction).
     * Uses an optimistic lock-free approach: open the file directly; if it doesn't exist
     * or is being evicted, fall through to the inner storage.
     *
     * Uses Netty pooled direct ByteBuf for zero-copy I/O. Caller MUST call
     * {@link ReadResult#release()} when done to return the buffer to the pool.
     */
    private CompletableFuture<ReadResult> tryReadFromDiskAsync(String path) {
        if (diskLru.getIfPresent(path) == null) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<ReadResult> result = new CompletableFuture<>();
        try {
            Path filePath = cacheFilePath(path);
            // Open file directly; catch NoSuchFileException for eviction races
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(filePath, StandardOpenOption.READ);
            long fileSize = channel.size();
            // Allocate direct pooled ByteBuf for zero-copy efficient I/O
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer((int) fileSize);
            ByteBuffer nioBuffer = byteBuf.nioBuffer(0, (int) fileSize);

            channel.read(nioBuffer, 0, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer bytesRead, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                    }
                    byteBuf.writerIndex(bytesRead);
                    result.complete(ReadResult.found(byteBuf));
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                    }
                    // Release pooled buffer on failure
                    byteBuf.release();
                    // Treat as cache miss; fall through to inner storage
                    result.complete(null);
                }
            });
        } catch (NoSuchFileException e) {
            // File was evicted after the getIfPresent check; treat as cache miss
            result.complete(null);
        } catch (Throwable t) {
            LOGGER.log(Level.WARNING, "Failed to read cache file for path: " + path, t);
            result.complete(null);
        }

        return result;
    }

    /**
     * Loads {@code path} from the inner storage, deduplicating concurrent loads of the
     * same key. On a successful load the bytes are written to disk and registered in
     * the LRU before being returned.
     * <p>
     * The pooled {@link ByteBuf} from the inner storage is kept end-to-end: it is
     * shared by the disk-write path and by every concurrent caller (each gets a
     * {@code retainedDuplicate} via {@code pending.thenApply}). No heap copy.
     */
    private CompletableFuture<ReadResult> loadAndCache(String path) {
        CompletableFuture<ByteBuf> pending = new CompletableFuture<>();
        // Register the wrapDuplicate dependent BEFORE issuing inner.read. inner.read
        // may complete synchronously (e.g. test fakes returning a completed future),
        // and the disk-write completion runs on an I/O thread that races with this
        // method's return. Keeping the dependent attached up front guarantees it
        // fires inside pending.complete — while the alive-for-followers retain is
        // still held — instead of after the alive retain has already been released.
        CompletableFuture<ReadResult> resultFuture = pending.thenApply(CachingObjectStorage::wrapDuplicate);

        CompletableFuture<ByteBuf> existing = inFlightReads.putIfAbsent(path, pending);
        if (existing != null) {
            return existing.thenApply(CachingObjectStorage::wrapDuplicate);
        }
        inner.read(path).whenComplete((result, err) -> {
            if (err != null) {
                try {
                    pending.completeExceptionally(err);
                } finally {
                    inFlightReads.remove(path, pending);
                }
                return;
            }
            if (result.status() != ReadResult.Status.FOUND) {
                try {
                    pending.complete(null);
                } finally {
                    inFlightReads.remove(path, pending);
                }
                return;
            }
            ByteBuf buf = result.byteBuf();
            // Two extra retains:
            //   +1 for admitToDisk (released inside writeCacheFileAsync when the disk write finishes).
            //   +1 to keep buf alive across pending.complete so all dependents
            //      (original caller + any in-flight followers) can retainedDuplicate.
            // The original `result`'s refcount is dropped right after.
            buf.retain(2);
            CompletableFuture<Void> diskWrite = admitToDisk(path, buf);
            try {
                diskWrite.whenComplete((v, e) -> {
                    try {
                        if (e != null) {
                            try {
                                pending.completeExceptionally(e);
                            } finally {
                                inFlightReads.remove(path, pending);
                            }
                            return;
                        }
                        try {
                            // pending.complete fires all dependents synchronously here:
                            // each does retainedDuplicate(buf), bumping refcount per caller.
                            pending.complete(buf);
                        } finally {
                            inFlightReads.remove(path, pending);
                        }
                    } finally {
                        // Drop the alive-for-followers retain. Each successful follower
                        // already holds its own retainedDuplicate; failed completion has
                        // no followers to keep alive.
                        buf.release();
                    }
                });
            } finally {
                result.release();
            }
        });
        return resultFuture;
    }

    private static ReadResult wrapDuplicate(ByteBuf buf) {
        if (buf == null) {
            return ReadResult.notFound();
        }
        return ReadResult.found(buf.retainedDuplicate());
    }

    /**
     * Serves a block slice from the disk cache (hit path) or from the inner storage
     * (miss path), updating hit/miss byte counters for Prometheus metrics (issue #336).
     */
    @Override
    public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
        long blockIndex = offset / blockSize;
        int offsetInBlock = (int) (offset % blockSize);
        String blockPath = path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex;
        return tryReadSliceFromDiskAsync(blockPath, offsetInBlock, length)
                .thenCompose(cached -> {
                    if (cached != null) {
                        // Cache HIT: increment byte counter for Prometheus gauge.
                        hitBytes.addAndGet(length);
                        return CompletableFuture.completedFuture(cached);
                    }
                    // Cache MISS: increment byte counter and fall through to inner storage.
                    missBytes.addAndGet(length);
                    return loadAndCache(blockPath).thenApply(full -> sliceFromFull(full, offsetInBlock, length));
                });
    }

    /**
     * Asynchronously reads only the requested slice from the on-disk block file using
     * {@link AsynchronousFileChannel}. Returns a future that completes with {@code null}
     * on miss (including {@link NoSuchFileException} from a concurrent eviction).
     * Uses an optimistic lock-free approach: open the file directly; if it doesn't exist,
     * fall through to the inner storage.
     *
     * Uses Netty pooled direct ByteBuf for zero-copy I/O. Caller MUST call
     * {@link ReadResult#release()} when done to return the buffer to the pool.
     */
    private CompletableFuture<ReadResult> tryReadSliceFromDiskAsync(String blockPath, int offsetInBlock, int length) {
        Long size = diskLru.getIfPresent(blockPath);
        if (size == null) {
            return CompletableFuture.completedFuture(null);
        }

        long blockLength = size;
        if (offsetInBlock >= blockLength) {
            return CompletableFuture.completedFuture(ReadResult.notFound());
        }

        int readable = (int) Math.min(length, blockLength - offsetInBlock);
        CompletableFuture<ReadResult> result = new CompletableFuture<>();

        try {
            Path filePath = cacheFilePath(blockPath);
            // Open file directly; catch NoSuchFileException for eviction races
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(filePath, StandardOpenOption.READ);
            // Allocate direct pooled ByteBuf for zero-copy efficient I/O
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(readable);
            ByteBuffer nioBuffer = byteBuf.nioBuffer(0, readable);

            channel.read(nioBuffer, offsetInBlock, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer bytesRead, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                    }
                    byteBuf.writerIndex(bytesRead);
                    result.complete(ReadResult.found(byteBuf));
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                    }
                    // Release pooled buffer on failure
                    byteBuf.release();
                    // Treat as cache miss; fall through to inner storage
                    result.complete(null);
                }
            });
        } catch (NoSuchFileException e) {
            // File was evicted after the size check; treat as cache miss
            result.complete(null);
        } catch (Throwable t) {
            LOGGER.log(Level.WARNING, "Failed to read slice from cache file for path: " + blockPath, t);
            result.complete(null);
        }

        return result;
    }

    private static ReadResult sliceFromFull(ReadResult full, int offsetInBlock, int length) {
        if (full.status() == ReadResult.Status.NOT_FOUND) {
            full.release();
            return ReadResult.notFound();
        }
        try {
            ByteBuf blockBuf = full.byteBuf();
            int blockLength = blockBuf.readableBytes();
            if (offsetInBlock >= blockLength) {
                return ReadResult.notFound();
            }
            int to = Math.min(offsetInBlock + length, blockLength);
            int sliceLength = to - offsetInBlock;
            // retainedSlice shares the underlying memory; no copy.
            return ReadResult.found(blockBuf.retainedSlice(blockBuf.readerIndex() + offsetInBlock, sliceLength));
        } finally {
            full.release();
        }
    }

    @Override
    public CompletableFuture<Boolean> deleteLogical(String path) {
        String multipartPrefix = path + ObjectStorage.MULTIPART_SUFFIX + "/";
        List<String> toInvalidate = new ArrayList<>(diskLru.asMap().keySet());
        toInvalidate.stream()
                .filter(k -> k.equals(path) || k.startsWith(multipartPrefix))
                .forEach(this::invalidateAndDelete);
        return inner.deleteLogical(path);
    }

    /**
     * Explicit invalidation path: removes the entry from the LRU and deletes the on-disk file.
     * Caffeine's {@code evictionListener} only fires for size/time-based eviction, not for
     * explicit {@code invalidate()}, so we must unlink the file ourselves here.
     */
    private void invalidateAndDelete(String path) {
        diskLru.invalidate(path);
        deleteCacheFile(path);
    }

    @Override
    public CompletableFuture<List<String>> listLogical(String prefix) {
        return inner.listLogical(prefix);
    }

    @Override
    public CompletableFuture<Boolean> delete(String path) {
        invalidateAndDelete(path);
        return inner.delete(path);
    }

    @Override
    public CompletableFuture<List<String>> list(String prefix) {
        return inner.list(prefix);
    }

    @Override
    public CompletableFuture<Integer> deleteByPrefix(String prefix) {
        return inner.deleteByPrefix(prefix).thenApply(count -> {
            List<String> keysToInvalidate = new ArrayList<>(diskLru.asMap().keySet());
            keysToInvalidate.stream()
                    .filter(k -> k.startsWith(prefix))
                    .forEach(this::invalidateAndDelete);
            return count;
        });
    }

    /**
     * Triggers pending Caffeine maintenance (evictions, removal listeners).
     * Package-private for use in tests.
     */
    void cleanUp() {
        diskLru.cleanUp();
    }

    @Override
    public void close() throws Exception {
        inner.close();
    }
}
