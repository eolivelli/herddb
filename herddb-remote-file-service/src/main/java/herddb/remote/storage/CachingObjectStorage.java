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
import com.google.common.util.concurrent.Striped;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
    private final ConcurrentHashMap<String, CompletableFuture<ReadResult>> inFlight = new ConcurrentHashMap<>();

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

    private void writeCacheFile(String path, byte[] bytes) throws IOException {
        Path target = cacheFilePath(path);
        Path tmp = cacheDir.resolve(target.getFileName() + ".tmp");
        Files.write(tmp, bytes);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private void deleteCacheFile(String path) {
        try {
            Files.deleteIfExists(cacheFilePath(path));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to delete cache file for path: " + path, e);
        }
    }

    /**
     * Stores {@code content} on disk and registers it in the disk LRU.
     * Must be called <b>without</b> holding any stripe lock.
     */
    private void admitToDisk(String path, byte[] content) {
        ReadWriteLock lock = locks.get(path);
        lock.writeLock().lock();
        try {
            writeCacheFile(path, content);
            diskLru.put(path, (long) content.length);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to write cache file for path: " + path, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Void> write(String path, byte[] content) {
        return inner.write(path, content).thenRun(() -> admitToDisk(path, content));
    }

    @Override
    public CompletableFuture<Void> writeBlock(String path, long blockIndex, byte[] content) {
        String blockPath = path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex;
        return inner.writeBlock(path, blockIndex, content).thenRun(() -> admitToDisk(blockPath, content));
    }

    @Override
    public CompletableFuture<ReadResult> read(String path) {
        return CompletableFuture.supplyAsync(() -> tryReadFromDisk(path), executor)
                .thenCompose(cached -> {
                    if (cached != null) {
                        return CompletableFuture.completedFuture(cached);
                    }
                    return loadAndCache(path);
                });
    }

    /**
     * Attempts to read the full file from the on-disk cache. Returns {@code null} on miss
     * (including {@link NoSuchFileException} from a concurrent eviction).
     */
    private ReadResult tryReadFromDisk(String path) {
        if (diskLru.getIfPresent(path) == null) {
            return null;
        }
        ReadWriteLock lock = locks.get(path);
        lock.readLock().lock();
        try {
            // Re-check under the lock: an evictor may have raced us between getIfPresent
            // and acquiring the read lock.
            if (diskLru.getIfPresent(path) == null) {
                return null;
            }
            byte[] bytes = Files.readAllBytes(cacheFilePath(path));
            return ReadResult.found(bytes);
        } catch (NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to read cache file for path: " + path, e);
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Loads {@code path} from the inner storage, deduplicating concurrent loads of the
     * same key. On a successful load the bytes are written to disk and registered in
     * the LRU before being returned.
     */
    private CompletableFuture<ReadResult> loadAndCache(String path) {
        CompletableFuture<ReadResult> pending = new CompletableFuture<>();
        CompletableFuture<ReadResult> existing = inFlight.putIfAbsent(path, pending);
        if (existing != null) {
            return existing;
        }
        inner.read(path).whenComplete((result, err) -> {
            try {
                if (err != null) {
                    pending.completeExceptionally(err);
                    return;
                }
                if (result.status() == ReadResult.Status.FOUND) {
                    admitToDisk(path, result.content());
                }
                pending.complete(result);
            } finally {
                inFlight.remove(path, pending);
            }
        });
        return pending;
    }

    @Override
    public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
        long blockIndex = offset / blockSize;
        int offsetInBlock = (int) (offset % blockSize);
        String blockPath = path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex;
        return CompletableFuture.supplyAsync(() -> tryReadSliceFromDisk(blockPath, offsetInBlock, length), executor)
                .thenCompose(cached -> {
                    if (cached != null) {
                        return CompletableFuture.completedFuture(cached);
                    }
                    return loadAndCache(blockPath).thenApply(full -> sliceFromFull(full, offsetInBlock, length));
                });
    }

    /**
     * Reads only the requested slice from the on-disk block file via {@link FileChannel}.
     * Returns {@code null} on miss; callers fall through to a full-block fetch.
     */
    private ReadResult tryReadSliceFromDisk(String blockPath, int offsetInBlock, int length) {
        Long size = diskLru.getIfPresent(blockPath);
        if (size == null) {
            return null;
        }
        ReadWriteLock lock = locks.get(blockPath);
        lock.readLock().lock();
        try {
            Long s = diskLru.getIfPresent(blockPath);
            if (s == null) {
                return null;
            }
            long blockLength = s;
            if (offsetInBlock >= blockLength) {
                return ReadResult.notFound();
            }
            int readable = (int) Math.min(length, blockLength - offsetInBlock);
            ByteBuffer buffer = ByteBuffer.allocate(readable);
            try (FileChannel channel = FileChannel.open(cacheFilePath(blockPath), StandardOpenOption.READ)) {
                int pos = 0;
                while (pos < readable) {
                    int n = channel.read(buffer, offsetInBlock + pos);
                    if (n < 0) {
                        break;
                    }
                    pos += n;
                }
                if (pos < readable) {
                    byte[] truncated = new byte[pos];
                    buffer.flip();
                    buffer.get(truncated);
                    return ReadResult.found(truncated);
                }
            }
            return ReadResult.found(buffer.array());
        } catch (NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to read slice from cache file for path: " + blockPath, e);
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    private static ReadResult sliceFromFull(ReadResult full, int offsetInBlock, int length) {
        if (full.status() == ReadResult.Status.NOT_FOUND) {
            return ReadResult.notFound();
        }
        byte[] blockBytes = full.content();
        if (offsetInBlock >= blockBytes.length) {
            return ReadResult.notFound();
        }
        int to = Math.min(offsetInBlock + length, blockBytes.length);
        byte[] slice = new byte[to - offsetInBlock];
        System.arraycopy(blockBytes, offsetInBlock, slice, 0, slice.length);
        return ReadResult.found(slice);
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
