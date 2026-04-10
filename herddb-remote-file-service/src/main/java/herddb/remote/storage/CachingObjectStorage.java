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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Two-tier cache decorator for {@link ObjectStorage} (e.g. S3 / MinIO).
 *
 * <h3>Caching tiers</h3>
 * <ol>
 *   <li><b>L1 – JVM heap</b>: a Caffeine {@link AsyncLoadingCache} limited to
 *       {@code maxCacheBytes} of raw byte array weight.  Items are kept in heap
 *       for the fastest possible access; they are evicted when the total weight
 *       exceeds the limit.</li>
 *   <li><b>L2 – local disk</b>: when a block is first fetched from object storage
 *       it is written to a flat file under {@code cacheDir}.  The disk file is
 *       <em>not</em> deleted when the item is evicted from the heap cache — it is
 *       retained as a disk-backed fallback.  On a subsequent Caffeine miss the
 *       loader reads the disk file first (local I/O, typically ≥ 100 MB/s) instead
 *       of re-fetching from MinIO (~4 MB/s in a local Docker cluster).</li>
 *   <li><b>L3 – object storage (MinIO)</b>: fallback when neither heap nor disk
 *       has the item.</li>
 * </ol>
 *
 * <h3>Why disk files are retained on heap eviction</h3>
 * The previous implementation deleted disk files in the Caffeine eviction listener.
 * This made the "disk cache" a pure mirror of the heap cache rather than an
 * independent tier: once an item was evicted from heap (and its disk file removed),
 * the next access had to re-fetch from MinIO, causing repeated multi-second stalls
 * for large FusedPQ graph blocks during vector-search benchmarks.
 *
 * <h3>Disk cache lifecycle</h3>
 * <ul>
 *   <li>Created/updated: whenever a block is written ({@link #writeBlock}) or a
 *       small object is written ({@link #write}), and whenever the L3 loader fetches
 *       from MinIO ({@link #loadFromObjectStorage}).</li>
 *   <li>Deleted: only via an explicit {@link #deleteLogical}, {@link #delete}, or
 *       {@link #deleteByPrefix} call, or when the cache directory is cleared on
 *       construction ({@link #clearCacheDir}).</li>
 * </ul>
 *
 * Cache files are stored flat in {@code cacheDir} with {@code /} replaced by
 * {@code _} in filenames.
 *
 * @author enrico.olivelli
 */
public class CachingObjectStorage implements ObjectStorage {

    private static final Logger LOGGER = Logger.getLogger(CachingObjectStorage.class.getName());

    private final ObjectStorage inner;
    private final Path cacheDir;
    private final AsyncLoadingCache<String, byte[]> cache;

    public CachingObjectStorage(ObjectStorage inner, Path cacheDir, ExecutorService executor,
                                long maxCacheBytes) throws IOException {
        this.inner = inner;
        this.cacheDir = cacheDir;
        clearCacheDir();
        Files.createDirectories(cacheDir);

        this.cache = Caffeine.newBuilder()
                .maximumWeight(maxCacheBytes)
                .weigher((String key, byte[] value) -> value.length)
                // Do NOT delete disk files on heap eviction: the disk file continues
                // to serve as an L2 cache, sparing a MinIO round-trip on the next access.
                .buildAsync((path, exec) -> loadFromDiskOrObjectStorage(path, executor));
    }

    /**
     * L2/L3 loader: try the local disk cache first; fall back to object storage if
     * the disk file does not exist or cannot be read.
     */
    private CompletableFuture<byte[]> loadFromDiskOrObjectStorage(String path, ExecutorService executor) {
        Path diskFile = cacheFilePath(path);
        if (Files.exists(diskFile)) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    byte[] bytes = Files.readAllBytes(diskFile);
                    LOGGER.fine(() -> "cache L2 hit (disk): " + path + " (" + bytes.length + " bytes)");
                    return bytes;
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Failed to read disk cache file, falling back to object storage: " + path, e);
                    return null;
                }
            }, executor).thenCompose(bytes -> {
                if (bytes != null) {
                    return CompletableFuture.completedFuture(bytes);
                }
                return loadFromObjectStorage(path);
            });
        }
        return loadFromObjectStorage(path);
    }

    /**
     * L3 loader: fetch from object storage (MinIO) and persist to disk for future L2 hits.
     */
    private CompletableFuture<byte[]> loadFromObjectStorage(String path) {
        return inner.read(path).thenApply(result -> {
            if (result.status() == ReadResult.Status.NOT_FOUND) {
                return null;
            }
            byte[] bytes = result.content();
            writeCacheFile(path, bytes);
            return bytes;
        });
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

    private void writeCacheFile(String path, byte[] bytes) {
        try {
            Path target = cacheFilePath(path);
            Path tmp = cacheDir.resolve(target.getFileName() + ".tmp");
            Files.write(tmp, bytes);
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to write cache file for path: " + path, e);
        }
    }

    private void deleteCacheFile(String path) {
        try {
            Files.deleteIfExists(cacheFilePath(path));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to delete cache file for path: " + path, e);
        }
    }

    @Override
    public CompletableFuture<Void> write(String path, byte[] content) {
        return inner.write(path, content).thenRun(() -> {
            writeCacheFile(path, content);
            cache.put(path, CompletableFuture.completedFuture(content));
        });
    }

    @Override
    public CompletableFuture<ReadResult> read(String path) {
        return cache.get(path).thenApply(bytes -> {
            if (bytes == null) {
                return ReadResult.notFound();
            }
            return ReadResult.found(bytes);
        });
    }

    @Override
    public CompletableFuture<Void> writeBlock(String path, long blockIndex, byte[] content) {
        String blockPath = path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex;
        return inner.writeBlock(path, blockIndex, content).thenRun(() -> {
            writeCacheFile(blockPath, content);
            cache.put(blockPath, CompletableFuture.completedFuture(content));
        });
    }

    @Override
    public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
        long blockIndex = offset / blockSize;
        int offsetInBlock = (int) (offset % blockSize);
        String blockPath = path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex;
        // Load the whole block from L1 heap (or L2 disk, or L3 MinIO), then slice
        return cache.get(blockPath).thenApply(blockBytes -> {
            if (blockBytes == null) {
                return ReadResult.notFound();
            }
            int from = offsetInBlock;
            int to = Math.min(from + length, blockBytes.length);
            if (from >= blockBytes.length) {
                return ReadResult.notFound();
            }
            byte[] slice = new byte[to - from];
            System.arraycopy(blockBytes, from, slice, 0, slice.length);
            return ReadResult.found(slice);
        });
    }

    @Override
    public CompletableFuture<Boolean> deleteLogical(String path) {
        // Invalidate all cached entries for this logical file and delete disk files
        String multipartPrefix = path + ObjectStorage.MULTIPART_SUFFIX + "/";
        List<String> toInvalidate = new ArrayList<>(cache.synchronous().asMap().keySet());
        toInvalidate.stream()
                .filter(k -> k.equals(path) || k.startsWith(multipartPrefix))
                .forEach(k -> {
                    cache.synchronous().invalidate(k);
                    deleteCacheFile(k);
                });
        return inner.deleteLogical(path);
    }

    @Override
    public CompletableFuture<List<String>> listLogical(String prefix) {
        return inner.listLogical(prefix);
    }

    @Override
    public CompletableFuture<Boolean> delete(String path) {
        cache.synchronous().invalidate(path);
        deleteCacheFile(path);
        return inner.delete(path);
    }

    @Override
    public CompletableFuture<List<String>> list(String prefix) {
        return inner.list(prefix);
    }

    @Override
    public CompletableFuture<Integer> deleteByPrefix(String prefix) {
        return inner.deleteByPrefix(prefix).thenApply(count -> {
            List<String> keysToInvalidate = new ArrayList<>(
                    cache.synchronous().asMap().keySet());
            keysToInvalidate.stream()
                    .filter(k -> k.startsWith(prefix))
                    .forEach(k -> {
                        cache.synchronous().invalidate(k);
                        deleteCacheFile(k);
                    });
            return count;
        });
    }

    /**
     * Triggers pending Caffeine maintenance (evictions, removal listeners).
     * Package-private for use in tests.
     */
    void cleanUp() {
        cache.synchronous().cleanUp();
    }

    @Override
    public void close() throws Exception {
        inner.close();
    }
}
