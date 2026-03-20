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
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
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
 * Decorator that wraps an inner {@link ObjectStorage} (e.g. S3) with a local disk cache
 * backed by a Caffeine {@link AsyncLoadingCache}.
 * <p>
 * The cache directory is cleared on construction. Cache files are stored flat in
 * {@code cacheDir} with {@code /} replaced by {@code _} in filenames.
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
                .evictionListener((RemovalListener<String, byte[]>) (key, value, cause) -> {
                    // Synchronous: called on the maintenance thread before removal completes.
                    if (key != null) {
                        deleteCacheFile(key);
                    }
                })
                .buildAsync((path, exec) ->
                        inner.read(path).thenApply(result -> {
                            if (result.status() == ReadResult.Status.NOT_FOUND) {
                                return null;
                            }
                            byte[] bytes = result.content();
                            writeCacheFile(path, bytes);
                            return bytes;
                        }));
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
