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
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Local filesystem implementation of {@link ObjectStorage}.
 * Extracted from RemoteFileServiceImpl to allow composability with caching/S3.
 *
 * @author enrico.olivelli
 */
public class LocalObjectStorage implements ObjectStorage {

    private static final Logger LOGGER = Logger.getLogger(LocalObjectStorage.class.getName());

    private final Path baseDirectory;
    private final ExecutorService metadataExecutor;
    private final AsyncLoadingCache<Path, Boolean> knownDirectories;

    public LocalObjectStorage(Path baseDirectory, ExecutorService metadataExecutor) throws IOException {
        this.baseDirectory = baseDirectory.toAbsolutePath();
        this.metadataExecutor = metadataExecutor;
        Files.createDirectories(this.baseDirectory);
        this.knownDirectories = Caffeine.newBuilder()
                .maximumSize(1000)
                .buildAsync((dir, executor) ->
                        CompletableFuture.supplyAsync(() -> {
                            try {
                                Files.createDirectories(dir);
                                return true;
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }, metadataExecutor));
        knownDirectories.put(this.baseDirectory, CompletableFuture.completedFuture(true));
        if (Files.exists(this.baseDirectory)) {
            Files.walk(this.baseDirectory)
                    .filter(Files::isDirectory)
                    .forEach(dir -> knownDirectories.put(dir, CompletableFuture.completedFuture(true)));
        }
    }

    private Path resolvePath(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return baseDirectory.resolve(path).normalize();
    }

    @Override
    public CompletableFuture<Void> write(String path, byte[] content) {
        Path target = resolvePath(path);
        Path parentDir = target.getParent();
        CompletableFuture<Void> result = new CompletableFuture<>();

        knownDirectories.get(parentDir).thenAccept(v -> {
            doWriteFile(path, target, content, result);
        }).exceptionally(t -> {
            LOGGER.log(Level.SEVERE, "write failed to create directory for path " + path, t);
            result.completeExceptionally(t);
            return null;
        });

        return result;
    }

    private void doWriteFile(String path, Path target, byte[] content, CompletableFuture<Void> result) {
        Path tmp = target.getParent().resolve(target.getFileName() + ".tmp");
        try {
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(
                    tmp, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            ByteBuffer buffer = ByteBuffer.wrap(content);
            channel.write(buffer, 0, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer bytesWritten, Void attachment) {
                    try {
                        channel.close();
                        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                        result.complete(null);
                    } catch (Throwable t) {
                        cleanup(tmp);
                        result.completeExceptionally(t);
                    }
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                    }
                    cleanup(tmp);
                    result.completeExceptionally(exc);
                }
            });
        } catch (Throwable t) {
            cleanup(tmp);
            result.completeExceptionally(t);
        }
    }

    @Override
    public CompletableFuture<ReadResult> read(String path) {
        Path target = resolvePath(path);

        if (!Files.exists(target)) {
            return CompletableFuture.completedFuture(ReadResult.notFound());
        }

        CompletableFuture<ReadResult> result = new CompletableFuture<>();
        try {
            long fileSize = Files.size(target);
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(target, StandardOpenOption.READ);
            ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);

            channel.read(buffer, 0, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer bytesRead, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                    }
                    buffer.flip();
                    byte[] content = new byte[buffer.remaining()];
                    buffer.get(content);
                    result.complete(ReadResult.found(content));
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                    }
                    result.completeExceptionally(exc);
                }
            });
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }

        return result;
    }

    @Override
    public CompletableFuture<Boolean> delete(String path) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return Files.deleteIfExists(resolvePath(path));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, metadataExecutor);
    }

    @Override
    public CompletableFuture<List<String>> list(String prefix) {
        return CompletableFuture.supplyAsync(() -> {
            List<String> results = new ArrayList<>();
            try {
                collectMatchingPaths(baseDirectory, prefix, results);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return results;
        }, metadataExecutor);
    }

    @Override
    public CompletableFuture<Integer> deleteByPrefix(String prefix) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<String> found = new ArrayList<>();
                collectMatchingPaths(baseDirectory, prefix, found);
                int count = 0;
                for (String p : found) {
                    if (Files.deleteIfExists(resolvePath(p))) {
                        count++;
                    }
                }
                return count;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, metadataExecutor);
    }

    @Override
    public void close() {
        // Executor is externally managed; nothing to close here.
    }

    private void collectMatchingPaths(Path base, String prefix, List<String> results) throws IOException {
        if (!Files.exists(base)) {
            return;
        }
        Files.walk(base)
                .filter(Files::isRegularFile)
                .forEach(p -> {
                    String relative = base.relativize(p).toString().replace('\\', '/');
                    if (relative.startsWith(prefix)) {
                        results.add(relative);
                    }
                });
    }

    private static void cleanup(Path tmp) {
        try {
            Files.deleteIfExists(tmp);
        } catch (IOException ignored) {
        }
    }
}
