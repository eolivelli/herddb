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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

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
    @Nullable
    private final OpStatsLogger diskReadLatency;
    @Nullable
    private final Counter diskReadBytes;
    @Nullable
    private final Counter diskReadRequests;

    public LocalObjectStorage(Path baseDirectory, ExecutorService metadataExecutor,
                              @Nullable StatsLogger statsLogger) throws IOException {
        this.baseDirectory = baseDirectory.toAbsolutePath();
        this.metadataExecutor = metadataExecutor;
        if (statsLogger != null) {
            StatsLogger diskScope = statsLogger.scope("rfs").scope("disk");
            this.diskReadLatency = diskScope.getOpStatsLogger("read_latency");
            this.diskReadBytes = diskScope.getCounter("read_bytes");
            this.diskReadRequests = diskScope.getCounter("read_requests");
        } else {
            this.diskReadLatency = null;
            this.diskReadBytes = null;
            this.diskReadRequests = null;
        }
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

    /**
     * Backward-compatible constructor without metrics logging.
     */
    public LocalObjectStorage(Path baseDirectory, ExecutorService metadataExecutor) throws IOException {
        this(baseDirectory, metadataExecutor, null);
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
    public CompletableFuture<Void> writeBlock(String path, long blockIndex, byte[] content) {
        String blockPath = path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex;
        Path target = resolvePath(blockPath);
        Path parentDir = target.getParent();
        CompletableFuture<Void> result = new CompletableFuture<>();
        knownDirectories.get(parentDir).thenAccept(v -> {
            doWriteFile(blockPath, target, content, result);
        }).exceptionally(t -> {
            LOGGER.log(Level.SEVERE, "writeBlock failed to create directory for path " + blockPath, t);
            result.completeExceptionally(t);
            return null;
        });
        return result;
    }

    @Override
    public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
        long blockIndex = offset / blockSize;
        int offsetInBlock = (int) (offset % blockSize);
        String blockPath = path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex;
        Path target = resolvePath(blockPath);
        if (!Files.exists(target)) {
            return CompletableFuture.completedFuture(ReadResult.notFound());
        }
        if (diskReadRequests != null) {
            diskReadRequests.inc();
        }
        CompletableFuture<ReadResult> result = new CompletableFuture<>();
        final long startNanos = System.nanoTime();
        try {
            long fileSize = Files.size(target);
            int available = (int) (fileSize - offsetInBlock);
            if (available <= 0) {
                if (diskReadLatency != null) {
                    diskReadLatency.registerFailedEvent(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
                }
                return CompletableFuture.completedFuture(ReadResult.notFound());
            }
            int toRead = Math.min(length, available);
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(target, StandardOpenOption.READ);
            ByteBuffer buffer = ByteBuffer.allocate(toRead);
            channel.read(buffer, offsetInBlock, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer bytesRead, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                    }
                    buffer.flip();
                    byte[] content = new byte[buffer.remaining()];
                    buffer.get(content);
                    if (diskReadLatency != null) {
                        diskReadLatency.registerSuccessfulEvent(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
                    }
                    if (diskReadBytes != null) {
                        diskReadBytes.add((long) content.length);
                    }
                    result.complete(ReadResult.found(content));
                }
                @Override
                public void failed(Throwable exc, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                    }
                    if (diskReadLatency != null) {
                        diskReadLatency.registerFailedEvent(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
                    }
                    result.completeExceptionally(exc);
                }
            });
        } catch (Throwable t) {
            if (diskReadLatency != null) {
                diskReadLatency.registerFailedEvent(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
            }
            result.completeExceptionally(t);
        }
        return result;
    }

    @Override
    public CompletableFuture<Boolean> deleteLogical(String path) {
        return CompletableFuture.supplyAsync(() -> {
            Path multipartDir = resolvePath(path + ObjectStorage.MULTIPART_SUFFIX);
            if (Files.isDirectory(multipartDir)) {
                try {
                    deleteRecursive(multipartDir);
                    return true;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                return Files.deleteIfExists(resolvePath(path));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, metadataExecutor);
    }

    private void deleteRecursive(Path dir) throws IOException {
        Files.walk(dir)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public CompletableFuture<List<String>> listLogical(String prefix) {
        return CompletableFuture.supplyAsync(() -> {
            java.util.LinkedHashSet<String> logical = new java.util.LinkedHashSet<>();
            try {
                if (!Files.exists(baseDirectory)) {
                    return new ArrayList<>(logical);
                }
                Files.walk(baseDirectory)
                        .filter(p -> Files.isRegularFile(p) || Files.isDirectory(p))
                        .forEach(p -> {
                            String relative = baseDirectory.relativize(p).toString().replace('\\', '/');
                            // Convert multipart block entries to logical paths
                            int mpIdx = relative.indexOf(ObjectStorage.MULTIPART_SUFFIX + "/");
                            if (mpIdx >= 0) {
                                String logicalPath = relative.substring(0, mpIdx);
                                if (logicalPath.startsWith(prefix)) {
                                    logical.add(logicalPath);
                                }
                            } else if (Files.isRegularFile(p) && !relative.endsWith(ObjectStorage.MULTIPART_SUFFIX)) {
                                if (relative.startsWith(prefix)) {
                                    logical.add(relative);
                                }
                            }
                        });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new ArrayList<>(logical);
        }, metadataExecutor);
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
