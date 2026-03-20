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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.ByteString;
import herddb.remote.proto.DeleteByPrefixRequest;
import herddb.remote.proto.DeleteByPrefixResponse;
import herddb.remote.proto.DeleteFileRequest;
import herddb.remote.proto.DeleteFileResponse;
import herddb.remote.proto.ListFilesEntry;
import herddb.remote.proto.ListFilesRequest;
import herddb.remote.proto.ReadFileRequest;
import herddb.remote.proto.ReadFileResponse;
import herddb.remote.proto.RemoteFileServiceGrpc;
import herddb.remote.proto.WriteFileRequest;
import herddb.remote.proto.WriteFileResponse;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Fully asynchronous gRPC service implementation backed by local filesystem.
 * <p>
 * Uses {@link AsynchronousFileChannel} for data read/write operations and a
 * dedicated {@link ExecutorService} for metadata operations (mkdir, move,
 * delete, walk) which have no async JDK API.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServiceImpl extends RemoteFileServiceGrpc.RemoteFileServiceImplBase {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileServiceImpl.class.getName());

    private final Path baseDirectory;
    private final ExecutorService metadataExecutor;
    private final AsyncLoadingCache<Path, Boolean> knownDirectories;

    public RemoteFileServiceImpl(Path baseDirectory, ExecutorService metadataExecutor) throws IOException {
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
        // Pre-populate cache with existing directories
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
    public void writeFile(WriteFileRequest request, StreamObserver<WriteFileResponse> responseObserver) {
        long start = System.nanoTime();
        Path target = resolvePath(request.getPath());
        Path parentDir = target.getParent();
        byte[] content = request.getContent().toByteArray();

        knownDirectories.get(parentDir).thenAccept(v -> {
            doWriteFile(request.getPath(), target, content, start, responseObserver);
        }).exceptionally(t -> {
            LOGGER.log(Level.SEVERE, "writeFile failed to create directory for path " + request.getPath(), t);
            responseObserver.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
            return null;
        });
    }

    private void doWriteFile(String path, Path target, byte[] content, long start,
                             StreamObserver<WriteFileResponse> responseObserver) {
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
                        LOGGER.log(Level.INFO, "writeFile path={0} size={1} time={2}ms",
                                new Object[]{path, content.length, elapsedMs(start)});
                        responseObserver.onNext(WriteFileResponse.newBuilder()
                                .setWrittenSize(content.length)
                                .build());
                        responseObserver.onCompleted();
                    } catch (Throwable t) {
                        cleanup(tmp);
                        LOGGER.log(Level.SEVERE, "writeFile post-write failed for path " + path, t);
                        responseObserver.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                    }
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                    }
                    cleanup(tmp);
                    LOGGER.log(Level.SEVERE, "writeFile async write failed for path " + path, exc);
                    responseObserver.onError(Status.INTERNAL.withDescription(exc.getMessage()).asRuntimeException());
                }
            });
        } catch (Throwable t) {
            cleanup(tmp);
            LOGGER.log(Level.SEVERE, "writeFile failed for path " + path, t);
            responseObserver.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void readFile(ReadFileRequest request, StreamObserver<ReadFileResponse> responseObserver) {
        long start = System.nanoTime();
        Path target = resolvePath(request.getPath());

        if (!Files.exists(target)) {
            LOGGER.log(Level.INFO, "readFile path={0} found=false time={1}ms",
                    new Object[]{request.getPath(), elapsedMs(start)});
            responseObserver.onNext(ReadFileResponse.newBuilder().setFound(false).build());
            responseObserver.onCompleted();
            return;
        }

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
                    LOGGER.log(Level.INFO, "readFile path={0} size={1} time={2}ms",
                            new Object[]{request.getPath(), content.length, elapsedMs(start)});
                    responseObserver.onNext(ReadFileResponse.newBuilder()
                            .setFound(true)
                            .setContent(ByteString.copyFrom(content))
                            .build());
                    responseObserver.onCompleted();
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                    }
                    LOGGER.log(Level.SEVERE, "readFile async read failed for path " + request.getPath(), exc);
                    responseObserver.onError(Status.INTERNAL.withDescription(exc.getMessage()).asRuntimeException());
                }
            });
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "readFile failed for path " + request.getPath(), t);
            responseObserver.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void deleteFile(DeleteFileRequest request, StreamObserver<DeleteFileResponse> responseObserver) {
        executeOnMetadataPool(() -> {
            long start = System.nanoTime();
            try {
                Path target = resolvePath(request.getPath());
                long size = Files.exists(target) ? Files.size(target) : 0;
                boolean deleted = Files.deleteIfExists(target);
                LOGGER.log(Level.INFO, "deleteFile path={0} size={1} deleted={2} time={3}ms",
                        new Object[]{request.getPath(), size, deleted, elapsedMs(start)});
                responseObserver.onNext(DeleteFileResponse.newBuilder().setDeleted(deleted).build());
                responseObserver.onCompleted();
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "deleteFile failed for path " + request.getPath(), t);
                responseObserver.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
            }
        }, responseObserver);
    }

    @Override
    public void listFiles(ListFilesRequest request, StreamObserver<ListFilesEntry> responseObserver) {
        ServerCallStreamObserver<ListFilesEntry> serverObserver =
                (ServerCallStreamObserver<ListFilesEntry>) responseObserver;

        executeOnMetadataPool(() -> {
            long start = System.nanoTime();
            try {
                String prefix = request.getPrefix();
                int count = 0;
                if (Files.exists(baseDirectory)) {
                    List<String> found = new ArrayList<>();
                    collectMatchingPaths(baseDirectory, prefix, found);
                    for (String path : found) {
                        while (!serverObserver.isReady()) {
                            LockSupport.parkNanos(1_000_000); // 1ms
                        }
                        serverObserver.onNext(ListFilesEntry.newBuilder().setPath(path).build());
                        count++;
                    }
                }
                LOGGER.log(Level.INFO, "listFiles prefix={0} count={1} time={2}ms",
                        new Object[]{prefix, count, elapsedMs(start)});
                serverObserver.onCompleted();
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "listFiles failed for prefix " + request.getPrefix(), t);
                serverObserver.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
            }
        }, serverObserver);
    }

    @Override
    public void deleteByPrefix(DeleteByPrefixRequest request, StreamObserver<DeleteByPrefixResponse> responseObserver) {
        executeOnMetadataPool(() -> {
            long start = System.nanoTime();
            try {
                String prefix = request.getPrefix();
                List<String> found = new ArrayList<>();
                collectMatchingPaths(baseDirectory, prefix, found);
                int count = 0;
                for (String p : found) {
                    Path target = resolvePath(p);
                    if (Files.deleteIfExists(target)) {
                        count++;
                    }
                }
                LOGGER.log(Level.INFO, "deleteByPrefix prefix={0} deletedCount={1} time={2}ms",
                        new Object[]{prefix, count, elapsedMs(start)});
                responseObserver.onNext(DeleteByPrefixResponse.newBuilder().setDeletedCount(count).build());
                responseObserver.onCompleted();
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "deleteByPrefix failed for prefix " + request.getPrefix(), t);
                responseObserver.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
            }
        }, responseObserver);
    }

    /**
     * Submits a task to the metadata executor, sending RESOURCE_EXHAUSTED if the pool rejects it.
     *
     * @return true if the task was submitted, false if rejected
     */
    private boolean executeOnMetadataPool(Runnable task, StreamObserver<?> responseObserver) {
        try {
            metadataExecutor.execute(task);
            return true;
        } catch (RejectedExecutionException e) {
            responseObserver.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("I/O pool full")
                    .asRuntimeException());
            return false;
        }
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
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
