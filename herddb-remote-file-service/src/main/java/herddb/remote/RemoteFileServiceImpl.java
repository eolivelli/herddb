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

import com.google.protobuf.UnsafeByteOperations;
import herddb.remote.proto.DeleteByPrefixRequest;
import herddb.remote.proto.DeleteByPrefixResponse;
import herddb.remote.proto.DeleteFileRequest;
import herddb.remote.proto.DeleteFileResponse;
import herddb.remote.proto.ListFilesEntry;
import herddb.remote.proto.ListFilesRequest;
import herddb.remote.proto.ReadFileRangeRequest;
import herddb.remote.proto.ReadFileRangeResponse;
import herddb.remote.proto.ReadFileRequest;
import herddb.remote.proto.ReadFileResponse;
import herddb.remote.proto.RemoteFileServiceGrpc;
import herddb.remote.proto.WriteFileBlockRequest;
import herddb.remote.proto.WriteFileBlockResponse;
import herddb.remote.proto.WriteFileRequest;
import herddb.remote.proto.WriteFileResponse;
import herddb.remote.storage.ObjectStorage;
import herddb.remote.storage.ReadResult;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Thin gRPC adapter that delegates all storage operations to an {@link ObjectStorage} backend.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServiceImpl extends RemoteFileServiceGrpc.RemoteFileServiceImplBase {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileServiceImpl.class.getName());

    private final ObjectStorage storage;
    private final Executor readExecutor;
    private final Executor writeExecutor;

    private final Counter writeRequests;
    private final Counter writeErrors;
    private final Counter writtenBytes;
    private final OpStatsLogger writeLatency;

    private final Counter readRequests;
    private final Counter readErrors;
    private final Counter readNotFound;
    private final Counter readBytes;
    private final OpStatsLogger readLatency;

    private final Counter deleteRequests;
    private final Counter deleteErrors;
    private final OpStatsLogger deleteLatency;

    private final Counter listRequests;
    private final Counter listErrors;
    private final OpStatsLogger listLatency;

    private final Counter deleteByPrefixRequests;
    private final Counter deleteByPrefixErrors;
    private final OpStatsLogger deleteByPrefixLatency;

    private final Counter writeBlockRequests;
    private final Counter writeBlockErrors;
    private final Counter writtenBlockBytes;
    private final OpStatsLogger writeBlockLatency;

    private final Counter readRangeRequests;
    private final Counter readRangeErrors;
    private final Counter readRangeNotFound;
    private final Counter readRangeBytes;
    private final OpStatsLogger readRangeLatency;

    public RemoteFileServiceImpl(ObjectStorage storage) {
        this(storage, NullStatsLogger.INSTANCE, null, null);
    }

    public RemoteFileServiceImpl(ObjectStorage storage, StatsLogger statsLogger) {
        this(storage, statsLogger, null, null);
    }

    /**
     * Creates the service with dedicated read/write execution lanes (issue #100).
     * Each RPC handler dispatches its storage call and its completion callback onto
     * {@code readExecutor} or {@code writeExecutor} according to operation type, so
     * slow write-path callbacks cannot starve read-path responses.
     *
     * <p>If either executor is {@code null}, handlers fall back to
     * {@code whenComplete} on the storage future's default thread, preserving the
     * legacy behavior.
     */
    public RemoteFileServiceImpl(ObjectStorage storage, StatsLogger statsLogger,
                                 Executor readExecutor, Executor writeExecutor) {
        this.storage = storage;
        this.readExecutor = readExecutor;
        this.writeExecutor = writeExecutor;

        StatsLogger scope = statsLogger.scope("rfs");

        this.writeRequests = scope.getCounter("write_requests");
        this.writeErrors = scope.getCounter("write_errors");
        this.writtenBytes = scope.getCounter("written_bytes");
        this.writeLatency = scope.getOpStatsLogger("write_latency");

        this.readRequests = scope.getCounter("read_requests");
        this.readErrors = scope.getCounter("read_errors");
        this.readNotFound = scope.getCounter("read_not_found");
        this.readBytes = scope.getCounter("read_bytes");
        this.readLatency = scope.getOpStatsLogger("read_latency");

        this.deleteRequests = scope.getCounter("delete_requests");
        this.deleteErrors = scope.getCounter("delete_errors");
        this.deleteLatency = scope.getOpStatsLogger("delete_latency");

        this.listRequests = scope.getCounter("list_requests");
        this.listErrors = scope.getCounter("list_errors");
        this.listLatency = scope.getOpStatsLogger("list_latency");

        this.deleteByPrefixRequests = scope.getCounter("deletebyprefix_requests");
        this.deleteByPrefixErrors = scope.getCounter("deletebyprefix_errors");
        this.deleteByPrefixLatency = scope.getOpStatsLogger("deletebyprefix_latency");

        this.writeBlockRequests = scope.getCounter("writeblock_requests");
        this.writeBlockErrors = scope.getCounter("writeblock_errors");
        this.writtenBlockBytes = scope.getCounter("writeblock_bytes");
        this.writeBlockLatency = scope.getOpStatsLogger("writeblock_latency");

        this.readRangeRequests = scope.getCounter("readrange_requests");
        this.readRangeErrors = scope.getCounter("readrange_errors");
        this.readRangeNotFound = scope.getCounter("readrange_not_found");
        this.readRangeBytes = scope.getCounter("readrange_bytes");
        this.readRangeLatency = scope.getOpStatsLogger("readrange_latency");
    }

    @Override
    public void writeFile(WriteFileRequest request, StreamObserver<WriteFileResponse> responseObserver) {
        runOnLane(writeExecutor, () -> writeFileImpl(request, responseObserver));
    }

    private void writeFileImpl(WriteFileRequest request, StreamObserver<WriteFileResponse> responseObserver) {
        long start = System.nanoTime();
        writeRequests.inc();
        byte[] content = request.getContent().toByteArray();
        attachCallback(storage.write(request.getPath(), content), writeExecutor,
                (v, t) -> {
                    long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
                    if (t != null) {
                        writeErrors.inc();
                        writeLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                        LOGGER.log(Level.SEVERE, "writeFile failed for path " + request.getPath(), t);
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                    } else {
                        writtenBytes.addCount(content.length);
                        writeLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                        LOGGER.log(Level.FINE, "writeFile path={0} size={1} time={2}ms",
                                new Object[]{request.getPath(), content.length, elapsedMs(start)});
                        responseObserver.onNext(WriteFileResponse.newBuilder()
                                .setWrittenSize(content.length)
                                .build());
                        responseObserver.onCompleted();
                    }
                });
    }

    @Override
    public void readFile(ReadFileRequest request, StreamObserver<ReadFileResponse> responseObserver) {
        runOnLane(readExecutor, () -> readFileImpl(request, responseObserver));
    }

    private void readFileImpl(ReadFileRequest request, StreamObserver<ReadFileResponse> responseObserver) {
        long start = System.nanoTime();
        readRequests.inc();
        attachCallback(storage.read(request.getPath()), readExecutor,
                (result, t) -> {
                    long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
                    try {
                        if (t != null) {
                            readErrors.inc();
                            readLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                            LOGGER.log(Level.SEVERE, "readFile failed for path " + request.getPath(), t);
                            responseObserver.onError(
                                    Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                        } else if (result.status() == ReadResult.Status.NOT_FOUND) {
                            readNotFound.inc();
                            readLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                            LOGGER.log(Level.INFO, "readFile path={0} found=false time={1}ms",
                                    new Object[]{request.getPath(), elapsedMs(start)});
                            responseObserver.onNext(ReadFileResponse.newBuilder().setFound(false).build());
                            responseObserver.onCompleted();
                        } else {
                            byte[] content = result.content();
                            readBytes.addCount(content.length);
                            readLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                            LOGGER.log(Level.FINE, "readFile path={0} size={1} time={2}ms",
                                    new Object[]{request.getPath(), content.length, elapsedMs(start)});
                            responseObserver.onNext(ReadFileResponse.newBuilder()
                                    .setFound(true)
                                    .setContent(UnsafeByteOperations.unsafeWrap(content))
                                    .build());
                            responseObserver.onCompleted();
                        }
                    } finally {
                        // Release pooled ByteBuf if result was backed by one
                        if (result != null) {
                            result.release();
                        }
                    }
                });
    }

    @Override
    public void writeFileBlock(WriteFileBlockRequest request,
                               StreamObserver<WriteFileBlockResponse> responseObserver) {
        runOnLane(writeExecutor, () -> writeFileBlockImpl(request, responseObserver));
    }

    private void writeFileBlockImpl(WriteFileBlockRequest request,
                                    StreamObserver<WriteFileBlockResponse> responseObserver) {
        long start = System.nanoTime();
        writeBlockRequests.inc();
        byte[] content = request.getContent().toByteArray();
        attachCallback(storage.writeBlock(request.getPath(), request.getBlockIndex(), content), writeExecutor,
                (v, t) -> {
                    long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
                    if (t != null) {
                        writeBlockErrors.inc();
                        writeBlockLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                        LOGGER.log(Level.SEVERE, "writeFileBlock failed for path " + request.getPath()
                                + " block " + request.getBlockIndex(), t);
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                    } else {
                        writtenBlockBytes.addCount(content.length);
                        writeBlockLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                        LOGGER.log(Level.FINE, "writeFileBlock path={0} block={1} size={2} time={3}ms",
                                new Object[]{request.getPath(), request.getBlockIndex(),
                                        content.length, elapsedMs(start)});
                        responseObserver.onNext(WriteFileBlockResponse.newBuilder()
                                .setWrittenSize(content.length)
                                .build());
                        responseObserver.onCompleted();
                    }
                });
    }

    @Override
    public void readFileRange(ReadFileRangeRequest request,
                              StreamObserver<ReadFileRangeResponse> responseObserver) {
        runOnLane(readExecutor, () -> readFileRangeImpl(request, responseObserver));
    }

    private void readFileRangeImpl(ReadFileRangeRequest request,
                                   StreamObserver<ReadFileRangeResponse> responseObserver) {
        long start = System.nanoTime();
        readRangeRequests.inc();
        attachCallback(
                storage.readRange(request.getPath(), request.getOffset(), request.getLength(), request.getBlockSize()),
                readExecutor,
                (result, t) -> {
                    long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
                    if (t != null) {
                        readRangeErrors.inc();
                        readRangeLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                        LOGGER.log(Level.SEVERE, "readFileRange failed for path " + request.getPath(), t);
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                    } else if (result.status() == ReadResult.Status.NOT_FOUND) {
                        readRangeNotFound.inc();
                        readRangeLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                        responseObserver.onNext(ReadFileRangeResponse.newBuilder().setFound(false).build());
                        responseObserver.onCompleted();
                    } else {
                        byte[] content = result.content();
                        readRangeBytes.addCount(content.length);
                        readRangeLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                        LOGGER.log(Level.FINE, "readFileRange path={0} offset={1} size={2} time={3}ms",
                                new Object[]{request.getPath(), request.getOffset(),
                                        content.length, elapsedMs(start)});
                        responseObserver.onNext(ReadFileRangeResponse.newBuilder()
                                .setFound(true)
                                .setContent(UnsafeByteOperations.unsafeWrap(content))
                                .build());
                        responseObserver.onCompleted();
                    }
                });
    }

    @Override
    public void deleteFile(DeleteFileRequest request, StreamObserver<DeleteFileResponse> responseObserver) {
        runOnLane(writeExecutor, () -> deleteFileImpl(request, responseObserver));
    }

    private void deleteFileImpl(DeleteFileRequest request, StreamObserver<DeleteFileResponse> responseObserver) {
        long start = System.nanoTime();
        deleteRequests.inc();
        attachCallback(storage.deleteLogical(request.getPath()), writeExecutor,
                (deleted, t) -> {
                    long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
                    if (t != null) {
                        deleteErrors.inc();
                        deleteLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                        LOGGER.log(Level.SEVERE, "deleteFile failed for path " + request.getPath(), t);
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                    } else {
                        deleteLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                        LOGGER.log(Level.INFO, "deleteFile path={0} deleted={1} time={2}ms",
                                new Object[]{request.getPath(), deleted, elapsedMs(start)});
                        responseObserver.onNext(DeleteFileResponse.newBuilder().setDeleted(deleted).build());
                        responseObserver.onCompleted();
                    }
                });
    }

    @Override
    public void listFiles(ListFilesRequest request, StreamObserver<ListFilesEntry> responseObserver) {
        runOnLane(readExecutor, () -> listFilesImpl(request, responseObserver));
    }

    private void listFilesImpl(ListFilesRequest request, StreamObserver<ListFilesEntry> responseObserver) {
        long start = System.nanoTime();
        listRequests.inc();
        attachCallback(storage.listLogical(request.getPrefix()), readExecutor,
                (paths, t) -> {
                    long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
                    if (t != null) {
                        listErrors.inc();
                        listLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                        LOGGER.log(Level.SEVERE, "listFiles failed for prefix " + request.getPrefix(), t);
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                    } else {
                        try {
                            for (String path : paths) {
                                responseObserver.onNext(ListFilesEntry.newBuilder().setPath(path).build());
                            }
                            listLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                            LOGGER.log(Level.INFO, "listFiles prefix={0} count={1} time={2}ms",
                                    new Object[]{request.getPrefix(), paths.size(), elapsedMs(start)});
                            responseObserver.onCompleted();
                        } catch (Exception streamError) {
                            listErrors.inc();
                            listLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                            LOGGER.log(Level.WARNING, "listFiles streaming failed for prefix " + request.getPrefix()
                                    + " after " + elapsedMs(start) + "ms (sent " + paths.size() + " entries)", streamError);
                            try {
                                responseObserver.onError(
                                        Status.INTERNAL.withDescription(streamError.getMessage()).withCause(streamError)
                                                .asRuntimeException());
                            } catch (Exception ignored) {
                                // stream may already be closed; nothing more we can do
                            }
                        }
                    }
                });
    }

    @Override
    public void deleteByPrefix(DeleteByPrefixRequest request,
                               StreamObserver<DeleteByPrefixResponse> responseObserver) {
        runOnLane(writeExecutor, () -> deleteByPrefixImpl(request, responseObserver));
    }

    private void deleteByPrefixImpl(DeleteByPrefixRequest request,
                                    StreamObserver<DeleteByPrefixResponse> responseObserver) {
        long start = System.nanoTime();
        deleteByPrefixRequests.inc();
        attachCallback(storage.deleteByPrefix(request.getPrefix()), writeExecutor,
                (count, t) -> {
                    long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
                    if (t != null) {
                        deleteByPrefixErrors.inc();
                        deleteByPrefixLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                        LOGGER.log(Level.SEVERE, "deleteByPrefix failed for prefix " + request.getPrefix(), t);
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                    } else {
                        deleteByPrefixLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                        LOGGER.log(Level.INFO, "deleteByPrefix prefix={0} deletedCount={1} time={2}ms",
                                new Object[]{request.getPrefix(), count, elapsedMs(start)});
                        responseObserver.onNext(DeleteByPrefixResponse.newBuilder()
                                .setDeletedCount(count)
                                .build());
                        responseObserver.onCompleted();
                    }
                });
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    /**
     * Executes {@code task} on the given lane executor, or inline if the executor is
     * {@code null} (legacy single-lane mode). Lane executors hop handler work off the
     * Netty worker thread so checkpoint and search paths cannot block each other.
     */
    private static void runOnLane(Executor lane, Runnable task) {
        if (lane != null) {
            lane.execute(task);
        } else {
            task.run();
        }
    }

    /**
     * Attaches {@code callback} to {@code future} using {@code executor} when non-null
     * ({@code whenCompleteAsync}), otherwise falls back to {@code whenComplete}. Keeps
     * the existing legacy path byte-for-byte when lanes are disabled.
     */
    private static <T> void attachCallback(CompletableFuture<T> future,
                                           Executor executor,
                                           BiConsumer<? super T, ? super Throwable> callback) {
        if (executor != null) {
            future.whenCompleteAsync(callback, executor);
        } else {
            future.whenComplete(callback);
        }
    }
}
