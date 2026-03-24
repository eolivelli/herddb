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

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
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
import herddb.remote.storage.ObjectStorage;
import herddb.remote.storage.ReadResult;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
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

    public RemoteFileServiceImpl(ObjectStorage storage) {
        this(storage, NullStatsLogger.INSTANCE);
    }

    public RemoteFileServiceImpl(ObjectStorage storage, StatsLogger statsLogger) {
        this.storage = storage;

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
    }

    @Override
    public void writeFile(WriteFileRequest request, StreamObserver<WriteFileResponse> responseObserver) {
        long start = System.nanoTime();
        writeRequests.inc();
        byte[] content = request.getContent().toByteArray();
        storage.write(request.getPath(), content)
                .whenComplete((v, t) -> {
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
        long start = System.nanoTime();
        readRequests.inc();
        storage.read(request.getPath())
                .whenComplete((result, t) -> {
                    long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
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
                });
    }

    @Override
    public void deleteFile(DeleteFileRequest request, StreamObserver<DeleteFileResponse> responseObserver) {
        long start = System.nanoTime();
        deleteRequests.inc();
        storage.delete(request.getPath())
                .whenComplete((deleted, t) -> {
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
        long start = System.nanoTime();
        listRequests.inc();
        storage.list(request.getPrefix())
                .whenComplete((paths, t) -> {
                    long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
                    if (t != null) {
                        listErrors.inc();
                        listLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                        LOGGER.log(Level.SEVERE, "listFiles failed for prefix " + request.getPrefix(), t);
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                    } else {
                        for (String path : paths) {
                            responseObserver.onNext(ListFilesEntry.newBuilder().setPath(path).build());
                        }
                        listLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                        LOGGER.log(Level.INFO, "listFiles prefix={0} count={1} time={2}ms",
                                new Object[]{request.getPrefix(), paths.size(), elapsedMs(start)});
                        responseObserver.onCompleted();
                    }
                });
    }

    @Override
    public void deleteByPrefix(DeleteByPrefixRequest request,
                               StreamObserver<DeleteByPrefixResponse> responseObserver) {
        long start = System.nanoTime();
        deleteByPrefixRequests.inc();
        storage.deleteByPrefix(request.getPrefix())
                .whenComplete((count, t) -> {
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
}
