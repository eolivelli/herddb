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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Thin gRPC adapter that delegates all storage operations to an {@link ObjectStorage} backend.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServiceImpl extends RemoteFileServiceGrpc.RemoteFileServiceImplBase {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileServiceImpl.class.getName());

    private final ObjectStorage storage;

    public RemoteFileServiceImpl(ObjectStorage storage) {
        this.storage = storage;
    }

    @Override
    public void writeFile(WriteFileRequest request, StreamObserver<WriteFileResponse> responseObserver) {
        long start = System.nanoTime();
        byte[] content = request.getContent().toByteArray();
        storage.write(request.getPath(), content)
                .whenComplete((v, t) -> {
                    if (t != null) {
                        LOGGER.log(Level.SEVERE, "writeFile failed for path " + request.getPath(), t);
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                    } else {
                        LOGGER.log(Level.INFO, "writeFile path={0} size={1} time={2}ms",
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
        storage.read(request.getPath())
                .whenComplete((result, t) -> {
                    if (t != null) {
                        LOGGER.log(Level.SEVERE, "readFile failed for path " + request.getPath(), t);
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                    } else if (result.status() == ReadResult.Status.NOT_FOUND) {
                        LOGGER.log(Level.INFO, "readFile path={0} found=false time={1}ms",
                                new Object[]{request.getPath(), elapsedMs(start)});
                        responseObserver.onNext(ReadFileResponse.newBuilder().setFound(false).build());
                        responseObserver.onCompleted();
                    } else {
                        byte[] content = result.content();
                        LOGGER.log(Level.INFO, "readFile path={0} size={1} time={2}ms",
                                new Object[]{request.getPath(), content.length, elapsedMs(start)});
                        responseObserver.onNext(ReadFileResponse.newBuilder()
                                .setFound(true)
                                .setContent(ByteString.copyFrom(content))
                                .build());
                        responseObserver.onCompleted();
                    }
                });
    }

    @Override
    public void deleteFile(DeleteFileRequest request, StreamObserver<DeleteFileResponse> responseObserver) {
        long start = System.nanoTime();
        storage.delete(request.getPath())
                .whenComplete((deleted, t) -> {
                    if (t != null) {
                        LOGGER.log(Level.SEVERE, "deleteFile failed for path " + request.getPath(), t);
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                    } else {
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
        storage.list(request.getPrefix())
                .whenComplete((paths, t) -> {
                    if (t != null) {
                        LOGGER.log(Level.SEVERE, "listFiles failed for prefix " + request.getPrefix(), t);
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                    } else {
                        for (String path : paths) {
                            responseObserver.onNext(ListFilesEntry.newBuilder().setPath(path).build());
                        }
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
        storage.deleteByPrefix(request.getPrefix())
                .whenComplete((count, t) -> {
                    if (t != null) {
                        LOGGER.log(Level.SEVERE, "deleteByPrefix failed for prefix " + request.getPrefix(), t);
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                    } else {
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
