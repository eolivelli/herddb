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
import herddb.remote.proto.ListFilesRequest;
import herddb.remote.proto.ListFilesResponse;
import herddb.remote.proto.ReadFileRequest;
import herddb.remote.proto.ReadFileResponse;
import herddb.remote.proto.RemoteFileServiceGrpc;
import herddb.remote.proto.WriteFileRequest;
import herddb.remote.proto.WriteFileResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gRPC service implementation backed by local filesystem.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServiceImpl extends RemoteFileServiceGrpc.RemoteFileServiceImplBase {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileServiceImpl.class.getName());

    private final Path baseDirectory;

    public RemoteFileServiceImpl(Path baseDirectory) throws IOException {
        this.baseDirectory = baseDirectory.toAbsolutePath();
        Files.createDirectories(this.baseDirectory);
    }

    private Path resolvePath(String path) {
        // Strip leading slash if present
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return baseDirectory.resolve(path).normalize();
    }

    @Override
    public void writeFile(WriteFileRequest request, StreamObserver<WriteFileResponse> responseObserver) {
        try {
            Path target = resolvePath(request.getPath());
            Files.createDirectories(target.getParent());
            Path tmp = target.getParent().resolve(target.getFileName() + ".tmp");
            Files.write(tmp, request.getContent().toByteArray());
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            responseObserver.onNext(WriteFileResponse.newBuilder().setOk(true).build());
            responseObserver.onCompleted();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "writeFile failed for path " + request.getPath(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void readFile(ReadFileRequest request, StreamObserver<ReadFileResponse> responseObserver) {
        try {
            Path target = resolvePath(request.getPath());
            if (!Files.exists(target)) {
                responseObserver.onNext(ReadFileResponse.newBuilder().setFound(false).build());
            } else {
                byte[] content = Files.readAllBytes(target);
                responseObserver.onNext(ReadFileResponse.newBuilder()
                        .setFound(true)
                        .setContent(ByteString.copyFrom(content))
                        .build());
            }
            responseObserver.onCompleted();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "readFile failed for path " + request.getPath(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void deleteFile(DeleteFileRequest request, StreamObserver<DeleteFileResponse> responseObserver) {
        try {
            Path target = resolvePath(request.getPath());
            boolean deleted = Files.deleteIfExists(target);
            responseObserver.onNext(DeleteFileResponse.newBuilder().setDeleted(deleted).build());
            responseObserver.onCompleted();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "deleteFile failed for path " + request.getPath(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void listFiles(ListFilesRequest request, StreamObserver<ListFilesResponse> responseObserver) {
        try {
            String prefix = request.getPrefix();
            List<String> found = new ArrayList<>();
            collectMatchingPaths(baseDirectory, prefix, found);
            responseObserver.onNext(ListFilesResponse.newBuilder().addAllPaths(found).build());
            responseObserver.onCompleted();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "listFiles failed for prefix " + request.getPrefix(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void deleteByPrefix(DeleteByPrefixRequest request, StreamObserver<DeleteByPrefixResponse> responseObserver) {
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
            responseObserver.onNext(DeleteByPrefixResponse.newBuilder().setDeletedCount(count).build());
            responseObserver.onCompleted();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "deleteByPrefix failed for prefix " + request.getPrefix(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    private void collectMatchingPaths(Path base, String prefix, List<String> results) throws IOException {
        if (!Files.exists(base)) {
            return;
        }
        // Walk all files and collect those whose relative path starts with prefix
        Files.walk(base)
                .filter(Files::isRegularFile)
                .forEach(p -> {
                    String relative = base.relativize(p).toString().replace('\\', '/');
                    if (relative.startsWith(prefix)) {
                        results.add(relative);
                    }
                });
    }
}
