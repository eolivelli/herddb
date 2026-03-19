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
import herddb.remote.proto.DeleteFileRequest;
import herddb.remote.proto.ListFilesRequest;
import herddb.remote.proto.ListFilesResponse;
import herddb.remote.proto.ReadFileRequest;
import herddb.remote.proto.ReadFileResponse;
import herddb.remote.proto.RemoteFileServiceGrpc;
import herddb.remote.proto.WriteFileRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for RemoteFileService. Manages one ManagedChannel per server and uses
 * ConsistentHashRouter for path distribution.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServiceClient implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileServiceClient.class.getName());

    private final ConsistentHashRouter router;
    private final Map<String, ManagedChannel> channels;
    private final Map<String, RemoteFileServiceGrpc.RemoteFileServiceBlockingStub> stubs;

    public RemoteFileServiceClient(List<String> servers) {
        this.router = new ConsistentHashRouter(servers);
        this.channels = new HashMap<>();
        this.stubs = new HashMap<>();

        for (String server : servers) {
            String[] parts = server.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
            channels.put(server, channel);
            stubs.put(server, RemoteFileServiceGrpc.newBlockingStub(channel));
        }
    }

    private RemoteFileServiceGrpc.RemoteFileServiceBlockingStub stubForPath(String path) {
        String server = router.getServer(path);
        return stubs.get(server);
    }

    public void writeFile(String path, byte[] content) {
        stubForPath(path).writeFile(WriteFileRequest.newBuilder()
                .setPath(path)
                .setContent(ByteString.copyFrom(content))
                .build());
    }

    public byte[] readFile(String path) {
        ReadFileResponse response = stubForPath(path).readFile(ReadFileRequest.newBuilder()
                .setPath(path)
                .build());
        if (!response.getFound()) {
            return null;
        }
        return response.getContent().toByteArray();
    }

    public boolean deleteFile(String path) {
        return stubForPath(path).deleteFile(DeleteFileRequest.newBuilder()
                .setPath(path)
                .build()).getDeleted();
    }

    public List<String> listFiles(String prefix) {
        List<String> result = new ArrayList<>();
        // Query all servers because with consistent hashing files with the same prefix
        // may be on different servers
        for (RemoteFileServiceGrpc.RemoteFileServiceBlockingStub stub : stubs.values()) {
            ListFilesResponse response = stub.listFiles(ListFilesRequest.newBuilder()
                    .setPrefix(prefix)
                    .build());
            result.addAll(response.getPathsList());
        }
        return result;
    }

    public int deleteByPrefix(String prefix) {
        int total = 0;
        // Delete from all servers
        for (RemoteFileServiceGrpc.RemoteFileServiceBlockingStub stub : stubs.values()) {
            total += stub.deleteByPrefix(herddb.remote.proto.DeleteByPrefixRequest.newBuilder()
                    .setPrefix(prefix)
                    .build()).getDeletedCount();
        }
        return total;
    }

    /**
     * Returns the server address that would store the given path.
     */
    public String getServerForPath(String path) {
        return router.getServer(path);
    }

    @Override
    public void close() {
        for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
            try {
                entry.getValue().shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARNING, "Interrupted while shutting down channel to " + entry.getKey(), e);
            }
        }
    }
}
