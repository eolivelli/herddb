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

package herddb.remote.admin;

import herddb.remote.proto.GetServerInfoRequest;
import herddb.remote.proto.GetServerInfoResponse;
import herddb.remote.proto.RemoteFileServerAdminGrpc;
import herddb.remote.proto.ResizeDiskCacheRequest;
import herddb.remote.proto.ResizeDiskCacheResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * Thin blocking gRPC client for the {@link RemoteFileServerAdminImpl} admin service.
 * <p>
 * Targets exactly one file-server {@code host:port} selected by the CLI user.
 * Unlike the data-plane {@link herddb.remote.RemoteFileServiceClient}, this class
 * does not participate in ZooKeeper discovery or fan out to multiple replicas.
 * <p>
 * Usage:
 * <pre>{@code
 * try (FileServerAdminClient client = new FileServerAdminClient("localhost:9845", 30)) {
 *     GetServerInfoResponse info = client.getServerInfo();
 *     ...
 * }
 * }</pre>
 *
 * @author enrico.olivelli
 */
public final class FileServerAdminClient implements Closeable {

    private final ManagedChannel channel;
    private final RemoteFileServerAdminGrpc.RemoteFileServerAdminBlockingStub stub;
    private final long timeoutSeconds;

    public FileServerAdminClient(String target, long timeoutSeconds) {
        this.channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .keepAliveTime(60, TimeUnit.SECONDS)
                .keepAliveTimeout(20, TimeUnit.SECONDS)
                .build();
        this.stub = RemoteFileServerAdminGrpc.newBlockingStub(channel);
        this.timeoutSeconds = timeoutSeconds;
    }

    private RemoteFileServerAdminGrpc.RemoteFileServerAdminBlockingStub stub() {
        return stub.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * Returns a snapshot of server identity, JVM stats, and both cache layers.
     */
    public GetServerInfoResponse getServerInfo() {
        return stub().getServerInfo(GetServerInfoRequest.newBuilder().build());
    }

    /**
     * Resizes the on-disk cache LRU to {@code newMaxBytes}.
     * Returns the previous and new limits. The change is not persistent.
     */
    public ResizeDiskCacheResponse resizeDiskCache(long newMaxBytes) {
        return stub().resizeDiskCache(ResizeDiskCacheRequest.newBuilder()
                .setNewMaxBytes(newMaxBytes)
                .build());
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
