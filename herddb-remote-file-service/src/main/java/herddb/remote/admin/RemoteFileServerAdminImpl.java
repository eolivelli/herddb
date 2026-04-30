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

import herddb.remote.RemoteFileServer;
import herddb.remote.proto.GetServerInfoRequest;
import herddb.remote.proto.GetServerInfoResponse;
import herddb.remote.proto.RemoteFileServerAdminGrpc;
import herddb.remote.proto.ResizeDiskCacheRequest;
import herddb.remote.proto.ResizeDiskCacheResponse;
import herddb.remote.storage.CachingObjectStorage;
import herddb.remote.storage.InMemoryBlockCacheObjectStorage;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gRPC admin service for the file server (issue #336).
 * <p>
 * Registered on the same gRPC port as {@link herddb.remote.RemoteFileServiceImpl};
 * gRPC multiplexes both services by their service descriptor name so no extra port
 * is needed.
 * <p>
 * Provides two RPCs:
 * <ul>
 *   <li>{@code GetServerInfo} — returns a snapshot of JVM, disk-cache, and block-cache
 *       stats. Safe to call at any time; all reads are lock-free.</li>
 *   <li>{@code ResizeDiskCache} — calls {@link CachingObjectStorage#setMaxCacheBytes(long)}
 *       to resize the disk-LRU on the fly. The change is not persistent.</li>
 * </ul>
 *
 * @author enrico.olivelli
 */
public class RemoteFileServerAdminImpl extends RemoteFileServerAdminGrpc.RemoteFileServerAdminImplBase {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileServerAdminImpl.class.getName());

    private final RemoteFileServer server;

    public RemoteFileServerAdminImpl(RemoteFileServer server) {
        this.server = server;
    }

    @Override
    public void getServerInfo(GetServerInfoRequest request,
                              StreamObserver<GetServerInfoResponse> responseObserver) {
        try {
            GetServerInfoResponse.Builder builder = GetServerInfoResponse.newBuilder()
                    .setGrpcHost(server.getHost())
                    .setGrpcPort(server.getPort())
                    .setStorageMode(server.getStorageMode());

            // JVM heap stats
            Runtime rt = Runtime.getRuntime();
            long heapUsed = rt.totalMemory() - rt.freeMemory();
            long heapMax = rt.maxMemory();
            builder.setJvmHeapUsedBytes(heapUsed)
                   .setJvmHeapMaxBytes(heapMax);

            // Disk cache (populated when storage.mode=s3)
            CachingObjectStorage dc = server.getDiskCache();
            if (dc != null) {
                com.github.benmanes.caffeine.cache.stats.CacheStats stats = dc.diskLruStats();
                builder.setDiskCacheMaxBytes(dc.getMaxCacheBytes())
                       .setDiskCacheHitCount(stats.hitCount())
                       .setDiskCacheMissCount(stats.missCount())
                       .setDiskCacheEvictionCount(stats.evictionCount())
                       .setDiskCacheHitBytes(dc.getHitBytes())
                       .setDiskCacheMissBytes(dc.getMissBytes())
                       .setDiskCacheEstimatedEntries(dc.estimatedEntries());
            }

            // In-heap block cache (populated when block.cache.enabled=true)
            InMemoryBlockCacheObjectStorage bc = server.getBlockCache();
            if (bc != null) {
                com.github.benmanes.caffeine.cache.stats.CacheStats bStats = bc.stats();
                builder.setBlockCacheMaxBytes(bc.getMaxBytes())
                       .setBlockCacheEstimatedBytes(bc.estimatedBytes())
                       .setBlockCacheEstimatedEntries(bc.estimatedSize())
                       .setBlockCacheHits(bStats.hitCount())
                       .setBlockCacheMisses(bStats.missCount())
                       .setBlockCacheEvictions(bStats.evictionCount());
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "GetServerInfo failed", e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
        }
    }

    @Override
    public void resizeDiskCache(ResizeDiskCacheRequest request,
                                StreamObserver<ResizeDiskCacheResponse> responseObserver) {
        CachingObjectStorage dc = server.getDiskCache();
        if (dc == null) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("disk cache not available: storage.mode is not s3")
                    .asRuntimeException());
            return;
        }
        long newMax = request.getNewMaxBytes();
        if (newMax <= 0) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("new_max_bytes must be > 0, got " + newMax)
                    .asRuntimeException());
            return;
        }
        long prev = dc.setMaxCacheBytes(newMax);
        LOGGER.log(Level.INFO,
                "ResizeDiskCache: previousMax={0} bytes, newMax={1} bytes",
                new Object[]{prev, newMax});
        responseObserver.onNext(ResizeDiskCacheResponse.newBuilder()
                .setPreviousMaxBytes(prev)
                .setNewMaxBytes(newMax)
                .build());
        responseObserver.onCompleted();
    }
}
