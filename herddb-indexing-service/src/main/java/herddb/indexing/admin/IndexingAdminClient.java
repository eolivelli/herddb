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

package herddb.indexing.admin;

import herddb.indexing.proto.DeleteSegmentRequest;
import herddb.indexing.proto.DeleteSegmentResponse;
import herddb.indexing.proto.DescribeIndexRequest;
import herddb.indexing.proto.DescribeIndexResponse;
import herddb.indexing.proto.GetEngineStatsRequest;
import herddb.indexing.proto.GetEngineStatsResponse;
import herddb.indexing.proto.GetIndexStatusRequest;
import herddb.indexing.proto.GetIndexStatusResponse;
import herddb.indexing.proto.GetInstanceInfoRequest;
import herddb.indexing.proto.GetInstanceInfoResponse;
import herddb.indexing.proto.GetShadowStatusRequest;
import herddb.indexing.proto.GetShadowStatusResponse;
import herddb.indexing.proto.IndexingServiceGrpc;
import herddb.indexing.proto.ListIndexesRequest;
import herddb.indexing.proto.ListIndexesResponse;
import herddb.indexing.proto.ListPrimaryKeysRequest;
import herddb.indexing.proto.PrimaryKeysChunk;
import herddb.indexing.proto.WaitForCheckpointRequest;
import herddb.indexing.proto.WaitForCheckpointResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Thin gRPC client used by the {@link IndexingAdminCli} to talk to a single
 * indexing service instance. Unlike
 * {@link herddb.indexing.IndexingServiceClient}, this class does not fan out
 * or participate in ZooKeeper discovery — it targets exactly one
 * {@code host:port} selected by the CLI user.
 */
public final class IndexingAdminClient implements Closeable {

    private final ManagedChannel channel;
    private final IndexingServiceGrpc.IndexingServiceBlockingStub stub;
    private final long timeoutSeconds;

    public IndexingAdminClient(String target, long timeoutSeconds) {
        this.channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .keepAliveTime(60, TimeUnit.SECONDS)
                .keepAliveTimeout(20, TimeUnit.SECONDS)
                .build();
        this.stub = IndexingServiceGrpc.newBlockingStub(channel);
        this.timeoutSeconds = timeoutSeconds;
    }

    private IndexingServiceGrpc.IndexingServiceBlockingStub stub() {
        return stub.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
    }

    public ListIndexesResponse listIndexes() {
        return stub().listIndexes(ListIndexesRequest.newBuilder().build());
    }

    public DescribeIndexResponse describeIndex(String tablespace, String table, String index) {
        return stub().describeIndex(DescribeIndexRequest.newBuilder()
                .setTablespace(tablespace == null ? "" : tablespace)
                .setTable(table)
                .setIndex(index)
                .build());
    }

    public GetIndexStatusResponse getIndexStatus(String tablespace, String table, String index) {
        return stub().getIndexStatus(GetIndexStatusRequest.newBuilder()
                .setTablespace(tablespace == null ? "" : tablespace)
                .setTable(table)
                .setIndex(index)
                .build());
    }

    public Iterator<PrimaryKeysChunk> listPrimaryKeys(String tablespace, String table, String index,
                                                      int chunkSize, boolean includeOnDisk, long limit) {
        ListPrimaryKeysRequest req = ListPrimaryKeysRequest.newBuilder()
                .setTablespace(tablespace == null ? "" : tablespace)
                .setTable(table)
                .setIndex(index)
                .setChunkSize(chunkSize)
                .setIncludeOndisk(includeOnDisk)
                .setLimit(limit)
                .build();
        return stub().listPrimaryKeys(req);
    }

    public GetEngineStatsResponse getEngineStats() {
        return stub().getEngineStats(GetEngineStatsRequest.newBuilder().build());
    }

    public GetInstanceInfoResponse getInstanceInfo() {
        return stub().getInstanceInfo(GetInstanceInfoRequest.newBuilder().build());
    }

    public GetShadowStatusResponse getShadowStatus() {
        return stub().getShadowStatus(GetShadowStatusRequest.newBuilder().build());
    }

    /**
     * Issue #617: operator remediation tool. Removes a single segment from
     * the IS in-memory metadata, optionally purging its multipart files.
     */
    public DeleteSegmentResponse deleteSegment(String tablespace, String table, String index,
                                                String segment, boolean purgeStorage, boolean force) {
        return stub().deleteSegment(DeleteSegmentRequest.newBuilder()
                .setTablespace(tablespace == null ? "" : tablespace)
                .setTable(table == null ? "" : table)
                .setIndex(index == null ? "" : index)
                .setSegment(segment == null ? "" : segment)
                .setPurgeStorage(purgeStorage)
                .setForce(force)
                .build());
    }

    public WaitForCheckpointResponse waitForCheckpoint(String tablespace, String table, String index,
                                                       long targetLedgerId, long targetOffset,
                                                       long waitTimeoutMs) {
        // The gRPC deadline is padded with the caller's wait budget so the
        // call does not time out before the server-side poll loop returns.
        long deadlineSecs = timeoutSeconds + Math.max(1, waitTimeoutMs / 1000L);
        return stub.withDeadlineAfter(deadlineSecs, TimeUnit.SECONDS)
                .waitForCheckpoint(WaitForCheckpointRequest.newBuilder()
                        .setTablespace(tablespace == null ? "" : tablespace)
                        .setTable(table == null ? "" : table)
                        .setIndex(index == null ? "" : index)
                        .setTargetLedgerId(targetLedgerId)
                        .setTargetOffset(targetOffset)
                        .setTimeoutMs(waitTimeoutMs)
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
