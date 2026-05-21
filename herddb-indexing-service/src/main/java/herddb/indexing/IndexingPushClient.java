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

package herddb.indexing;

import com.google.protobuf.UnsafeByteOperations;
import herddb.indexing.proto.GetIndexStatusRequest;
import herddb.indexing.proto.GetIndexStatusResponse;
import herddb.indexing.proto.IndexingServiceGrpc;
import herddb.indexing.proto.PushEntriesRequest;
import herddb.indexing.proto.PushEntriesResponse;
import herddb.indexing.proto.PushedLogEntry;
import herddb.indexing.proto.SearchRequest;
import herddb.indexing.proto.SearchResponse;
import herddb.log.LogSequenceNumber;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Thin gRPC client for the testing-only push-based indexing API. Built for
 * test tooling (the {@code VectorBench} {@code --protocol grpc} mode) that
 * feeds serialized {@link herddb.log.LogEntry} objects straight into an
 * indexing service running with {@code indexing.log.type=push}.
 *
 * <p>The channel deliberately uses <b>no deadline</b>: {@code PushEntries}
 * blocks server-side while the bounded push buffer is full (during a
 * checkpoint/compaction), so a finite deadline would spuriously abort
 * ingestion. The inbound message-size cap is raised so batched pushes of many
 * INSERT entries are not rejected by gRPC's 4&nbsp;MiB default.
 *
 * @author enrico.olivelli
 */
public final class IndexingPushClient implements AutoCloseable {

    /** Matches {@code IndexingServerConfiguration.PROPERTY_GRPC_MAX_MESSAGE_SIZE_DEFAULT}. */
    public static final int DEFAULT_MAX_MESSAGE_SIZE = 64 * 1024 * 1024;

    private final ManagedChannel channel;
    private final IndexingServiceGrpc.IndexingServiceBlockingStub stub;

    /** Connects to {@code endpoint} ({@code host:port}) with the default message-size cap. */
    public IndexingPushClient(String endpoint) {
        this(endpoint, DEFAULT_MAX_MESSAGE_SIZE);
    }

    public IndexingPushClient(String endpoint, int maxMessageSize) {
        this.channel = ManagedChannelBuilder.forTarget(endpoint)
                .usePlaintext()
                .maxInboundMessageSize(maxMessageSize)
                .build();
        // No withDeadlineAfter(): PushEntries may park for the full duration
        // of a server-side checkpoint/compaction.
        this.stub = IndexingServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Pushes a batch of {@code (LSN, serialized-LogEntry)} pairs. The
     * {@code serializedEntries} {@link ByteBuf}s are wrapped <em>zero-copy</em>
     * into the protobuf request — the caller retains ownership and MUST
     * {@code release()} them after this method returns.
     *
     * <p>Blocks until the indexing service has accepted every entry into its
     * push buffer.
     *
     * @param lsns              client-assigned log sequence numbers, in
     *                          strictly increasing order
     * @param serializedEntries matching {@code LogEntry.serializeAsByteBuf()}
     *                          buffers, one per LSN
     * @return the server's acknowledgement (accepted count + tailer watermark)
     */
    public PushEntriesResponse pushEntries(List<LogSequenceNumber> lsns, List<ByteBuf> serializedEntries) {
        if (lsns.size() != serializedEntries.size()) {
            throw new IllegalArgumentException("lsns (" + lsns.size() + ") and entries ("
                    + serializedEntries.size() + ") size mismatch");
        }
        PushEntriesRequest.Builder request = PushEntriesRequest.newBuilder();
        for (int i = 0; i < lsns.size(); i++) {
            LogSequenceNumber lsn = lsns.get(i);
            ByteBuf buf = serializedEntries.get(i);
            request.addEntries(PushedLogEntry.newBuilder()
                    .setLsnLedger(lsn.ledgerId)
                    .setLsnOffset(lsn.offset)
                    .setEntry(UnsafeByteOperations.unsafeWrap(buf.nioBuffer()))
                    .build());
        }
        return stub.pushEntries(request.build());
    }

    /** Queries an index's status — used to verify the indexed vector count. */
    public GetIndexStatusResponse getIndexStatus(String tablespace, String table, String index) {
        return stub.getIndexStatus(GetIndexStatusRequest.newBuilder()
                .setTablespace(tablespace)
                .setTable(table)
                .setIndex(index)
                .build());
    }

    /**
     * Runs an ANN search via the {@code Search} RPC. Used by the
     * {@code VectorBench} {@code --protocol grpc} query/recall phase to issue
     * vector searches against the same indexing service the bench has just
     * populated. The response carries each match's raw serialized
     * {@code primary_key} bytes (no score, since {@code returnScore=false}); the
     * caller deserializes them with {@code RecordSerializer} according to the
     * configured table schema.
     *
     * @param tablespace HerdDB tablespace name
     * @param table      table containing the vector column
     * @param index      name of the vector index to query
     * @param vector     query vector (dimension must match the index)
     * @param limit      top-K — maximum number of results to return
     */
    public SearchResponse search(String tablespace, String table, String index, float[] vector, int limit) {
        SearchRequest.Builder request = SearchRequest.newBuilder()
                .setTablespace(tablespace)
                .setTable(table)
                .setIndex(index)
                .setLimit(limit)
                .setReturnScore(false);
        for (float v : vector) {
            request.addVector(v);
        }
        return stub.search(request.build());
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
