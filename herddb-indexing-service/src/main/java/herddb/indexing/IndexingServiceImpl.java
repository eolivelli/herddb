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

import com.google.protobuf.ByteString;
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
import herddb.indexing.proto.IndexDescriptor;
import herddb.indexing.proto.IndexingServiceGrpc;
import herddb.indexing.proto.ListIndexesRequest;
import herddb.indexing.proto.ListIndexesResponse;
import herddb.indexing.proto.ListPrimaryKeysRequest;
import herddb.indexing.proto.PrimaryKeysChunk;
import herddb.indexing.proto.SearchRequest;
import herddb.indexing.proto.SearchResponse;
import herddb.indexing.proto.SearchResult;
import herddb.indexing.proto.WaitForCheckpointRequest;
import herddb.indexing.proto.WaitForCheckpointResponse;
import herddb.log.LogSequenceNumber;
import herddb.utils.Bytes;
import herddb.utils.VectorSearchRequestContext;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * gRPC implementation that delegates to the {@link IndexingServiceEngine}.
 *
 * @author enrico.olivelli
 */
public class IndexingServiceImpl extends IndexingServiceGrpc.IndexingServiceImplBase {

    private static final Logger LOGGER = Logger.getLogger(IndexingServiceImpl.class.getName());

    private static final int DEFAULT_PK_CHUNK_SIZE = 1000;
    private static final int MAX_PK_CHUNK_SIZE = 10000;

    private final IndexingServiceEngine engine;

    private final Counter searchRequests;
    private final Counter searchErrors;
    private final OpStatsLogger searchLatency;
    private final Counter searchBytes;
    private final Counter statusRequests;
    private final Counter statusErrors;
    private final Counter shadowNotReadyResponses;

    // Per-request readFileRange metrics — populated from VectorSearchRequestContext
    // at the end of every search(). Allow attributing network I/O to an
    // individual VectorSearch RPC (see issue #196).
    private final Counter searchReadFileRangeCalls;
    private final Counter searchReadFileRangeBytes;
    private final Counter searchReadFileRangeWaitNanos;
    private final OpStatsLogger searchReadFileRangeCallsPerRequest;
    private final OpStatsLogger searchReadFileRangeBytesPerRequest;
    private final OpStatsLogger searchReadFileRangeWaitPerRequest;
    private final OpStatsLogger searchCacheHitsPerRequest;
    private final OpStatsLogger searchCacheMissesPerRequest;

    public IndexingServiceImpl(IndexingServiceEngine engine, StatsLogger statsLogger) {
        this.engine = engine;
        StatsLogger scope = statsLogger.scope("indexing");
        this.searchRequests = scope.getCounter("search_requests");
        this.searchErrors = scope.getCounter("search_errors");
        this.searchLatency = scope.getOpStatsLogger("search_latency");
        this.searchBytes = scope.getCounter("search_bytes");
        this.statusRequests = scope.getCounter("status_requests");
        this.statusErrors = scope.getCounter("status_errors");
        this.shadowNotReadyResponses = scope.getCounter("shadow_not_ready_responses");
        this.searchReadFileRangeCalls = scope.getCounter("search_readfilerange_calls");
        this.searchReadFileRangeBytes = scope.getCounter("search_readfilerange_bytes");
        this.searchReadFileRangeWaitNanos = scope.getCounter("search_readfilerange_wait_nanos");
        this.searchReadFileRangeCallsPerRequest =
                scope.getOpStatsLogger("search_readfilerange_calls_per_request");
        this.searchReadFileRangeBytesPerRequest =
                scope.getOpStatsLogger("search_readfilerange_bytes_per_request");
        this.searchReadFileRangeWaitPerRequest =
                scope.getOpStatsLogger("search_readfilerange_wait_per_request");
        this.searchCacheHitsPerRequest = scope.getOpStatsLogger("search_cache_hits_per_request");
        this.searchCacheMissesPerRequest = scope.getOpStatsLogger("search_cache_misses_per_request");
    }

    /**
     * Short-circuits a query RPC with FAILED_PRECONDITION when this instance
     * is a shadow replica that has not yet completed its first successful
     * reload. The description is machine-parseable
     * ({@code shadow_not_ready:<shadowOf>}) so the HerdDB-side
     * {@link IndexingServiceClient} can recognise it and fall back to another
     * endpoint in the same pool.
     *
     * @return {@code true} iff the caller should abort the RPC (a NOT_READY
     *         response has been sent via {@code responseObserver})
     */
    private boolean abortIfShadowNotReady(StreamObserver<?> responseObserver) {
        if (engine.isConfiguredAsShadow() && !engine.isShadowReady()) {
            shadowNotReadyResponses.inc();
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("shadow_not_ready:" + engine.getShadowOfOrMinusOne())
                    .asRuntimeException());
            return true;
        }
        return false;
    }

    @Override
    public void search(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {
        if (abortIfShadowNotReady(responseObserver)) {
            return;
        }
        searchRequests.inc();
        long start = System.nanoTime();
        VectorSearchRequestContext ctx = VectorSearchRequestContext.begin();
        try {
            float[] vector = new float[request.getVectorCount()];
            for (int i = 0; i < vector.length; i++) {
                vector[i] = request.getVector(i);
            }

            List<Map.Entry<Bytes, Float>> results = engine.search(
                    request.getTablespace(),
                    request.getTable(),
                    request.getIndex(),
                    vector,
                    request.getLimit());

            SearchResponse.Builder responseBuilder = SearchResponse.newBuilder();
            for (Map.Entry<Bytes, Float> entry : results) {
                SearchResult.Builder resultBuilder = SearchResult.newBuilder()
                        .setPrimaryKey(ByteString.copyFrom(entry.getKey().getBuffer(),
                                entry.getKey().getOffset(), entry.getKey().getLength()));
                if (request.getReturnScore()) {
                    resultBuilder.setScore(entry.getValue());
                }
                responseBuilder.addResults(resultBuilder);
            }

            SearchResponse response = responseBuilder.build();
            long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
            searchLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
            searchBytes.inc();
            recordRequestContext(ctx, true);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            searchErrors.inc();
            long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
            searchLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
            recordRequestContext(ctx, false);
            LOGGER.log(Level.SEVERE, "Search failed for index " + request.getIndex(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        } finally {
            VectorSearchRequestContext.end();
        }
    }

    /**
     * Drains the per-request accumulator into the aggregate counters and the
     * per-request histograms. Keeps the drain logic in one place so that the
     * success and failure paths in {@link #search} stay parallel.
     */
    private void recordRequestContext(VectorSearchRequestContext ctx, boolean success) {
        if (ctx == null) {
            return;
        }
        long calls = ctx.getReadFileRangeCalls();
        long bytes = ctx.getReadFileRangeBytes();
        long waitNanos = ctx.getReadFileRangeWaitNanos();
        long hits = ctx.getCacheHits();
        long misses = ctx.getCacheMisses();
        searchReadFileRangeCalls.addCount(calls);
        searchReadFileRangeBytes.addCount(bytes);
        searchReadFileRangeWaitNanos.addCount(waitNanos);
        if (success) {
            searchReadFileRangeCallsPerRequest.registerSuccessfulEvent(calls, TimeUnit.NANOSECONDS);
            searchReadFileRangeBytesPerRequest.registerSuccessfulEvent(bytes, TimeUnit.NANOSECONDS);
            searchReadFileRangeWaitPerRequest.registerSuccessfulEvent(waitNanos, TimeUnit.NANOSECONDS);
            searchCacheHitsPerRequest.registerSuccessfulEvent(hits, TimeUnit.NANOSECONDS);
            searchCacheMissesPerRequest.registerSuccessfulEvent(misses, TimeUnit.NANOSECONDS);
        } else {
            searchReadFileRangeCallsPerRequest.registerFailedEvent(calls, TimeUnit.NANOSECONDS);
            searchReadFileRangeBytesPerRequest.registerFailedEvent(bytes, TimeUnit.NANOSECONDS);
            searchReadFileRangeWaitPerRequest.registerFailedEvent(waitNanos, TimeUnit.NANOSECONDS);
            searchCacheHitsPerRequest.registerFailedEvent(hits, TimeUnit.NANOSECONDS);
            searchCacheMissesPerRequest.registerFailedEvent(misses, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public void getIndexStatus(GetIndexStatusRequest request, StreamObserver<GetIndexStatusResponse> responseObserver) {
        if (abortIfShadowNotReady(responseObserver)) {
            return;
        }
        statusRequests.inc();
        try {
            IndexingServiceEngine.IndexStatusInfo info = engine.getIndexStatus(
                    request.getTablespace(),
                    request.getTable(),
                    request.getIndex());

            GetIndexStatusResponse response = GetIndexStatusResponse.newBuilder()
                    .setVectorCount(info.getVectorCount())
                    .setSegmentCount(info.getSegmentCount())
                    .setTailerLsnLedger(info.getTailerLsnLedger())
                    .setTailerLsnOffset(info.getTailerLsnOffset())
                    .setDurableLsnLedger(info.getDurableLsnLedger())
                    .setDurableLsnOffset(info.getDurableLsnOffset())
                    .setStatus(info.getStatus())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            statusErrors.inc();
            LOGGER.log(Level.SEVERE, "GetIndexStatus failed for index " + request.getIndex(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void listIndexes(ListIndexesRequest request, StreamObserver<ListIndexesResponse> responseObserver) {
        if (abortIfShadowNotReady(responseObserver)) {
            return;
        }
        try {
            List<IndexingServiceEngine.IndexDescriptor> indexes = engine.listIndexes();
            ListIndexesResponse.Builder builder = ListIndexesResponse.newBuilder();
            for (IndexingServiceEngine.IndexDescriptor d : indexes) {
                builder.addIndexes(IndexDescriptor.newBuilder()
                        .setTablespace(nullToEmpty(d.getTablespace()))
                        .setTable(nullToEmpty(d.getTable()))
                        .setIndex(nullToEmpty(d.getIndex()))
                        .setVectorCount(d.getVectorCount())
                        .setStatus(nullToEmpty(d.getStatus()))
                        .setSegmentCount(d.getSegmentCount())
                        .build());
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "ListIndexes failed", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void describeIndex(DescribeIndexRequest request, StreamObserver<DescribeIndexResponse> responseObserver) {
        if (abortIfShadowNotReady(responseObserver)) {
            return;
        }
        try {
            IndexingServiceEngine.IndexDetails details = engine.describeIndex(
                    request.getTablespace(),
                    request.getTable(),
                    request.getIndex());
            if (details == null) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("index not loaded on this instance: "
                                + request.getTable() + "." + request.getIndex())
                        .asRuntimeException());
                return;
            }
            DescribeIndexResponse.Builder builder = DescribeIndexResponse.newBuilder()
                    .setBasic(IndexDescriptor.newBuilder()
                            .setTablespace(nullToEmpty(details.tablespace))
                            .setTable(nullToEmpty(details.table))
                            .setIndex(nullToEmpty(details.index))
                            .setVectorCount(details.vectorCount)
                            .setStatus(nullToEmpty(details.status))
                            .build())
                    .setDimension(details.dimension)
                    .setSimilarity(nullToEmpty(details.similarity))
                    .setLiveNodeCount(details.liveNodeCount)
                    .setOndiskNodeCount(details.onDiskNodeCount)
                    .setSegmentCount(details.segmentCount)
                    .setLiveShardCount(details.liveShardCount)
                    .setEstimatedMemoryBytes(details.estimatedMemoryBytes)
                    .setLiveVectorsMemoryBytes(details.liveVectorsMemoryBytes)
                    .setOndiskSizeBytes(details.onDiskSizeBytes)
                    .setDirty(details.dirty)
                    .setTailerLsnLedger(details.tailerLsnLedger)
                    .setTailerLsnOffset(details.tailerLsnOffset)
                    .setDurableLsnLedger(details.durableLsnLedger)
                    .setDurableLsnOffset(details.durableLsnOffset)
                    .setFusedPqEnabled(details.fusedPQEnabled)
                    .setM(details.m)
                    .setBeamWidth(details.beamWidth)
                    .setPersistent(details.persistent)
                    .setStoreClass(nullToEmpty(details.storeClass))
                    .setCompactionPhase(nullToEmpty(details.compactionPhase))
                    .setCompactionProgress(details.compactionProgress)
                    .setCompactionNodesDone(details.compactionNodesDone)
                    .setCompactionNodesTotal(details.compactionNodesTotal)
                    .setUploadBytesDone(details.uploadBytesDone)
                    .setUploadBytesTotal(details.uploadBytesTotal)
                    .setNextNodeId(details.nextNodeId);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "DescribeIndex failed for index " + request.getIndex(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void listPrimaryKeys(ListPrimaryKeysRequest request,
                                 StreamObserver<PrimaryKeysChunk> responseObserver) {
        if (abortIfShadowNotReady(responseObserver)) {
            return;
        }
        try {
            int chunkSize = request.getChunkSize() > 0 ? request.getChunkSize() : DEFAULT_PK_CHUNK_SIZE;
            if (chunkSize > MAX_PK_CHUNK_SIZE) {
                chunkSize = MAX_PK_CHUNK_SIZE;
            }
            final int effectiveChunkSize = chunkSize;
            final List<ByteString>[] buffer = new List[]{new ArrayList<>(effectiveChunkSize)};
            engine.streamPrimaryKeys(
                    request.getTablespace(),
                    request.getTable(),
                    request.getIndex(),
                    request.getIncludeOndisk(),
                    request.getLimit(),
                    pk -> {
                        buffer[0].add(ByteString.copyFrom(pk.getBuffer(), pk.getOffset(), pk.getLength()));
                        if (buffer[0].size() >= effectiveChunkSize) {
                            responseObserver.onNext(PrimaryKeysChunk.newBuilder()
                                    .addAllPrimaryKeys(buffer[0])
                                    .setLast(false)
                                    .build());
                            buffer[0] = new ArrayList<>(effectiveChunkSize);
                        }
                        return true;
                    });
            // Always send a terminal chunk so clients can detect end-of-stream
            // even when the total count is an exact multiple of chunkSize.
            responseObserver.onNext(PrimaryKeysChunk.newBuilder()
                    .addAllPrimaryKeys(buffer[0])
                    .setLast(true)
                    .build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "ListPrimaryKeys failed for index " + request.getIndex(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void getEngineStats(GetEngineStatsRequest request,
                                StreamObserver<GetEngineStatsResponse> responseObserver) {
        try {
            long start = engine.getStartTimeMillis();
            long uptime = start > 0 ? System.currentTimeMillis() - start : 0L;
            LogSequenceNumber lsn = engine.getLastProcessedLsn();
            java.lang.management.MemoryUsage heap =
                    java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            long heapUsed = heap.getUsed();
            long heapMax = heap.getMax();
            int heapPct = heapMax > 0 ? (int) Math.min(100L, (100L * heapUsed) / heapMax) : -1;
            GetEngineStatsResponse response = GetEngineStatsResponse.newBuilder()
                    .setUptimeMillis(uptime)
                    .setTailerWatermarkLedger(lsn != null ? lsn.ledgerId : -1L)
                    .setTailerWatermarkOffset(lsn != null ? lsn.offset : -1L)
                    .setTailerEntriesProcessed(engine.getTailerEntriesProcessed())
                    .setTailerRunning(engine.isTailerRunning())
                    .setApplyQueueSize(engine.getApplyQueueSize())
                    .setApplyQueueCapacity(engine.getApplyQueueCapacity())
                    .setTotalEstimatedMemoryBytes(engine.getTotalEstimatedMemoryBytes())
                    .setLoadedIndexCount(engine.getLoadedIndexCount())
                    .setApplyParallelism(engine.getApplyParallelism())
                    .setJvmHeapUsedBytes(heapUsed)
                    .setJvmHeapMaxBytes(heapMax)
                    .setJvmHeapUsedPct(heapPct)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "GetEngineStats failed", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void getInstanceInfo(GetInstanceInfoRequest request,
                                 StreamObserver<GetInstanceInfoResponse> responseObserver) {
        try {
            IndexingServerConfiguration cfg = engine.getConfig();
            String storageType = cfg != null
                    ? cfg.getString(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE,
                            IndexingServerConfiguration.PROPERTY_STORAGE_TYPE_DEFAULT)
                    : "";
            String dataDir = engine.getDataDirectory() != null
                    ? engine.getDataDirectory().toAbsolutePath().toString()
                    : "";
            String tablespaceName = cfg != null
                    ? cfg.getString(IndexingServerConfiguration.PROPERTY_TABLESPACE_NAME,
                            IndexingServerConfiguration.PROPERTY_TABLESPACE_NAME_DEFAULT)
                    : "";
            int grpcPort = cfg != null
                    ? cfg.getInt(IndexingServerConfiguration.PROPERTY_GRPC_PORT,
                            IndexingServerConfiguration.PROPERTY_GRPC_PORT_DEFAULT)
                    : 0;
            String grpcHost = cfg != null
                    ? cfg.getString(IndexingServerConfiguration.PROPERTY_GRPC_HOST,
                            IndexingServerConfiguration.PROPERTY_GRPC_HOST_DEFAULT)
                    : "";
            int instanceOrdinal = cfg != null
                    ? cfg.getInt(IndexingServerConfiguration.PROPERTY_INSTANCE_ID,
                            IndexingServerConfiguration.PROPERTY_INSTANCE_ID_DEFAULT)
                    : 0;
            int numInstances = cfg != null
                    ? cfg.getInt(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES,
                            IndexingServerConfiguration.PROPERTY_NUM_INSTANCES_DEFAULT)
                    : 1;
            String role = cfg != null
                    ? cfg.getString(IndexingServerConfiguration.PROPERTY_ROLE,
                            IndexingServerConfiguration.PROPERTY_ROLE_DEFAULT)
                    : IndexingServerConfiguration.ROLE_PRIMARY;
            int shadowOf = engine.getShadowOfOrMinusOne();
            GetInstanceInfoResponse response = GetInstanceInfoResponse.newBuilder()
                    .setInstanceId(nullToEmpty(engine.getInstanceIdLabel()))
                    .setGrpcHost(nullToEmpty(grpcHost))
                    .setGrpcPort(grpcPort)
                    .setStorageType(nullToEmpty(storageType))
                    .setDataDir(nullToEmpty(dataDir))
                    .setJvmMaxHeapBytes(Runtime.getRuntime().maxMemory())
                    .setTablespaceName(nullToEmpty(tablespaceName))
                    .setTablespaceUuid(nullToEmpty(engine.getTableSpaceUUID()))
                    .setInstanceOrdinal(instanceOrdinal)
                    .setNumInstances(numInstances)
                    .setRole(role)
                    .setShadowOf(engine.isConfiguredAsShadow() ? shadowOf : -1)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "GetInstanceInfo failed", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    @Override
    public void getShadowStatus(GetShadowStatusRequest request,
                                 StreamObserver<GetShadowStatusResponse> responseObserver) {
        // Diagnostic — never gated by the NOT_READY shadow check.
        try {
            LogSequenceNumber loaded = engine.getShadowLoadedLsn();
            LogSequenceNumber advertised = engine.getPrimaryAdvertisedLsn();
            GetShadowStatusResponse.Builder b = GetShadowStatusResponse.newBuilder()
                    .setIsShadow(engine.isConfiguredAsShadow())
                    .setShadowOf(engine.isConfiguredAsShadow() ? engine.getShadowOfOrMinusOne() : -1)
                    .setReady(engine.isConfiguredAsShadow() ? engine.isShadowReady() : true)
                    .setLoadedLedgerId(loaded != null ? loaded.ledgerId : -1L)
                    .setLoadedOffset(loaded != null ? loaded.offset : -1L)
                    .setPrimaryAdvertisedLedgerId(advertised != null ? advertised.ledgerId : -1L)
                    .setPrimaryAdvertisedOffset(advertised != null ? advertised.offset : -1L)
                    .setLastReloadTimestampMs(engine.getShadowLastReloadTimestampMs())
                    .setReloadCount(engine.getShadowReloadCount())
                    .setAppliedIndexStatusGeneration(engine.getMinAppliedIndexStatusGeneration());
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "GetShadowStatus failed", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void waitForCheckpoint(WaitForCheckpointRequest request,
                                   StreamObserver<WaitForCheckpointResponse> responseObserver) {
        // Signal-based polling every 200ms; honours both the gRPC deadline and
        // the explicit timeout_ms. Always diagnostic — not gated by NOT_READY.
        long requestTimeoutMs = request.getTimeoutMs();
        long deadlineMs = requestTimeoutMs > 0
                ? System.currentTimeMillis() + requestTimeoutMs
                : Long.MAX_VALUE;
        LogSequenceNumber target = new LogSequenceNumber(
                request.getTargetLedgerId(), request.getTargetOffset());
        try {
            while (true) {
                LogSequenceNumber current = currentLoadedLsnForRequest(request);
                if (current != null && (current.equals(target) || current.after(target))) {
                    responseObserver.onNext(buildWaitResponse(current, true));
                    responseObserver.onCompleted();
                    return;
                }
                long now = System.currentTimeMillis();
                if (now >= deadlineMs) {
                    responseObserver.onNext(buildWaitResponse(
                            current == null ? LogSequenceNumber.START_OF_TIME : current, false));
                    responseObserver.onCompleted();
                    return;
                }
                long sleepMs = Math.min(200L, deadlineMs - now);
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    responseObserver.onError(Status.CANCELLED
                            .withDescription("interrupted").asRuntimeException());
                    return;
                }
            }
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "WaitForCheckpoint failed", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    /**
     * Returns the LSN to compare against the target for WaitForCheckpoint:
     * for shadows it's the shadow's loaded LSN (the point on disk the shadow
     * is currently serving); for primaries it's the engine's lastProcessedLsn.
     */
    private LogSequenceNumber currentLoadedLsnForRequest(WaitForCheckpointRequest request) {
        if (engine.isConfiguredAsShadow()) {
            return engine.getShadowLoadedLsn();
        }
        return engine.getLastProcessedLsn();
    }

    private static WaitForCheckpointResponse buildWaitResponse(LogSequenceNumber cur, boolean reached) {
        return WaitForCheckpointResponse.newBuilder()
                .setCurrentLedgerId(cur.ledgerId)
                .setCurrentOffset(cur.offset)
                .setReached(reached)
                .build();
    }
}
