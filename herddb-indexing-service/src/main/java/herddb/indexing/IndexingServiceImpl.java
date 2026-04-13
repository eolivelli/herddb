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
import herddb.indexing.proto.IndexDescriptor;
import herddb.indexing.proto.IndexingServiceGrpc;
import herddb.indexing.proto.ListIndexesRequest;
import herddb.indexing.proto.ListIndexesResponse;
import herddb.indexing.proto.ListPrimaryKeysRequest;
import herddb.indexing.proto.PrimaryKeysChunk;
import herddb.indexing.proto.SearchRequest;
import herddb.indexing.proto.SearchResponse;
import herddb.indexing.proto.SearchResult;
import herddb.log.LogSequenceNumber;
import herddb.utils.Bytes;
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
    private final Counter statusRequests;
    private final Counter statusErrors;

    public IndexingServiceImpl(IndexingServiceEngine engine, StatsLogger statsLogger) {
        this.engine = engine;
        StatsLogger scope = statsLogger.scope("indexing");
        this.searchRequests = scope.getCounter("search_requests");
        this.searchErrors = scope.getCounter("search_errors");
        this.searchLatency = scope.getOpStatsLogger("search_latency");
        this.statusRequests = scope.getCounter("status_requests");
        this.statusErrors = scope.getCounter("status_errors");
    }

    @Override
    public void search(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {
        searchRequests.inc();
        long start = System.nanoTime();
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

            long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
            searchLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            searchErrors.inc();
            long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
            searchLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
            LOGGER.log(Level.SEVERE, "Search failed for index " + request.getIndex(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void getIndexStatus(GetIndexStatusRequest request, StreamObserver<GetIndexStatusResponse> responseObserver) {
        statusRequests.inc();
        try {
            IndexingServiceEngine.IndexStatusInfo info = engine.getIndexStatus(
                    request.getTablespace(),
                    request.getTable(),
                    request.getIndex());

            GetIndexStatusResponse response = GetIndexStatusResponse.newBuilder()
                    .setVectorCount(info.getVectorCount())
                    .setSegmentCount(info.getSegmentCount())
                    .setLastLsnLedger(info.getLastLsnLedger())
                    .setLastLsnOffset(info.getLastLsnOffset())
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
                    .setLastLsnLedger(details.lastLsnLedger)
                    .setLastLsnOffset(details.lastLsnOffset)
                    .setFusedPqEnabled(details.fusedPQEnabled)
                    .setM(details.m)
                    .setBeamWidth(details.beamWidth)
                    .setPersistent(details.persistent)
                    .setStoreClass(nullToEmpty(details.storeClass));
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
}
