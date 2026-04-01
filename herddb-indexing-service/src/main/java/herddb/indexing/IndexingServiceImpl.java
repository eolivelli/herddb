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
import herddb.indexing.proto.GetIndexStatusRequest;
import herddb.indexing.proto.GetIndexStatusResponse;
import herddb.indexing.proto.IndexingServiceGrpc;
import herddb.indexing.proto.SearchRequest;
import herddb.indexing.proto.SearchResponse;
import herddb.indexing.proto.SearchResult;
import herddb.utils.Bytes;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
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
        LOGGER.log(Level.INFO, "gRPC search request: tablespace={0}, table={1}, index={2}, limit={3}, vectorDim={4}",
                new Object[]{request.getTablespace(), request.getTable(), request.getIndex(),
                        request.getLimit(), request.getVectorCount()});
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
            LOGGER.log(Level.INFO, "gRPC search completed: index={0}, {1} results in {2} us",
                    new Object[]{request.getIndex(), results.size(), elapsedMicros});
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
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
        } catch (Exception e) {
            statusErrors.inc();
            LOGGER.log(Level.SEVERE, "GetIndexStatus failed for index " + request.getIndex(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }
}
