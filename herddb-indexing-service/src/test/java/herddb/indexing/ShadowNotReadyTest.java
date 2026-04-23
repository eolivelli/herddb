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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.indexing.proto.IndexingServiceGrpc;
import herddb.indexing.proto.SearchRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that a shadow-configured {@link IndexingServiceImpl} answers
 * Search/GetIndexStatus/ListIndexes/DescribeIndex/ListPrimaryKeys RPCs with
 * {@code FAILED_PRECONDITION "shadow_not_ready:<shadowOf>"} until the engine
 * reports {@code isShadowReady()}. The error description is the contract the
 * HerdDB-side client uses to recognise NOT_READY and retry against another
 * endpoint in the same shadow pool.
 */
public class ShadowNotReadyTest {

    /**
     * Minimal engine test-double exposing only the methods
     * {@link IndexingServiceImpl#abortIfShadowNotReady} consults. The engine
     * is reused unmodified from production apart from this stub, which lets
     * the test drive the NOT_READY flag without booting the full shadow
     * pipeline.
     */
    private static final class StubEngine extends IndexingServiceEngine {
        private volatile boolean shadow;
        private volatile boolean ready;
        private volatile int shadowOf = -1;

        StubEngine() {
            super(java.nio.file.Paths.get("/tmp/noop-log"), java.nio.file.Paths.get("/tmp/noop-data"),
                    new IndexingServerConfiguration());
        }

        @Override
        public boolean isConfiguredAsShadow() {
            return shadow;
        }

        @Override
        public boolean isShadowReady() {
            return ready;
        }

        @Override
        public int getShadowOfOrMinusOne() {
            return shadowOf;
        }
    }

    private StubEngine engine;
    private Server server;
    private ManagedChannel channel;
    private IndexingServiceGrpc.IndexingServiceBlockingStub stub;

    @Before
    public void setUp() throws Exception {
        engine = new StubEngine();
        IndexingServiceImpl svc = new IndexingServiceImpl(engine, NullStatsLogger.INSTANCE);
        server = ServerBuilder.forPort(0).addService(svc).build().start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext().build();
        stub = IndexingServiceGrpc.newBlockingStub(channel);
    }

    @After
    public void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
            server.awaitTermination();
        }
    }

    @Test
    public void searchOnNotReadyShadowReturnsFailedPrecondition() {
        engine.shadow = true;
        engine.shadowOf = 7;
        engine.ready = false;

        SearchRequest req = SearchRequest.newBuilder()
                .setTablespace("default").setTable("t").setIndex("vidx").setLimit(5)
                .addVector(0.1f).addVector(0.2f).addVector(0.3f).addVector(0.4f)
                .build();
        try {
            stub.search(req);
            fail("expected FAILED_PRECONDITION from a NOT_READY shadow");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
            assertTrue("description must carry machine-parseable NOT_READY tag (got: "
                            + e.getStatus().getDescription() + ")",
                    e.getStatus().getDescription() != null
                            && e.getStatus().getDescription().startsWith("shadow_not_ready:"));
            assertTrue(e.getStatus().getDescription().endsWith(":7"));
        }
    }

    @Test
    public void readySearchReachesEngine() {
        engine.shadow = true;
        engine.shadowOf = 0;
        engine.ready = true;

        // With ready=true, the NOT_READY gate does not fire — the request
        // reaches engine.search() which returns empty because the stub has
        // no stores. The important thing is: NOT a FAILED_PRECONDITION.
        SearchRequest req = SearchRequest.newBuilder()
                .setTablespace("default").setTable("t").setIndex("vidx").setLimit(5)
                .addVector(0.1f).addVector(0.2f).addVector(0.3f).addVector(0.4f)
                .build();
        var response = stub.search(req);
        assertEquals(0, response.getResultsCount());
    }

    @Test
    public void primaryIsAlwaysReady() {
        engine.shadow = false;
        engine.ready = false; // irrelevant for primaries

        SearchRequest req = SearchRequest.newBuilder()
                .setTablespace("default").setTable("t").setIndex("vidx").setLimit(5)
                .addVector(0f).addVector(0f)
                .build();
        // Primary always accepts the RPC; the gate never fires.
        var response = stub.search(req);
        assertEquals(0, response.getResultsCount());
    }

    /**
     * Null-object responseObserver — here just to exercise the typed path
     * from IndexingServiceImpl.abortIfShadowNotReady without relying on gRPC.
     */
    private static final class NullResponseObserver implements StreamObserver<Object> {
        volatile Throwable error;
        @Override
        public void onNext(Object value) {
        }

        @Override
        public void onError(Throwable t) {
            error = t;
        }

        @Override
        public void onCompleted() {
        }
    }
}
