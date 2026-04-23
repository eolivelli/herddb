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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import com.google.protobuf.ByteString;
import herddb.indexing.proto.IndexingServiceGrpc;
import herddb.indexing.proto.SearchRequest;
import herddb.indexing.proto.SearchResponse;
import herddb.indexing.proto.SearchResult;
import herddb.metadata.IndexingServiceInstanceDescriptor;
import herddb.utils.Bytes;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Test;

/**
 * Verifies the per-pool load-balancing + fallback behaviour added by step 7:
 * the client groups endpoints by effective instanceId (primary + its shadows),
 * picks one per pool per query, and transparently retries another endpoint
 * in the same pool on NOT_READY or UNAVAILABLE.
 */
public class IndexingServiceClientShadowPoolTest {

    private final List<FakeServer> servers = new ArrayList<>();
    private IndexingServiceClient client;

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        for (FakeServer s : servers) {
            s.close();
        }
        servers.clear();
    }

    private FakeServer start(io.grpc.BindableService impl) throws IOException {
        FakeServer s = new FakeServer(impl);
        s.start();
        servers.add(s);
        return s;
    }

    @Test
    public void poolFallsBackFromNotReadyShadowToPrimary() throws Exception {
        CountingImpl notReadyShadow = new CountingImpl(() -> {
            throw Status.FAILED_PRECONDITION
                    .withDescription("shadow_not_ready:0")
                    .asRuntimeException();
        });
        CountingImpl primary = new CountingImpl(() ->
                SearchResponse.newBuilder()
                        .addResults(entry(new byte[]{1}, 0.9f))
                        .build());

        FakeServer shadowServer = start(notReadyShadow);
        FakeServer primaryServer = start(primary);

        client = IndexingServiceClient.fromAddresses(Arrays.asList(), 5);
        client.updateInstances(Arrays.asList(
                IndexingServiceInstanceDescriptor.shadow("s0", shadowServer.address(), 0),
                IndexingServiceInstanceDescriptor.primary("p0", primaryServer.address(), 0)
        ));

        List<Map.Entry<Bytes, Float>> results =
                client.search("ts", "t", "i", new float[]{0f, 1f}, 1);
        assertEquals(1, results.size());
        assertEquals(Float.valueOf(0.9f), results.get(0).getValue());
        // Shadow hit at least once (round-robin happened to land on it) OR
        // primary was chosen; if shadow was picked we must have fallen back.
        int totalCalls = primary.count.get() + notReadyShadow.count.get();
        assertTrue("at least one RPC must have reached a server", totalCalls >= 1);
        // When the shadow is picked first, the fallback sends a second RPC
        // to the primary — so primary must have received a call whenever
        // the shadow received one.
        if (notReadyShadow.count.get() > 0) {
            assertTrue("primary must receive the fallback RPC", primary.count.get() >= 1);
        }
    }

    @Test
    public void singlePrimaryPoolFastPathUnchanged() throws Exception {
        CountingImpl p = new CountingImpl(() ->
                SearchResponse.newBuilder()
                        .addResults(entry(new byte[]{7}, 0.42f))
                        .build());
        FakeServer srv = start(p);

        client = IndexingServiceClient.fromAddresses(Arrays.asList(), 5);
        client.updateInstances(Arrays.asList(
                IndexingServiceInstanceDescriptor.primary("p0", srv.address(), 0)
        ));

        List<Map.Entry<Bytes, Float>> results =
                client.search("ts", "t", "i", new float[]{0f, 0f}, 1);
        assertEquals(1, results.size());
        assertEquals(1, p.count.get());
    }

    @Test
    public void poolFailsWhenAllEndpointsExhausted() throws Exception {
        CountingImpl s1 = new CountingImpl(() -> {
            throw Status.FAILED_PRECONDITION
                    .withDescription("shadow_not_ready:0").asRuntimeException();
        });
        CountingImpl s2 = new CountingImpl(() -> {
            throw Status.UNAVAILABLE.withDescription("oops").asRuntimeException();
        });
        FakeServer f1 = start(s1);
        FakeServer f2 = start(s2);

        client = IndexingServiceClient.fromAddresses(Arrays.asList(), 5);
        client.updateInstances(Arrays.asList(
                IndexingServiceInstanceDescriptor.shadow("s0a", f1.address(), 0),
                IndexingServiceInstanceDescriptor.shadow("s0b", f2.address(), 0)
        ));

        try {
            client.search("ts", "t", "i", new float[]{0f, 0f}, 1);
            fail("expected pool to fail after all endpoints exhausted");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage(),
                    e.getMessage().contains("pool 0") && e.getMessage().contains("exhausted"));
        }
        assertEquals(1, s1.count.get());
        assertEquals(1, s2.count.get());
    }

    @Test
    public void multiPoolFanOutWithShadowPerPrimary() throws Exception {
        // Two primaries (0 and 1), each with one shadow.
        CountingImpl p0 = new CountingImpl(() ->
                SearchResponse.newBuilder().addResults(entry(new byte[]{0}, 0.5f)).build());
        CountingImpl s0 = new CountingImpl(() ->
                SearchResponse.newBuilder().addResults(entry(new byte[]{1}, 0.6f)).build());
        CountingImpl p1 = new CountingImpl(() ->
                SearchResponse.newBuilder().addResults(entry(new byte[]{2}, 0.7f)).build());
        CountingImpl s1 = new CountingImpl(() ->
                SearchResponse.newBuilder().addResults(entry(new byte[]{3}, 0.8f)).build());
        FakeServer fp0 = start(p0);
        FakeServer fs0 = start(s0);
        FakeServer fp1 = start(p1);
        FakeServer fs1 = start(s1);

        client = IndexingServiceClient.fromAddresses(Arrays.asList(), 5);
        client.updateInstances(Arrays.asList(
                IndexingServiceInstanceDescriptor.primary("p0", fp0.address(), 0),
                IndexingServiceInstanceDescriptor.shadow("s0", fs0.address(), 0),
                IndexingServiceInstanceDescriptor.primary("p1", fp1.address(), 1),
                IndexingServiceInstanceDescriptor.shadow("s1", fs1.address(), 1)
        ));

        // 20 iterations should hit every endpoint at least once on average;
        // we only assert that every pool returns exactly one result per call
        // (so topK = 2 globally).
        for (int i = 0; i < 20; i++) {
            List<Map.Entry<Bytes, Float>> results =
                    client.search("ts", "t", "i", new float[]{0f, 0f}, 2);
            assertEquals(2, results.size());
        }
        // Both pools got exactly 20 RPCs combined (one per call), spread
        // across their endpoints.
        assertEquals(20, p0.count.get() + s0.count.get());
        assertEquals(20, p1.count.get() + s1.count.get());
    }

    // --------- helpers ---------

    private static SearchResult entry(byte[] pk, float score) {
        return SearchResult.newBuilder()
                .setPrimaryKey(ByteString.copyFrom(pk))
                .setScore(score)
                .build();
    }

    private static final class FakeServer implements AutoCloseable {
        private final io.grpc.BindableService impl;
        private Server server;

        FakeServer(io.grpc.BindableService impl) {
            this.impl = impl;
        }

        void start() throws IOException {
            server = ServerBuilder.forPort(0).addService(impl).build().start();
            assertNotNull(server);
        }

        String address() {
            return "localhost:" + server.getPort();
        }

        @Override
        public void close() throws InterruptedException {
            if (server != null) {
                server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    /** Impl that invokes the supplied response producer on each search. */
    private static final class CountingImpl extends IndexingServiceGrpc.IndexingServiceImplBase {
        private final java.util.function.Supplier<SearchResponse> producer;
        final AtomicInteger count = new AtomicInteger();

        CountingImpl(java.util.function.Supplier<SearchResponse> producer) {
            this.producer = producer;
        }

        @Override
        public void search(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {
            count.incrementAndGet();
            try {
                responseObserver.onNext(producer.get());
                responseObserver.onCompleted();
            } catch (RuntimeException e) {
                responseObserver.onError(e);
            }
        }
    }
}
