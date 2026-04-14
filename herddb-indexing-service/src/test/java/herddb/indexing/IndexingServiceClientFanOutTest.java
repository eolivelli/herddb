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
import herddb.utils.Bytes;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;

/**
 * Exercises the parallel fan-out / fail-fast / bounded top-K merge behaviour
 * of {@link IndexingServiceClient#search}. Uses lightweight fake gRPC servers
 * with controllable responses (normal, error, slow) instead of a real
 * IndexingService instance.
 */
public class IndexingServiceClientFanOutTest {

    private final List<FakeIndexingServer> servers = new ArrayList<>();
    private IndexingServiceClient client;

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        for (FakeIndexingServer s : servers) {
            s.close();
        }
        servers.clear();
    }

    private FakeIndexingServer start(IndexingServiceGrpc.IndexingServiceImplBase impl) throws IOException {
        FakeIndexingServer s = new FakeIndexingServer(impl);
        s.start();
        servers.add(s);
        return s;
    }

    /**
     * Two instances: one returns a normal response, the other throws UNAVAILABLE.
     * The client must fail fast, not silently return partial results.
     */
    @Test
    public void searchFailsFastWhenOneInstanceThrows() throws Exception {
        FakeIndexingServer ok = start(new StaticResponseImpl(Arrays.asList(
                entry(new byte[]{1}, 0.9f),
                entry(new byte[]{2}, 0.8f))));
        FakeIndexingServer broken = start(new ThrowingImpl(Status.UNAVAILABLE.withDescription("boom")));

        client = new IndexingServiceClient(Arrays.asList(ok.address(), broken.address()), 10);

        try {
            client.search("herd", "t1", "vidx", new float[]{1, 0, 0}, 5);
            fail("Expected the search to fail fast when an instance errors");
        } catch (RuntimeException e) {
            // Root cause must be a gRPC status exception carrying UNAVAILABLE.
            Throwable root = e;
            while (root.getCause() != null) {
                root = root.getCause();
            }
            assertTrue("root cause mentions UNAVAILABLE or boom: " + root,
                    root.getMessage() != null
                            && (root.getMessage().contains("UNAVAILABLE") || root.getMessage().contains("boom")));
        }
    }

    /**
     * Two instances, each returning 4 entries with overlapping score ranges.
     * The merged top-3 must pick the globally highest scores across both
     * instances, sorted descending, and must not include lower-score entries
     * that a plain sort-and-truncate would still hold in memory.
     */
    @Test
    public void searchMergesTopKFromAllInstances() throws Exception {
        // Instance A scores: 0.9, 0.7, 0.5, 0.3
        FakeIndexingServer serverA = start(new StaticResponseImpl(Arrays.asList(
                entry(new byte[]{'a', 1}, 0.9f),
                entry(new byte[]{'a', 2}, 0.7f),
                entry(new byte[]{'a', 3}, 0.5f),
                entry(new byte[]{'a', 4}, 0.3f))));
        // Instance B scores: 0.85, 0.6, 0.4, 0.2
        FakeIndexingServer serverB = start(new StaticResponseImpl(Arrays.asList(
                entry(new byte[]{'b', 1}, 0.85f),
                entry(new byte[]{'b', 2}, 0.6f),
                entry(new byte[]{'b', 3}, 0.4f),
                entry(new byte[]{'b', 4}, 0.2f))));

        client = new IndexingServiceClient(Arrays.asList(serverA.address(), serverB.address()), 10);

        List<Map.Entry<Bytes, Float>> results =
                client.search("herd", "t1", "vidx", new float[]{1, 0, 0}, 3);

        assertEquals(3, results.size());
        // Expected top-3: a1 (0.9), b1 (0.85), a2 (0.7) — descending by score.
        assertEquals(Float.valueOf(0.9f), results.get(0).getValue());
        assertEquals(Float.valueOf(0.85f), results.get(1).getValue());
        assertEquals(Float.valueOf(0.7f), results.get(2).getValue());

        assertEquals(Bytes.from_array(new byte[]{'a', 1}), results.get(0).getKey());
        assertEquals(Bytes.from_array(new byte[]{'b', 1}), results.get(1).getKey());
        assertEquals(Bytes.from_array(new byte[]{'a', 2}), results.get(2).getKey());
    }

    /**
     * Two slow instances (~700 ms each). With a sequential loop this would
     * take ~1.4 s; with parallel dispatch it must finish in well under 1 s.
     * Asserts both correctness and that the fan-out actually runs in parallel.
     */
    @Test
    public void searchParallelDispatchRunsWithinSingleDeadline() throws Exception {
        long delayMs = 700;
        FakeIndexingServer slowA = start(new DelayedResponseImpl(Collections.singletonList(
                entry(new byte[]{'a'}, 0.9f)), delayMs));
        FakeIndexingServer slowB = start(new DelayedResponseImpl(Collections.singletonList(
                entry(new byte[]{'b'}, 0.8f)), delayMs));

        client = new IndexingServiceClient(Arrays.asList(slowA.address(), slowB.address()), 10);

        long start = System.nanoTime();
        List<Map.Entry<Bytes, Float>> results =
                client.search("herd", "t1", "vidx", new float[]{1, 0, 0}, 5);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertEquals(2, results.size());
        assertTrue("parallel fan-out should finish well under 2 * delayMs, elapsed=" + elapsedMs + "ms",
                elapsedMs < (2L * delayMs) - 100);
    }

    /**
     * A single-instance deployment must take the blocking fast-path and still
     * return the expected results unchanged.
     */
    @Test
    public void searchSingleInstanceUsesBlockingFastPath() throws Exception {
        FakeIndexingServer only = start(new StaticResponseImpl(Arrays.asList(
                entry(new byte[]{1}, 0.9f),
                entry(new byte[]{2}, 0.5f))));

        client = new IndexingServiceClient(Collections.singletonList(only.address()), 10);

        List<Map.Entry<Bytes, Float>> results =
                client.search("herd", "t1", "vidx", new float[]{1, 0, 0}, 5);

        assertEquals(2, results.size());
        assertEquals(Float.valueOf(0.9f), results.get(0).getValue());
        assertEquals(Float.valueOf(0.5f), results.get(1).getValue());
    }

    // ----------------- helpers -----------------

    private static SearchResult entry(byte[] pk, float score) {
        return SearchResult.newBuilder()
                .setPrimaryKey(ByteString.copyFrom(pk))
                .setScore(score)
                .build();
    }

    /** Thin wrapper that binds a BindableService on a random TCP port. */
    private static final class FakeIndexingServer implements AutoCloseable {
        private final io.grpc.BindableService impl;
        private Server server;

        FakeIndexingServer(io.grpc.BindableService impl) {
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

    /** Returns a fixed list of entries on every search. */
    private static final class StaticResponseImpl extends IndexingServiceGrpc.IndexingServiceImplBase {
        private final List<SearchResult> entries;

        StaticResponseImpl(List<SearchResult> entries) {
            this.entries = entries;
        }

        @Override
        public void search(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {
            responseObserver.onNext(SearchResponse.newBuilder().addAllResults(entries).build());
            responseObserver.onCompleted();
        }
    }

    /** Throws the given gRPC status on every search. */
    private static final class ThrowingImpl extends IndexingServiceGrpc.IndexingServiceImplBase {
        private final Status status;

        ThrowingImpl(Status status) {
            this.status = status;
        }

        @Override
        public void search(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {
            responseObserver.onError(status.asRuntimeException());
        }
    }

    /** Sleeps for {@code delayMs} before returning a fixed list of entries. */
    private static final class DelayedResponseImpl extends IndexingServiceGrpc.IndexingServiceImplBase {
        private final List<SearchResult> entries;
        private final long delayMs;

        DelayedResponseImpl(List<SearchResult> entries, long delayMs) {
            this.entries = entries;
            this.delayMs = delayMs;
        }

        @Override
        public void search(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                responseObserver.onError(Status.ABORTED.withCause(e).asRuntimeException());
                return;
            }
            responseObserver.onNext(SearchResponse.newBuilder().addAllResults(entries).build());
            responseObserver.onCompleted();
        }
    }
}
