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
import herddb.index.vector.VectorIndexManager;
import herddb.indexing.proto.IndexingServiceGrpc;
import herddb.indexing.proto.SearchRequest;
import herddb.indexing.proto.SearchResponse;
import herddb.indexing.proto.SearchResult;
import herddb.model.StatementExecutionException;
import herddb.utils.Bytes;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Test;

/**
 * Exercises {@link VectorIndexManager.ExpandingSearchIterator} layered on top
 * of a multi-instance {@link IndexingServiceClient}. The iterator calls the
 * client once per expansion — each call fans out to every configured gRPC
 * instance in parallel, the client merges into a bounded top-K, and the
 * iterator then dedupes across expansions on the data-node side.
 *
 * <p>These tests use lightweight in-process gRPC servers that honour the
 * requested {@code limit} and return a deterministic slice of a pre-seeded
 * score list, so the entire expansion schedule is observable.
 */
public class IndexingServiceClientPredicateFanOutTest {

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
     * Fan-out over 3 instances, each owning a disjoint subset of vectors. The
     * iterator drains all three shards in interleaved score order and must
     * not emit duplicates across expansions. A simulated predicate keeps only
     * even-pk entries; the iterator must keep expanding until 5 matches show
     * up.
     */
    @Test
    public void multiInstanceFanOutWithPredicate() throws Exception {
        // Instance A: pks 0,3,6,9,... scores decreasing.
        // Instance B: pks 1,4,7,10,...
        // Instance C: pks 2,5,8,11,...
        // Score of pk i == 1.0 - i * 0.001 so global ann order == ascending pk.
        LimitedResponseImpl implA = new LimitedResponseImpl(seedByMod(0, 3, 60));
        LimitedResponseImpl implB = new LimitedResponseImpl(seedByMod(1, 3, 60));
        LimitedResponseImpl implC = new LimitedResponseImpl(seedByMod(2, 3, 60));
        FakeIndexingServer a = start(implA);
        FakeIndexingServer b = start(implB);
        FakeIndexingServer c = start(implC);

        client = new IndexingServiceClient(
                Arrays.asList(a.address(), b.address(), c.address()), 10);

        // Simulate VectorANNScanOp: ask for 5 matches where "predicate" is
        // pk % 2 == 0. Start with the same budget / factor / floor that
        // VectorANNScanOp uses in production.
        int matched = 0;
        List<Integer> hitPks = new ArrayList<>();
        Set<Bytes> distinct = new HashSet<>();
        try (VectorIndexManager.SearchIterator it =
                     VectorIndexManager.newSearchIteratorForTest(client,
                             "herd", "t1", "vidx", new float[]{1, 0, 0},
                             5, 1.5f, 16, 6, null, null)) {
            while (it.hasNext() && matched < 5) {
                Map.Entry<Bytes, Float> e = it.next();
                assertTrue("no duplicates across expansions",
                        distinct.add(e.getKey()));
                int pk = e.getKey().to_int();
                if (pk % 2 == 0) {
                    hitPks.add(pk);
                    matched++;
                }
            }
        }
        assertEquals("5 predicate-matching rows must be returned", 5, matched);
        // Expected ann order of even pks is 0, 2, 4, 6, 8 (ascending pk).
        assertEquals(Arrays.asList(0, 2, 4, 6, 8), hitPks);
    }

    /**
     * One instance has only 2 vectors, another has 50. A predicate forces
     * the iterator to keep expanding; the small instance is drained on the
     * first call and further expansions must still return more entries from
     * the large instance without looping forever.
     */
    @Test
    public void multiInstanceFanOutWithOneExhaustedInstance() throws Exception {
        // Instance A: 2 entries with very high score.
        LimitedResponseImpl implA = new LimitedResponseImpl(Arrays.asList(
                entry(Bytes.from_int(1).getBuffer(), 0.99f),
                entry(Bytes.from_int(2).getBuffer(), 0.98f)));
        // Instance B: 50 entries with decreasing score from 0.97 down.
        List<SearchResult> bSeed = new ArrayList<>();
        for (int i = 3; i <= 52; i++) {
            bSeed.add(entry(Bytes.from_int(i).getBuffer(), 0.97f - (i - 3) * 0.001f));
        }
        LimitedResponseImpl implB = new LimitedResponseImpl(bSeed);
        FakeIndexingServer a = start(implA);
        FakeIndexingServer b = start(implB);

        client = new IndexingServiceClient(Arrays.asList(a.address(), b.address()), 10);

        // Simulate predicate "pk is odd" → 26 matches across {1,3,5,...,51}
        // Ask for LIMIT 10.
        int matched = 0;
        Set<Bytes> distinct = new HashSet<>();
        try (VectorIndexManager.SearchIterator it =
                     VectorIndexManager.newSearchIteratorForTest(client,
                             "herd", "t1", "vidx", new float[]{1, 0, 0},
                             10, 1.5f, 16, 6, null, null)) {
            while (it.hasNext() && matched < 10) {
                Map.Entry<Bytes, Float> e = it.next();
                assertTrue("no duplicates", distinct.add(e.getKey()));
                int pk = e.getKey().to_int();
                if (pk % 2 == 1) {
                    matched++;
                }
            }
        }
        assertEquals("10 odd-pk rows must be returned", 10, matched);
        // Drain test: the second call per-instance should have received at
        // least one merge from the larger instance — validated by the fact
        // that we matched 10 while only 1 odd pk exists in instance A.
    }

    /**
     * First fan-out succeeds; second fan-out (during expansion) errors on
     * instance B → the iterator must propagate the error and not return
     * partial results. Mirrors {@code searchFailsFastWhenOneInstanceThrows}
     * but exercises the failure on the expansion RPC specifically.
     */
    @Test
    public void multiInstanceFailFastOnExpansion() throws Exception {
        // A always returns 16 entries with ascending pks — enough for the
        // initial budget but not enough for the iterator to give up asking
        // for more when the caller keeps pulling.
        List<SearchResult> aSeed = new ArrayList<>();
        for (int i = 1; i <= 32; i++) {
            aSeed.add(entry(Bytes.from_int(i).getBuffer(), 1.0f - i * 0.001f));
        }
        LimitedResponseImpl implA = new LimitedResponseImpl(aSeed);
        // B is healthy on the first call, errors on the second.
        FlakyImpl implB = new FlakyImpl(Status.UNAVAILABLE.withDescription("boom"));
        FakeIndexingServer a = start(implA);
        FakeIndexingServer b = start(implB);

        client = new IndexingServiceClient(Arrays.asList(a.address(), b.address()), 10);

        // Use factor=1.0, floor=16, so initial budget=16 and the first RPC
        // succeeds. The caller drains exactly the first batch, then the
        // next hasNext()/next() triggers a second fan-out that errors on B.
        try (VectorIndexManager.SearchIterator it =
                     VectorIndexManager.newSearchIteratorForTest(client,
                             "herd", "t1", "vidx", new float[]{1, 0, 0},
                             10, 1.0f, 16, 6, null, null)) {
            // Drain exactly 16 entries without calling hasNext after the
            // final next() — we want to avoid triggering the expansion
            // RPC inside the drain loop and keep the fail-fast assertion
            // localized to the "pull one more" step below.
            for (int i = 0; i < 16; i++) {
                assertTrue("initial batch has at least 16 entries", it.hasNext());
                it.next();
            }
            try {
                if (it.hasNext()) {
                    it.next();
                }
                fail("expected the expansion RPC to fail fast");
            } catch (StatementExecutionException expected) {
                Throwable root = expected;
                while (root.getCause() != null) {
                    root = root.getCause();
                }
                assertNotNull(root.getMessage());
                assertTrue("root cause mentions UNAVAILABLE or boom: " + root.getMessage(),
                        root.getMessage().contains("UNAVAILABLE")
                                || root.getMessage().contains("boom"));
            }
        }
        // B must have been called at least twice (once OK, once error).
        assertTrue("instance B was called on expansion", implB.callCount() >= 2);
    }

    /**
     * Single-instance deployment with predicate: the fast path still works.
     * Asserts one RPC per expansion (no future/fan-out overhead).
     */
    @Test
    public void singleInstanceFastPathWithExpansion() throws Exception {
        // 100 entries — expansion will keep asking for more as long as the
        // caller keeps pulling.
        List<SearchResult> seed = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            seed.add(entry(Bytes.from_int(i).getBuffer(), 1.0f - i * 0.001f));
        }
        LimitedResponseImpl impl = new LimitedResponseImpl(seed);
        FakeIndexingServer only = start(impl);

        client = new IndexingServiceClient(Collections.singletonList(only.address()), 10);

        int drained = 0;
        try (VectorIndexManager.SearchIterator it =
                     VectorIndexManager.newSearchIteratorForTest(client,
                             "herd", "t1", "vidx", new float[]{1, 0, 0},
                             10, 1.5f, 16, 6, null, null)) {
            while (it.hasNext() && drained < 50) {
                it.next();
                drained++;
            }
            assertEquals(50, drained);
        }
        // We started at budget=16 and doubled each time: 16, 32, 64, ...
        // drained=50 should be covered by the first two calls (16 + 16 new
        // from the 32-budget call = 32 entries). The third call (budget=64)
        // yields 32 new → reaches 64, more than 50. So 3 RPCs total.
        assertTrue("at most a handful of RPCs expected", impl.callCount() <= 4);
        assertTrue("at least 2 RPCs for expansion", impl.callCount() >= 2);
    }

    // ----------------- helpers -----------------

    private static SearchResult entry(byte[] pk, float score) {
        return SearchResult.newBuilder()
                .setPrimaryKey(ByteString.copyFrom(pk))
                .setScore(score)
                .build();
    }

    /**
     * Generates a seed list of SearchResults containing integer PKs that
     * match {@code pk % modulus == ordinal}, starting from {@code ordinal}
     * and stepping by {@code modulus}, with {@code count} total entries.
     * Scores are {@code 1.0 - pk * 0.001f} so ascending pk == descending score.
     */
    private static List<SearchResult> seedByMod(int ordinal, int modulus, int count) {
        List<SearchResult> out = new ArrayList<>(count);
        int pk = ordinal;
        for (int i = 0; i < count; i++, pk += modulus) {
            out.add(entry(Bytes.from_int(pk).getBuffer(), 1.0f - pk * 0.001f));
        }
        return out;
    }

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

    /**
     * Returns up to {@code request.getLimit()} entries from a fixed list.
     * Counts calls so tests can assert the expansion schedule.
     */
    private static final class LimitedResponseImpl extends IndexingServiceGrpc.IndexingServiceImplBase {
        private final List<SearchResult> entries;
        private final AtomicInteger callCount = new AtomicInteger();

        LimitedResponseImpl(List<SearchResult> entries) {
            this.entries = entries;
        }

        int callCount() {
            return callCount.get();
        }

        @Override
        public void search(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {
            callCount.incrementAndGet();
            int limit = Math.min(request.getLimit(), entries.size());
            responseObserver.onNext(SearchResponse.newBuilder()
                    .addAllResults(entries.subList(0, limit))
                    .build());
            responseObserver.onCompleted();
        }
    }

    /**
     * Returns healthy results on the first call, then fails every subsequent
     * call with the given gRPC status. Used to simulate a fault that only
     * surfaces on the expansion RPC.
     */
    private static final class FlakyImpl extends IndexingServiceGrpc.IndexingServiceImplBase {
        private final Status errorStatus;
        private final AtomicInteger callCount = new AtomicInteger();

        FlakyImpl(Status errorStatus) {
            this.errorStatus = errorStatus;
        }

        int callCount() {
            return callCount.get();
        }

        @Override
        public void search(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {
            int c = callCount.incrementAndGet();
            if (c == 1) {
                // Healthy first call: return some high-score entries.
                responseObserver.onNext(SearchResponse.newBuilder()
                        .addResults(entry(Bytes.from_int(100).getBuffer(), 0.99f))
                        .addResults(entry(Bytes.from_int(101).getBuffer(), 0.98f))
                        .build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(errorStatus.asRuntimeException());
            }
        }
    }
}
