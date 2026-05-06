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
import herddb.index.vector.RemoteVectorIndexService;
import herddb.indexing.proto.GetIndexStatusRequest;
import herddb.indexing.proto.GetIndexStatusResponse;
import herddb.indexing.proto.IndexingServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Issue #423: wire-level test that
 * {@link IndexingServiceClient#getIndexStatus(String, String, String)} maps
 * the proto {@code tailer_lsn_timestamp} and {@code durable_lsn_timestamp}
 * fields onto {@link RemoteVectorIndexService.IndexStatusInfo}.
 *
 * <p>An in-process gRPC server returns hand-crafted timestamps so the
 * client-side mapping can be verified independently of the engine.
 */
public class IndexingServiceClientLagTimestampsTest {

    private FakeServer fakeServer;

    @Before
    public void setUp() {
        fakeServer = null;
    }

    @After
    public void tearDown() throws InterruptedException {
        if (fakeServer != null) {
            fakeServer.close();
        }
    }

    @Test
    public void getIndexStatusCarriesTailerAndDurableTimestamps() throws Exception {
        long tailerTs = 1_700_000_001_500L;
        long durableTs = 1_699_999_999_000L;
        StaticIndexStatusImpl impl = new StaticIndexStatusImpl(
                /*tailerLedger*/ 70, /*tailerOffset*/ 200, tailerTs,
                /*durableLedger*/ 50, /*durableOffset*/ 10, durableTs);
        fakeServer = new FakeServer(impl);
        fakeServer.start();

        try (IndexingServiceClient client = IndexingServiceClient.fromAddresses(
                Arrays.asList(fakeServer.address()), 5)) {
            RemoteVectorIndexService.IndexStatusInfo info =
                    client.getIndexStatus("ts", "t", "i");
            assertEquals("tailer ledger", 70L, info.getTailerLsnLedger());
            assertEquals("tailer offset", 200L, info.getTailerLsnOffset());
            assertEquals("tailer timestamp must be carried over from the wire",
                    tailerTs, info.getTailerLsnTimestamp());
            assertEquals("durable ledger", 50L, info.getDurableLsnLedger());
            assertEquals("durable offset", 10L, info.getDurableLsnOffset());
            assertEquals("durable timestamp must be carried over from the wire",
                    durableTs, info.getDurableLsnTimestamp());
        }
    }

    /**
     * When the IS has not yet seen any entry / not yet checkpointed, both
     * timestamps must default to {@code 0} ("unknown") on the wire — the
     * client must NOT re-interpret them.
     */
    @Test
    public void getIndexStatusReportsZeroWhenServerHasNoTimestamps() throws Exception {
        StaticIndexStatusImpl impl = new StaticIndexStatusImpl(
                /*tailerLedger*/ -1, /*tailerOffset*/ -1, 0L,
                /*durableLedger*/ -1, /*durableOffset*/ -1, 0L);
        fakeServer = new FakeServer(impl);
        fakeServer.start();

        try (IndexingServiceClient client = IndexingServiceClient.fromAddresses(
                Arrays.asList(fakeServer.address()), 5)) {
            RemoteVectorIndexService.IndexStatusInfo info =
                    client.getIndexStatus("ts", "t", "i");
            assertEquals("tailer timestamp 0 = unknown",
                    0L, info.getTailerLsnTimestamp());
            assertEquals("durable timestamp 0 = unknown",
                    0L, info.getDurableLsnTimestamp());
        }
    }

    // -------------------- harness --------------------

    /** Wraps an in-process gRPC server bound to a random TCP port. */
    private static final class FakeServer implements AutoCloseable {
        private final StaticIndexStatusImpl impl;
        private Server server;

        FakeServer(StaticIndexStatusImpl impl) {
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

    private static final class StaticIndexStatusImpl
            extends IndexingServiceGrpc.IndexingServiceImplBase {
        private final long tailerLedger;
        private final long tailerOffset;
        private final long tailerTimestamp;
        private final long durableLedger;
        private final long durableOffset;
        private final long durableTimestamp;

        StaticIndexStatusImpl(long tailerLedger, long tailerOffset, long tailerTimestamp,
                              long durableLedger, long durableOffset, long durableTimestamp) {
            this.tailerLedger = tailerLedger;
            this.tailerOffset = tailerOffset;
            this.tailerTimestamp = tailerTimestamp;
            this.durableLedger = durableLedger;
            this.durableOffset = durableOffset;
            this.durableTimestamp = durableTimestamp;
        }

        @Override
        public void getIndexStatus(GetIndexStatusRequest request,
                                    StreamObserver<GetIndexStatusResponse> responseObserver) {
            GetIndexStatusResponse resp = GetIndexStatusResponse.newBuilder()
                    .setVectorCount(0)
                    .setSegmentCount(0)
                    .setTailerLsnLedger(tailerLedger)
                    .setTailerLsnOffset(tailerOffset)
                    .setTailerLsnTimestamp(tailerTimestamp)
                    .setDurableLsnLedger(durableLedger)
                    .setDurableLsnOffset(durableOffset)
                    .setDurableLsnTimestamp(durableTimestamp)
                    .setStatus("static")
                    .build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }
    }
}
