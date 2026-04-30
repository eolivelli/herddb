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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.indexing.proto.GetIndexStatusRequest;
import herddb.indexing.proto.GetIndexStatusResponse;
import herddb.indexing.proto.IndexingServiceGrpc;
import herddb.log.LogSequenceNumber;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that {@link IndexingServiceClient} consumes the durable LSN
 * advertised by an IS instance — never the in-memory tailer LSN — when
 * computing the server-side commit-log retention floor and when waiting
 * for catch-up. This is the wire-side half of the issue #364 fix; the
 * engine-side half is in {@link IndexingServiceEngineDurableLsnTest}.
 *
 * <p>Each test wires a tiny in-process gRPC server that responds to
 * {@code GetIndexStatus} with hand-crafted LSN pairs, then asserts the
 * client behaviour observable from
 * {@link IndexingServiceClient#getMinProcessedLsn} and
 * {@link IndexingServiceClient#waitForCatchUp}.
 */
public class IndexingServiceClientDurableLsnTest {

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

    /**
     * Pure-protocol test: when the IS advertises {@code tailer_lsn=(70,200)}
     * and {@code durable_lsn=(50,10)}, the retention floor returned to the
     * server must be {@code (50,10)}. Reading the tailer LSN here would
     * pin retention 20 ledgers ahead of the recovery floor and re-introduce
     * the BIGANN 1B failure mode.
     */
    @Test
    public void getMinProcessedLsnReadsDurableLsnNotTailerLsn() throws Exception {
        StaticIndexStatusImpl impl = new StaticIndexStatusImpl(
                /*tailerLedger*/ 70, /*tailerOffset*/ 200,
                /*durableLedger*/ 50, /*durableOffset*/ 10);
        fakeServer = new FakeServer(impl);
        fakeServer.start();

        try (IndexingServiceClient client = IndexingServiceClient.fromAddresses(
                Arrays.asList(fakeServer.address()), 5)) {
            Optional<LogSequenceNumber> floor = client.getMinProcessedLsn("ts1");
            assertTrue("retention floor must be present", floor.isPresent());
            assertEquals("retention floor MUST be the durable LSN, not the tailer LSN",
                    new LogSequenceNumber(50, 10), floor.get());
        }
    }

    /**
     * When an IS instance has not yet completed any successful checkpoint
     * (durable_lsn = (0, 0) — proto default) it has nothing to recover
     * from. The client must treat that as
     * {@link LogSequenceNumber#START_OF_TIME} so the server pins retention
     * and never drops a ledger the IS would still need.
     *
     * <p>This also covers the wire-default case for old/missing fields:
     * even though the proto change is breaking, a misconfigured peer that
     * leaves the durable fields unset MUST fall back to the safe pin.
     */
    @Test
    public void getMinProcessedLsnPinsWhenDurableLsnIsUnset() throws Exception {
        StaticIndexStatusImpl impl = new StaticIndexStatusImpl(
                /*tailerLedger*/ 70, /*tailerOffset*/ 200,
                /*durableLedger*/ 0, /*durableOffset*/ 0);
        fakeServer = new FakeServer(impl);
        fakeServer.start();

        try (IndexingServiceClient client = IndexingServiceClient.fromAddresses(
                Arrays.asList(fakeServer.address()), 5)) {
            Optional<LogSequenceNumber> floor = client.getMinProcessedLsn("ts1");
            assertTrue("retention floor must be present", floor.isPresent());
            assertEquals(
                    "retention floor MUST be START_OF_TIME when no durable watermark exists",
                    LogSequenceNumber.START_OF_TIME, floor.get());
        }
    }

    /**
     * {@link IndexingServiceClient#waitForCatchUp} is a queryability gate:
     * it must compare against the tailer LSN (in-memory progress), NOT the
     * durable LSN. WAITFORINDEXES is what SQL callers use to know "is it
     * safe to issue a query?" — that question is about whether the in-memory
     * state has all the entries, not whether they are durably checkpointed.
     * Recovery / retention is the SEPARATE concern handled by
     * {@link IndexingServiceClient#getMinProcessedLsn}.
     */
    @Test
    public void waitForCatchUpComparesAgainstTailerLsn() throws Exception {
        // tailer is at (70, 200) — past the target (60, 0). Even though
        // durable is well behind (50, 10), waitForCatchUp must return true
        // because the IS has the entries in memory and is queryable now.
        StaticIndexStatusImpl impl = new StaticIndexStatusImpl(
                70, 200, 50, 10);
        fakeServer = new FakeServer(impl);
        fakeServer.start();

        try (IndexingServiceClient client = IndexingServiceClient.fromAddresses(
                Arrays.asList(fakeServer.address()), 5)) {
            boolean caughtUp = client.waitForCatchUp(
                    "ts1", new LogSequenceNumber(60, 0), 5000);
            assertTrue(
                    "waitForCatchUp must return true when the tailer LSN (70,200) is "
                            + "at or past the target (60,0), regardless of durable LSN",
                    caughtUp);
        }
    }

    /**
     * The mirror of the previous test: when the tailer LSN is short of the
     * target, waitForCatchUp returns false on timeout — even if the durable
     * LSN happens to be advanced (e.g. recovered from snapshot).
     */
    @Test
    public void waitForCatchUpReturnsFalseWhenTailerLsnShortOfTarget() throws Exception {
        StaticIndexStatusImpl impl = new StaticIndexStatusImpl(
                /*tailer*/ 50, 5, /*durable*/ 50, 5);
        fakeServer = new FakeServer(impl);
        fakeServer.start();

        try (IndexingServiceClient client = IndexingServiceClient.fromAddresses(
                Arrays.asList(fakeServer.address()), 2)) {
            boolean caughtUp = client.waitForCatchUp(
                    "ts1", new LogSequenceNumber(60, 50), 1500);
            assertFalse(
                    "waitForCatchUp must return false when the tailer is short of the target",
                    caughtUp);
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

    /**
     * Returns a fixed {@link GetIndexStatusResponse} on every call, so the
     * test can inspect exactly which fields the client reads and which it
     * ignores.
     */
    private static final class StaticIndexStatusImpl
            extends IndexingServiceGrpc.IndexingServiceImplBase {
        private final long tailerLedger;
        private final long tailerOffset;
        private final long durableLedger;
        private final long durableOffset;

        StaticIndexStatusImpl(long tailerLedger, long tailerOffset,
                              long durableLedger, long durableOffset) {
            this.tailerLedger = tailerLedger;
            this.tailerOffset = tailerOffset;
            this.durableLedger = durableLedger;
            this.durableOffset = durableOffset;
        }

        @Override
        public void getIndexStatus(GetIndexStatusRequest request,
                                    StreamObserver<GetIndexStatusResponse> responseObserver) {
            GetIndexStatusResponse resp = GetIndexStatusResponse.newBuilder()
                    .setVectorCount(0)
                    .setSegmentCount(0)
                    .setTailerLsnLedger(tailerLedger)
                    .setTailerLsnOffset(tailerOffset)
                    .setDurableLsnLedger(durableLedger)
                    .setDurableLsnOffset(durableOffset)
                    .setStatus("static")
                    .build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }
    }
}
