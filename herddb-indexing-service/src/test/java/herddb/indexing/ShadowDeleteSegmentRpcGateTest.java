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
import herddb.indexing.proto.DeleteSegmentRequest;
import herddb.indexing.proto.IndexingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.file.Paths;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * pr-reviewer follow-up #4 for issue #617: exercises the
 * <em>real-server</em> shadow gate on the {@code DeleteSegment} RPC.
 *
 * <p>The existing
 * {@code IndexingAdminCliDeleteSegmentTest.shadowConfiguredServerRejectsWithNotPrimaryPrefix}
 * test runs against a hand-rolled gRPC fake that returns a hard-coded
 * {@code FAILED_PRECONDITION} response — it validates the CLI's surface,
 * not the production gate at
 * {@link IndexingServiceImpl#deleteSegment(DeleteSegmentRequest,
 * io.grpc.stub.StreamObserver)}. A regression that deleted the production
 * gate would still pass that fake-server test.
 *
 * <p>This test mirrors the {@link ShadowAdminCliAndRpcTest} harness: a real
 * {@link IndexingServiceImpl} wired to a real {@link IndexingServiceEngine}
 * stub configured as a shadow ({@code isConfiguredAsShadow() == true}),
 * exposed via an in-process gRPC server, invoked via the real blocking
 * stub. The engine's {@code deleteSegment} is intentionally NOT overridden
 * — the test would FAIL if it were ever reached, proving the production
 * shadow gate at the service boundary is what's blocking the call.
 *
 * <p>Asserts the documented error contract:
 * <ul>
 *   <li>{@code Status.Code == FAILED_PRECONDITION};</li>
 *   <li>description starts with
 *       {@code "not_primary: this indexing-service instance is a shadow replica (shadowOf="};</li>
 *   <li>description contains {@code "target the primary instead"}.</li>
 * </ul>
 */
public class ShadowDeleteSegmentRpcGateTest {

    /**
     * Minimal {@link IndexingServiceEngine} subclass: exposes shadow=true with
     * a configurable shadowOf, and BLOWS UP if anything ever calls the
     * engine's own {@code deleteSegment} — that would mean the service-level
     * shadow gate was bypassed.
     */
    private static final class ShadowStubEngine extends IndexingServiceEngine {
        volatile boolean shadow = true;
        volatile int shadowOf = 0;

        ShadowStubEngine() {
            super(Paths.get("/tmp/noop-log-shadow-rpc"),
                    Paths.get("/tmp/noop-data-shadow-rpc"),
                    new IndexingServerConfiguration());
        }

        @Override
        public boolean isConfiguredAsShadow() {
            return shadow;
        }

        @Override
        public boolean isShadowReady() {
            // Ready=true so the IndexingServiceImpl @Override interceptor for
            // shadow-not-ready does NOT short-circuit our shadow gate ahead of
            // time. Ready vs not-ready is orthogonal to the DeleteSegment gate
            // — we want to prove the DeleteSegment gate fires even when the
            // shadow is otherwise healthy.
            return true;
        }

        @Override
        public int getShadowOfOrMinusOne() {
            return shadowOf;
        }

        @Override
        public String getInstanceIdLabel() {
            return "shadow-stub";
        }

        @Override
        public String getTableSpaceUUID() {
            return "ts-uuid";
        }

        @Override
        public DeleteSegmentResult deleteSegment(String table, String indexName,
                                                  String segmentStorageKey,
                                                  boolean purgeStorage, boolean force) {
            // If this is ever reached the service-level shadow gate was
            // deleted or bypassed. The test asserts that this does NOT
            // happen — the gRPC stub call must terminate inside
            // IndexingServiceImpl.deleteSegment with FAILED_PRECONDITION,
            // before this method is ever invoked.
            throw new AssertionError("engine.deleteSegment must NOT be reached "
                    + "when the service is configured as a shadow — the production"
                    + " gate at IndexingServiceImpl.deleteSegment must short-circuit"
                    + " the RPC with not_primary:");
        }
    }

    private ShadowStubEngine engine;
    private Server server;
    private ManagedChannel channel;
    private IndexingServiceGrpc.IndexingServiceBlockingStub stub;

    @Before
    public void setUp() throws Exception {
        engine = new ShadowStubEngine();
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
            server.shutdownNow().awaitTermination();
        }
    }

    /**
     * pr-reviewer follow-up #4: the real-server gate must reject the RPC
     * with the documented FAILED_PRECONDITION prefix WITHOUT ever entering
     * the engine. ShadowStubEngine.deleteSegment throws AssertionError if
     * reached — that fails the test, proving the gate is real.
     */
    @Test
    public void deleteSegmentRpcOnShadowReturnsNotPrimaryFailedPrecondition() {
        engine.shadow = true;
        engine.shadowOf = 0;

        DeleteSegmentRequest request = DeleteSegmentRequest.newBuilder()
                .setTablespace("ts")
                .setTable("vectable")
                .setIndex("vidx")
                .setSegment("vidx_aaa_seg1")
                .setPurgeStorage(false)
                .setForce(false)
                .build();
        try {
            stub.deleteSegment(request);
            fail("DeleteSegment RPC on shadow must throw StatusRuntimeException "
                    + "with FAILED_PRECONDITION");
        } catch (StatusRuntimeException e) {
            assertEquals("status code must be FAILED_PRECONDITION",
                    Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
            String desc = e.getStatus().getDescription();
            assertNotNull("status must carry a description", desc);
            String expectedPrefix = "not_primary: this indexing-service instance is a"
                    + " shadow replica (shadowOf=";
            assertTrue("status description must start with the documented "
                            + "not_primary prefix, got: " + desc,
                    desc.startsWith(expectedPrefix));
            assertTrue("status description must instruct the operator to target"
                            + " the primary, got: " + desc,
                    desc.contains("target the primary instead"));
            // Sanity: shadowOf=0 from our stub must appear in the message.
            assertTrue("status description must include the shadowOf integer, got: " + desc,
                    desc.contains("shadowOf=0"));
        }
    }

    /**
     * Defensive: when shadow=true but shadowOf=-1 (configured-as-shadow
     * without a numeric pointer), the gate still fires with the documented
     * prefix. shadowOf="-1" still flows through the message verbatim.
     */
    @Test
    public void deleteSegmentRpcRejectsEvenWithoutKnownShadowOf() {
        engine.shadow = true;
        engine.shadowOf = -1;

        DeleteSegmentRequest request = DeleteSegmentRequest.newBuilder()
                .setTable("vectable")
                .setIndex("vidx")
                .setSegment("vidx_aaa_seg2")
                .build();
        try {
            stub.deleteSegment(request);
            fail("DeleteSegment RPC on shadow with shadowOf=-1 must still be"
                    + " rejected with FAILED_PRECONDITION");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
            String desc = e.getStatus().getDescription();
            assertNotNull(desc);
            assertTrue("description must start with not_primary prefix, got: " + desc,
                    desc.startsWith("not_primary: this indexing-service instance is a"
                            + " shadow replica (shadowOf="));
            assertTrue("description must instruct to target the primary, got: " + desc,
                    desc.contains("target the primary instead"));
        }
    }
}
