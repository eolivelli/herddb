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
import herddb.indexing.admin.IndexingAdminCli;
import herddb.indexing.proto.GetShadowStatusResponse;
import herddb.indexing.proto.IndexingServiceGrpc;
import herddb.indexing.proto.WaitForCheckpointRequest;
import herddb.indexing.proto.WaitForCheckpointResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies the GetShadowStatus + WaitForCheckpoint RPCs added in step 8, and
 * the three CLI subcommands that wrap them (shadow-status, wait-shadow). The
 * engine is a minimal stub that exposes exactly the accessors these RPCs
 * call, so we test the wiring end-to-end without spinning up a full shadow.
 */
public class ShadowAdminCliAndRpcTest {

    private static final class StubEngine extends IndexingServiceEngine {
        volatile boolean shadow;
        volatile boolean ready;
        volatile int shadowOf = -1;
        volatile herddb.log.LogSequenceNumber loaded;
        volatile herddb.log.LogSequenceNumber advertised;
        volatile long lastReload;
        volatile long reloadCount;

        StubEngine() {
            super(Paths.get("/tmp/noop-log"), Paths.get("/tmp/noop-data"),
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

        @Override
        public herddb.log.LogSequenceNumber getShadowLoadedLsn() {
            return loaded;
        }

        @Override
        public herddb.log.LogSequenceNumber getPrimaryAdvertisedLsn() {
            return advertised;
        }

        @Override
        public long getShadowLastReloadTimestampMs() {
            return lastReload;
        }

        @Override
        public long getShadowReloadCount() {
            return reloadCount;
        }

        @Override
        public herddb.log.LogSequenceNumber getLastProcessedLsn() {
            return loaded;
        }

        @Override
        public String getInstanceIdLabel() {
            return "stub";
        }

        @Override
        public String getTableSpaceUUID() {
            return "ts-uuid";
        }
    }

    private StubEngine engine;
    private Server server;
    private ManagedChannel channel;
    private IndexingServiceGrpc.IndexingServiceBlockingStub stub;
    private String address;

    @Before
    public void setUp() throws Exception {
        engine = new StubEngine();
        IndexingServiceImpl svc = new IndexingServiceImpl(engine, NullStatsLogger.INSTANCE);
        server = ServerBuilder.forPort(0).addService(svc).build().start();
        address = "localhost:" + server.getPort();
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

    @Test
    public void getShadowStatusReturnsEngineFields() {
        engine.shadow = true;
        engine.shadowOf = 0;
        engine.ready = true;
        engine.loaded = new herddb.log.LogSequenceNumber(3, 100);
        engine.advertised = new herddb.log.LogSequenceNumber(3, 200);
        engine.lastReload = 123L;
        engine.reloadCount = 7L;

        GetShadowStatusResponse resp = stub.getShadowStatus(
                herddb.indexing.proto.GetShadowStatusRequest.getDefaultInstance());
        assertEquals(true, resp.getIsShadow());
        assertEquals(0, resp.getShadowOf());
        assertEquals(true, resp.getReady());
        assertEquals(3L, resp.getLoadedLedgerId());
        assertEquals(100L, resp.getLoadedOffset());
        assertEquals(200L, resp.getPrimaryAdvertisedOffset());
        assertEquals(7L, resp.getReloadCount());
    }

    @Test
    public void waitForCheckpointReturnsReachedImmediately() {
        engine.shadow = true;
        engine.ready = true;
        engine.loaded = new herddb.log.LogSequenceNumber(5, 500);

        WaitForCheckpointResponse resp = stub.waitForCheckpoint(
                WaitForCheckpointRequest.newBuilder()
                        .setTargetLedgerId(5).setTargetOffset(100)  // already reached
                        .setTimeoutMs(1000)
                        .build());
        assertEquals(true, resp.getReached());
        assertEquals(5L, resp.getCurrentLedgerId());
        assertEquals(500L, resp.getCurrentOffset());
    }

    @Test
    public void waitForCheckpointTimesOutWhenTargetUnreachable() {
        engine.shadow = true;
        engine.ready = true;
        engine.loaded = new herddb.log.LogSequenceNumber(1, 50);

        long start = System.currentTimeMillis();
        WaitForCheckpointResponse resp = stub.waitForCheckpoint(
                WaitForCheckpointRequest.newBuilder()
                        .setTargetLedgerId(99).setTargetOffset(99)
                        .setTimeoutMs(400)
                        .build());
        long elapsed = System.currentTimeMillis() - start;
        assertEquals(false, resp.getReached());
        assertTrue("must have waited roughly the timeout, was " + elapsed + "ms",
                elapsed >= 300 && elapsed < 2500);
    }

    @Test
    public void shadowStatusCliCommand() {
        engine.shadow = true;
        engine.shadowOf = 2;
        engine.ready = true;
        engine.loaded = new herddb.log.LogSequenceNumber(1, 10);
        engine.reloadCount = 4;

        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        IndexingAdminCli cli = new IndexingAdminCli(
                new PrintStream(stdout, true, StandardCharsets.UTF_8),
                new PrintStream(stderr, true, StandardCharsets.UTF_8));
        int rc = cli.run(new String[]{"shadow-status", "--server", address});
        String out = new String(stdout.toByteArray(), StandardCharsets.UTF_8);
        assertEquals(out, 0, rc);
        assertTrue(out, out.contains("is_shadow=true"));
        assertTrue(out, out.contains("shadow_of=2"));
        assertTrue(out, out.contains("ready=true"));
        assertTrue(out, out.contains("reload_count=4"));
    }

    @Test
    public void waitShadowCliExitCodes() {
        engine.shadow = true;
        engine.ready = true;
        engine.loaded = new herddb.log.LogSequenceNumber(10, 1000);

        // Target already reached → exit code 0.
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        IndexingAdminCli cli = new IndexingAdminCli(
                new PrintStream(stdout, true, StandardCharsets.UTF_8),
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
        int rc = cli.run(new String[]{"wait-shadow",
                "--server", address,
                "--tablespace", "default",
                "--table", "t", "--index", "vidx",
                "--target-ledger-id", "10", "--target-offset", "100",
                "--timeout-ms", "500"});
        assertEquals(new String(stdout.toByteArray(), StandardCharsets.UTF_8), 0, rc);

        // Target unreachable → exit code 1 after timeout.
        stdout = new ByteArrayOutputStream();
        cli = new IndexingAdminCli(
                new PrintStream(stdout, true, StandardCharsets.UTF_8),
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
        int rc2 = cli.run(new String[]{"wait-shadow",
                "--server", address,
                "--tablespace", "default",
                "--table", "t", "--index", "vidx",
                "--target-ledger-id", "999", "--target-offset", "0",
                "--timeout-ms", "300"});
        assertEquals(new String(stdout.toByteArray(), StandardCharsets.UTF_8), 1, rc2);
    }
}
