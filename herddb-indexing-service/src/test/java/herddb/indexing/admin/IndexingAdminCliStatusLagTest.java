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

package herddb.indexing.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.indexing.proto.GetIndexStatusRequest;
import herddb.indexing.proto.GetIndexStatusResponse;
import herddb.indexing.proto.IndexingServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Issue #423: the {@code status} sub-command of {@link IndexingAdminCli} must
 * surface {@code tailer_lsn_timestamp} / {@code durable_lsn_timestamp} and the
 * computed {@code tailer_lag_ms} / {@code durable_lag_ms} (= {@code now - timestamp},
 * with {@code -1} when the timestamp is {@code 0} ("unknown")).
 *
 * <p>The test boots a tiny in-process gRPC server that returns hand-crafted
 * timestamps, then drives the CLI's {@code run("status", ..., "--json")} entry
 * point and asserts on the resulting JSON.
 */
public class IndexingAdminCliStatusLagTest {

    private FakeServer fakeServer;
    private ByteArrayOutputStream stdoutBytes;
    private ByteArrayOutputStream stderrBytes;
    private IndexingAdminCli cli;

    @Before
    public void setUp() {
        stdoutBytes = new ByteArrayOutputStream();
        stderrBytes = new ByteArrayOutputStream();
        cli = new IndexingAdminCli(
                new PrintStream(stdoutBytes, true, StandardCharsets.UTF_8),
                new PrintStream(stderrBytes, true, StandardCharsets.UTF_8));
    }

    @After
    public void tearDown() throws InterruptedException {
        if (fakeServer != null) {
            fakeServer.close();
        }
    }

    private String stdout() {
        return new String(stdoutBytes.toByteArray(), StandardCharsets.UTF_8);
    }

    /**
     * When both timestamps are non-zero (a live IS), the JSON output must
     * carry the raw timestamps AND the computed lag (now - timestamp), and
     * the computed lag must be a non-negative number.
     */
    @Test
    public void statusJsonIncludesRawTimestampsAndComputedLags() throws Exception {
        long tailerTs = System.currentTimeMillis() - 1500L;
        long durableTs = System.currentTimeMillis() - 60_000L;
        StaticIndexStatusImpl impl = new StaticIndexStatusImpl(tailerTs, durableTs);
        fakeServer = new FakeServer(impl);
        fakeServer.start();

        int rc = cli.run(new String[]{
            "status",
            "--server", fakeServer.address(),
            "--tablespace", "ts",
            "--table", "t",
            "--index", "i",
            "--json"
        });
        assertEquals("CLI should succeed; stderr=\n"
                + new String(stderrBytes.toByteArray(), StandardCharsets.UTF_8), 0, rc);
        String out = stdout().trim();
        assertTrue("expected JSON object: " + out, out.startsWith("{"));
        assertTrue("must contain raw tailer_lsn_timestamp: " + out,
                out.contains("\"tailer_lsn_timestamp\":" + tailerTs));
        assertTrue("must contain raw durable_lsn_timestamp: " + out,
                out.contains("\"durable_lsn_timestamp\":" + durableTs));
        assertTrue("must contain tailer_lag_ms key: " + out,
                out.contains("\"tailer_lag_ms\":"));
        assertTrue("must contain durable_lag_ms key: " + out,
                out.contains("\"durable_lag_ms\":"));

        // Sanity-check the computed lag values (not exact because the CLI
        // takes the lag at print time, which is after the test set up the
        // hand-crafted timestamps). Tailer lag must be >= 1500 - epsilon,
        // durable lag must be >= 60000 - epsilon.
        long tailerLag = extractLong(out, "tailer_lag_ms");
        long durableLag = extractLong(out, "durable_lag_ms");
        assertTrue("tailer_lag_ms must be at least the configured stale period: " + tailerLag,
                tailerLag >= 1000L);
        assertTrue("durable_lag_ms must reflect the durable timestamp: " + durableLag,
                durableLag >= 50_000L);
    }

    /**
     * When the IS reports {@code 0} timestamps ("unknown"), the CLI must
     * output {@code -1} for the lag — the documented "no information yet"
     * sentinel that dashboards rely on.
     */
    @Test
    public void statusJsonReportsLagMinusOneWhenTimestampUnknown() throws Exception {
        StaticIndexStatusImpl impl = new StaticIndexStatusImpl(0L, 0L);
        fakeServer = new FakeServer(impl);
        fakeServer.start();

        int rc = cli.run(new String[]{
            "status",
            "--server", fakeServer.address(),
            "--tablespace", "ts",
            "--table", "t",
            "--index", "i",
            "--json"
        });
        assertEquals(0, rc);
        String out = stdout().trim();
        assertTrue("must contain tailer_lag_ms=-1: " + out,
                out.contains("\"tailer_lag_ms\":-1"));
        assertTrue("must contain durable_lag_ms=-1: " + out,
                out.contains("\"durable_lag_ms\":-1"));
    }

    /**
     * Text output: the {@code status} command's plain-text path must include
     * both lag values so an operator running it from a shell sees them
     * without having to add {@code --json}.
     */
    @Test
    public void statusTextOutputIncludesLagValues() throws Exception {
        long tailerTs = System.currentTimeMillis() - 2000L;
        StaticIndexStatusImpl impl = new StaticIndexStatusImpl(tailerTs, 0L);
        fakeServer = new FakeServer(impl);
        fakeServer.start();

        int rc = cli.run(new String[]{
            "status",
            "--server", fakeServer.address(),
            "--tablespace", "ts",
            "--table", "t",
            "--index", "i"
        });
        assertEquals(0, rc);
        String out = stdout();
        assertTrue("text output must include tailer_lag_ms: " + out,
                out.contains("tailer_lag_ms="));
        assertTrue("text output must include durable_lag_ms: " + out,
                out.contains("durable_lag_ms="));
        assertTrue("durable_lag_ms must be -1 when the timestamp is unknown: " + out,
                out.contains("durable_lag_ms=-1"));
    }

    private static long extractLong(String json, String key) {
        int i = json.indexOf("\"" + key + "\":");
        if (i < 0) {
            throw new AssertionError("key not found in JSON: " + key);
        }
        i += key.length() + 3;
        int end = i;
        while (end < json.length()
                && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
            end++;
        }
        return Long.parseLong(json.substring(i, end));
    }

    // -------------------- harness --------------------

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
        private final long tailerTimestamp;
        private final long durableTimestamp;

        StaticIndexStatusImpl(long tailerTimestamp, long durableTimestamp) {
            this.tailerTimestamp = tailerTimestamp;
            this.durableTimestamp = durableTimestamp;
        }

        @Override
        public void getIndexStatus(GetIndexStatusRequest request,
                                    StreamObserver<GetIndexStatusResponse> responseObserver) {
            GetIndexStatusResponse resp = GetIndexStatusResponse.newBuilder()
                    .setVectorCount(0)
                    .setSegmentCount(1)
                    .setTailerLsnLedger(7)
                    .setTailerLsnOffset(99)
                    .setTailerLsnTimestamp(tailerTimestamp)
                    .setDurableLsnLedger(7)
                    .setDurableLsnOffset(50)
                    .setDurableLsnTimestamp(durableTimestamp)
                    .setStatus("static")
                    .build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }
    }
}
