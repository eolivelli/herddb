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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.indexing.proto.GetEngineStatsRequest;
import herddb.indexing.proto.GetEngineStatsResponse;
import herddb.indexing.proto.IndexingServiceGrpc;
import herddb.indexing.proto.MetricEntry;
import herddb.indexing.proto.MetricValue;
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
 * Issue #467 items 3 and 4: unit-level tests for edge-cases in
 * {@link IndexingAdminCli}'s {@code engine-stats} rendering path that cannot
 * be triggered through a live {@link herddb.indexing.EmbeddedIndexingService}
 * because the production server never emits duplicate keys or unset
 * {@code MetricValue} kinds.
 *
 * <p>The harness pattern follows {@link IndexingAdminCliStatusLagTest}: start a
 * minimal in-process gRPC server that returns hand-crafted
 * {@link GetEngineStatsResponse} messages, then drive the CLI's
 * {@link IndexingAdminCli#run(String[])} entry point and assert the resulting
 * stdout/stderr.
 */
public class IndexingAdminCliMetricEntryTest {

    private FakeEngineStatsServer fakeServer;
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

    private String stderr() {
        return new String(stderrBytes.toByteArray(), StandardCharsets.UTF_8);
    }

    /**
     * Issue #467 item 3: {@code IndexingAdminCli.engineStatsToMap} builds a
     * {@link java.util.LinkedHashMap} by calling {@code put(key, value)} for
     * each {@link MetricEntry} in the server's ordered list. When two entries
     * share the same key, {@code LinkedHashMap.put} silently overwrites the
     * first value with the second (last-write-wins). The current server never
     * emits duplicate keys, but the generic {@code repeated MetricEntry} schema
     * technically permits them, and any future consumer that adds a metric must
     * not accidentally produce a key collision that silently truncates the
     * earlier value in dashboards.
     *
     * <p>This test locks the documented last-write-wins contract:
     * <ol>
     *   <li>The fake server returns two {@link MetricEntry} records for the
     *       same key ({@code dupe_metric}) with values 42 and then 99.</li>
     *   <li>The CLI {@code --json} output must contain {@code "dupe_metric":99}
     *       (the second value) and must NOT contain {@code "dupe_metric":42}
     *       (the first).</li>
     * </ol>
     */
    @Test
    public void duplicateKeyInResponseLastValueWins() throws Exception {
        GetEngineStatsResponse response = GetEngineStatsResponse.newBuilder()
                .addMetrics(MetricEntry.newBuilder()
                        .setKey("dupe_metric")
                        .setValue(MetricValue.newBuilder().setInt64Value(42L).build())
                        .build())
                .addMetrics(MetricEntry.newBuilder()
                        .setKey("dupe_metric")
                        .setValue(MetricValue.newBuilder().setInt64Value(99L).build())
                        .build())
                .build();
        fakeServer = new FakeEngineStatsServer(response);
        fakeServer.start();

        int rc = cli.run(new String[]{
            "engine-stats",
            "--server", fakeServer.address(),
            "--json"
        });
        assertEquals("CLI should succeed; stderr=\n" + stderr(), 0, rc);
        String out = stdout().trim();
        assertTrue("output must be a JSON object, got: " + out,
                out.startsWith("{") && out.endsWith("}"));
        // Second (last) value must win.
        assertTrue("last-write-wins: must contain dupe_metric:99, got: " + out,
                out.contains("\"dupe_metric\":99"));
        // First value must have been overwritten and must not appear separately.
        assertFalse("first value must be overwritten: must NOT contain "
                + "dupe_metric:42, got: " + out, out.contains("\"dupe_metric\":42"));
    }

    /**
     * Issue #467 item 4: {@code IndexingAdminCli.engineStatsToMap} has a
     * {@code case KIND_NOT_SET} branch that silently skips entries whose
     * {@link MetricValue} {@code oneof kind} field is unset — forward-compat
     * for a hypothetical future value type (e.g. {@code bytes_value}) that an
     * older CLI does not recognise.
     *
     * <p>This test locks the graceful-skip contract:
     * <ol>
     *   <li>The fake server returns one {@code KIND_NOT_SET} entry (key
     *       {@code future_metric}) plus one valid {@code int64} entry (key
     *       {@code good_metric}, value 42).</li>
     *   <li>The CLI must succeed (exit code 0), must include
     *       {@code "good_metric":42} in the JSON output, and must NOT include
     *       {@code future_metric} at all (skipped, not crashed on).</li>
     * </ol>
     */
    @Test
    public void kindNotSetEntryIsSkippedGracefully() throws Exception {
        GetEngineStatsResponse response = GetEngineStatsResponse.newBuilder()
                // KIND_NOT_SET: a MetricEntry whose MetricValue has no oneof
                // field set — represents an unknown future value kind.
                .addMetrics(MetricEntry.newBuilder()
                        .setKey("future_metric")
                        .setValue(MetricValue.newBuilder().build())  // no kind set
                        .build())
                // A normal int64 entry that must still appear in the output.
                .addMetrics(MetricEntry.newBuilder()
                        .setKey("good_metric")
                        .setValue(MetricValue.newBuilder().setInt64Value(42L).build())
                        .build())
                .build();
        fakeServer = new FakeEngineStatsServer(response);
        fakeServer.start();

        int rc = cli.run(new String[]{
            "engine-stats",
            "--server", fakeServer.address(),
            "--json"
        });
        assertEquals("CLI must not crash on KIND_NOT_SET; stderr=\n" + stderr(), 0, rc);
        String out = stdout().trim();
        assertTrue("output must be a JSON object, got: " + out,
                out.startsWith("{") && out.endsWith("}"));
        // The valid metric must appear.
        assertTrue("valid metric must be present in output, got: " + out,
                out.contains("\"good_metric\":42"));
        // The KIND_NOT_SET entry must have been silently skipped — not
        // represented as null, zero, or any other fallback value.
        assertFalse("KIND_NOT_SET entry must be absent from output, got: " + out,
                out.contains("future_metric"));
    }

    // -----------------------------------------------------------------------
    // Harness
    // -----------------------------------------------------------------------

    private static final class FakeEngineStatsServer implements AutoCloseable {
        private final GetEngineStatsResponse canned;
        private Server server;

        FakeEngineStatsServer(GetEngineStatsResponse canned) {
            this.canned = canned;
        }

        void start() throws IOException {
            server = ServerBuilder.forPort(0)
                    .addService(new EngineStatsImpl(canned))
                    .build()
                    .start();
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

    private static final class EngineStatsImpl
            extends IndexingServiceGrpc.IndexingServiceImplBase {
        private final GetEngineStatsResponse response;

        EngineStatsImpl(GetEngineStatsResponse response) {
            this.response = response;
        }

        @Override
        public void getEngineStats(GetEngineStatsRequest request,
                                   StreamObserver<GetEngineStatsResponse> responseObserver) {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
