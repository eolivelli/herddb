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
import herddb.indexing.proto.DeleteSegmentRequest;
import herddb.indexing.proto.DeleteSegmentResponse;
import herddb.indexing.proto.IndexingServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Issue #617: wire-level tests for the {@code delete-segment} sub-command of
 * {@link IndexingAdminCli}. Each test boots a tiny in-process gRPC server
 * whose {@code deleteSegment} implementation records the inbound request and
 * returns a hand-crafted response, then drives the CLI's {@link
 * IndexingAdminCli#run(String[])} entry point with the relevant flags
 * (purge-storage, force, --yes, --json, stdin confirmation) and asserts on:
 *
 * <ul>
 *   <li>the exact wire request the CLI emitted (so the operator's flags
 *       reach the IS without translation drift);</li>
 *   <li>the process exit code (0 when the server reported
 *       {@code removed=true}, 1 when {@code removed=false} — so wrapper
 *       scripts can distinguish "I removed it" from "it was already
 *       gone");</li>
 *   <li>the stdout payload (text vs. JSON);</li>
 *   <li>the stdin-confirmation behaviour (aborts cleanly on "n",
 *       proceeds on "y", and aborts when stdin is closed).</li>
 * </ul>
 *
 * <p>Engine-level coverage (a real {@link herddb.indexing.vector.PersistentVectorStore}
 * with real segments + shadow notification on segment removal) lives in
 * {@code herddb.indexing.ShadowDeleteSegmentE2ETest}.
 */
public class IndexingAdminCliDeleteSegmentTest {

    private FakeDeleteSegmentServer fakeServer;
    private ByteArrayOutputStream stdoutBytes;
    private ByteArrayOutputStream stderrBytes;
    private IndexingAdminCli cli;
    private InputStream savedStdin;

    @Before
    public void setUp() {
        stdoutBytes = new ByteArrayOutputStream();
        stderrBytes = new ByteArrayOutputStream();
        cli = new IndexingAdminCli(
                new PrintStream(stdoutBytes, true, StandardCharsets.UTF_8),
                new PrintStream(stderrBytes, true, StandardCharsets.UTF_8));
        savedStdin = System.in;
    }

    @After
    public void tearDown() throws InterruptedException {
        System.setIn(savedStdin);
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
     * Happy path: {@code --yes} skips the confirmation prompt, and the
     * server reports a successful removal. The CLI must forward every flag
     * to the server (in particular {@code purge-storage} → {@code true}
     * and {@code force} → {@code false}), exit with code 0, and emit JSON
     * carrying every response field.
     */
    @Test
    public void happyPathYesPurgeJsonReportsRemovedAndExits0() throws Exception {
        fakeServer = new FakeDeleteSegmentServer(
                req -> DeleteSegmentResponse.newBuilder()
                        .setSegment(req.getSegment())
                        .setRemoved(true)
                        .setVectorsLost(12345L)
                        .setGraphFilePresent(false)
                        .setStoragePurged(req.getPurgeStorage())
                        .build());
        fakeServer.start();

        int rc = cli.run(new String[]{
            "delete-segment",
            "--server", fakeServer.address(),
            "--tablespace", "ts",
            "--table", "vectable",
            "--index", "vidx",
            "--segment", "vidx_aaa_seg627",
            "--purge-storage",
            "--yes",
            "--json"
        });
        assertEquals("CLI should succeed; stderr=\n" + stderr(), 0, rc);

        DeleteSegmentRequest captured = fakeServer.lastRequest();
        assertNotNull("server must have received the request", captured);
        assertEquals("ts", captured.getTablespace());
        assertEquals("vectable", captured.getTable());
        assertEquals("vidx", captured.getIndex());
        assertEquals("vidx_aaa_seg627", captured.getSegment());
        assertTrue("CLI must forward --purge-storage", captured.getPurgeStorage());
        assertFalse("CLI must NOT set force when --force is absent", captured.getForce());

        String out = stdout().trim();
        assertTrue("JSON output expected, got:\n" + out, out.startsWith("{") && out.endsWith("}"));
        assertTrue("must include the segment name, got:\n" + out,
                out.contains("\"segment\":\"vidx_aaa_seg627\""));
        assertTrue("must include removed=true, got:\n" + out,
                out.contains("\"removed\":true"));
        assertTrue("must include vectors_lost, got:\n" + out,
                out.contains("\"vectors_lost\":12345"));
        assertTrue("must include storage_purged=true, got:\n" + out,
                out.contains("\"storage_purged\":true"));
    }

    /**
     * Text mode (no {@code --json}) — the CLI must emit a single
     * human-readable line containing the same fields the JSON mode
     * emits, in {@code key=value} form.
     */
    @Test
    public void textModeProducesSingleLineSummary() throws Exception {
        fakeServer = new FakeDeleteSegmentServer(
                req -> DeleteSegmentResponse.newBuilder()
                        .setSegment(req.getSegment())
                        .setRemoved(true)
                        .setVectorsLost(7L)
                        .setGraphFilePresent(true)
                        .setStoragePurged(false)
                        .build());
        fakeServer.start();

        int rc = cli.run(new String[]{
            "delete-segment",
            "--server", fakeServer.address(),
            "--table", "vectable",
            "--index", "vidx",
            "--segment", "vidx_bbb_seg42",
            "--force",
            "--yes"
        });
        assertEquals(0, rc);
        String out = stdout();
        assertTrue("text output must include segment=, got:\n" + out,
                out.contains("segment=vidx_bbb_seg42"));
        assertTrue(out.contains("removed=true"));
        assertTrue(out.contains("vectors_lost=7"));
        assertTrue(out.contains("graph_file_present=true"));
        assertTrue(out.contains("storage_purged=false"));
    }

    /**
     * When {@code --force} is set the CLI must forward {@code force=true}
     * to the server. The server-side refusal is exercised by the engine
     * E2E test; here we only check the wire-protocol forwarding.
     */
    @Test
    public void forceFlagIsForwardedToServer() throws Exception {
        AtomicReference<DeleteSegmentRequest> captured = new AtomicReference<>();
        fakeServer = new FakeDeleteSegmentServer(req -> {
            captured.set(req);
            return DeleteSegmentResponse.newBuilder()
                    .setSegment(req.getSegment())
                    .setRemoved(true)
                    .setVectorsLost(0L)
                    .setGraphFilePresent(true)
                    .setStoragePurged(false)
                    .build();
        });
        fakeServer.start();

        int rc = cli.run(new String[]{
            "delete-segment",
            "--server", fakeServer.address(),
            "--table", "t",
            "--index", "i",
            "--segment", "vidx_seg1",
            "--force",
            "--yes"
        });
        assertEquals(0, rc);
        assertTrue("CLI must forward --force as force=true",
                captured.get().getForce());
    }

    /**
     * Server-side refusal (mapped to {@code FAILED_PRECONDITION}) must
     * surface as a non-zero CLI exit code and an error line on stderr —
     * not a stack trace.
     */
    @Test
    public void serverRefusalIsReportedAsErrorWithNonZeroExit() throws Exception {
        fakeServer = new FakeDeleteSegmentServer(req -> {
            throw Status.FAILED_PRECONDITION
                    .withDescription("refusing to delete segment vidx_seg1: graph file IS reachable")
                    .asRuntimeException();
        });
        fakeServer.start();

        int rc = cli.run(new String[]{
            "delete-segment",
            "--server", fakeServer.address(),
            "--table", "t",
            "--index", "i",
            "--segment", "vidx_seg1",
            "--yes"
        });
        assertTrue("CLI must exit non-zero on server refusal, got rc=" + rc, rc != 0);
        String err = stderr();
        assertTrue("stderr must mention the failure, got:\n" + err,
                err.contains("ERROR") || err.toLowerCase().contains("refus"));
    }

    /**
     * When the server returns {@code removed=false} (no-op race), the CLI
     * exits with code 1 so wrapper scripts can tell "I removed it" from
     * "it was already gone".
     */
    @Test
    public void removedFalseExitsWithCode1() throws Exception {
        fakeServer = new FakeDeleteSegmentServer(
                req -> DeleteSegmentResponse.newBuilder()
                        .setSegment(req.getSegment())
                        .setRemoved(false)
                        .setVectorsLost(0L)
                        .setGraphFilePresent(false)
                        .setStoragePurged(false)
                        .build());
        fakeServer.start();

        int rc = cli.run(new String[]{
            "delete-segment",
            "--server", fakeServer.address(),
            "--table", "t",
            "--index", "i",
            "--segment", "vidx_gone",
            "--yes"
        });
        assertEquals("removed=false must exit 1", 1, rc);
    }

    /**
     * Stdin confirmation: when {@code --yes} is absent the CLI must read
     * one byte from stdin and abort on anything other than {@code y/Y}.
     * The server must never receive a request in the aborted case.
     */
    @Test
    public void stdinPromptAbortsOnAnyNonYesInput() throws Exception {
        AtomicReference<DeleteSegmentRequest> captured = new AtomicReference<>();
        fakeServer = new FakeDeleteSegmentServer(req -> {
            captured.set(req);
            return DeleteSegmentResponse.newBuilder()
                    .setSegment(req.getSegment())
                    .setRemoved(true)
                    .build();
        });
        fakeServer.start();

        System.setIn(new ByteArrayInputStream("n\n".getBytes(StandardCharsets.UTF_8)));
        int rc = cli.run(new String[]{
            "delete-segment",
            "--server", fakeServer.address(),
            "--table", "t",
            "--index", "i",
            "--segment", "vidx_seg1"
        });
        assertEquals("rejected prompt must exit non-zero", 1, rc);
        assertTrue("stderr must mention abort, got:\n" + stderr(),
                stderr().toLowerCase().contains("abort"));
        // Critical safety property: the RPC must NOT have been invoked.
        assertEquals("server must not have received any request when user aborted",
                null, captured.get());
    }

    /**
     * {@code y\n} on stdin (without {@code --yes}) must proceed with the
     * RPC. Verifies the inverse of the abort path above.
     */
    @Test
    public void stdinPromptProceedsOnYInput() throws Exception {
        AtomicReference<DeleteSegmentRequest> captured = new AtomicReference<>();
        fakeServer = new FakeDeleteSegmentServer(req -> {
            captured.set(req);
            return DeleteSegmentResponse.newBuilder()
                    .setSegment(req.getSegment())
                    .setRemoved(true)
                    .setVectorsLost(1L)
                    .build();
        });
        fakeServer.start();

        System.setIn(new ByteArrayInputStream("y\n".getBytes(StandardCharsets.UTF_8)));
        int rc = cli.run(new String[]{
            "delete-segment",
            "--server", fakeServer.address(),
            "--table", "t",
            "--index", "i",
            "--segment", "vidx_seg2"
        });
        assertEquals(0, rc);
        assertNotNull("server must have received the request after y/Y",
                captured.get());
        assertEquals("vidx_seg2", captured.get().getSegment());
    }

    /**
     * pr-reviewer follow-up #1: when the targeted IS is a shadow replica,
     * the server short-circuits the RPC with the exact prefix
     * {@code "not_primary: this indexing-service instance is a shadow
     * replica (shadowOf=...)"}. The CLI must surface this as a non-zero
     * exit code with the rejection text on stderr.
     */
    @Test
    public void shadowConfiguredServerRejectsWithNotPrimaryPrefix() throws Exception {
        // The fake server emulates the IndexingServiceImpl.deleteSegment
        // shadow gate: it never invokes the engine, just returns
        // FAILED_PRECONDITION with the exact "not_primary:" message.
        fakeServer = new FakeDeleteSegmentServer(req -> {
            throw Status.FAILED_PRECONDITION
                    .withDescription("not_primary: this indexing-service instance is a"
                            + " shadow replica (shadowOf=0); target the primary instead")
                    .asRuntimeException();
        });
        fakeServer.start();

        int rc = cli.run(new String[]{
            "delete-segment",
            "--server", fakeServer.address(),
            "--table", "t",
            "--index", "i",
            "--segment", "vidx_seg1",
            "--yes"
        });
        assertTrue("CLI must exit non-zero on shadow rejection, got rc=" + rc, rc != 0);
        String err = stderr();
        assertTrue("stderr must carry the not_primary: prefix, got:\n" + err,
                err.contains("not_primary:"));
        assertTrue("stderr must mention shadowOf=, got:\n" + err,
                err.contains("shadowOf="));
    }

    /**
     * pr-reviewer follow-up #3 (text output): when the server returns
     * {@code vectors_lost=-1} (the engine's race-sentinel for "the segment
     * disappeared between the snapshot and the drop call — count is
     * unknown"), the CLI must NOT print the raw {@code -1} number. Operators
     * read the field as a count, so {@code -1} would be confusing.
     * Instead emit {@code vectors_lost=unknown (race)}.
     */
    @Test
    public void textOutputRendersNegativeVectorsLostAsUnknownRace() throws Exception {
        fakeServer = new FakeDeleteSegmentServer(
                req -> DeleteSegmentResponse.newBuilder()
                        .setSegment(req.getSegment())
                        .setRemoved(false)
                        .setVectorsLost(-1L)
                        .setGraphFilePresent(false)
                        .setStoragePurged(false)
                        .build());
        fakeServer.start();

        int rc = cli.run(new String[]{
            "delete-segment",
            "--server", fakeServer.address(),
            "--table", "vectable",
            "--index", "vidx",
            "--segment", "vidx_race_seg",
            "--yes"
        });
        // removed=false → rc=1 (existing contract from removedFalseExitsWithCode1).
        assertEquals("removed=false must still map to rc=1", 1, rc);

        String out = stdout();
        assertTrue("text output must render -1 as 'unknown (race)', got:\n" + out,
                out.contains("vectors_lost=unknown (race)"));
        // And it must NOT include the raw -1, which would be visually
        // misleading next to the other numeric fields.
        assertFalse("text output must not include the raw -1 sentinel, got:\n" + out,
                out.contains("vectors_lost=-1"));
        // The other fields are still rendered.
        assertTrue(out.contains("segment=vidx_race_seg"));
        assertTrue(out.contains("removed=false"));
    }

    /**
     * pr-reviewer follow-up #3 (JSON output): same input as above (server
     * returns {@code vectors_lost=-1}). The JSON form keeps the
     * {@code vectors_lost} key in the object (so the schema stays stable)
     * but emits the string {@code "unknown"} instead of the raw number.
     * Wrapper scripts that consume the JSON can distinguish a real count
     * from "we don't know" by type-checking the field.
     */
    @Test
    public void jsonOutputRendersNegativeVectorsLostAsUnknownString() throws Exception {
        fakeServer = new FakeDeleteSegmentServer(
                req -> DeleteSegmentResponse.newBuilder()
                        .setSegment(req.getSegment())
                        .setRemoved(false)
                        .setVectorsLost(-1L)
                        .setGraphFilePresent(false)
                        .setStoragePurged(false)
                        .build());
        fakeServer.start();

        int rc = cli.run(new String[]{
            "delete-segment",
            "--server", fakeServer.address(),
            "--table", "vectable",
            "--index", "vidx",
            "--segment", "vidx_race_seg_json",
            "--yes",
            "--json"
        });
        assertEquals("removed=false must still map to rc=1", 1, rc);

        String out = stdout().trim();
        assertTrue("expected JSON object, got:\n" + out,
                out.startsWith("{") && out.endsWith("}"));
        // The vectors_lost key MUST still be present (stable schema), but as
        // a JSON string, not a number. So the substring is the
        // quoted-key-and-value form.
        assertTrue("JSON must carry vectors_lost as the string \"unknown\", got:\n" + out,
                out.contains("\"vectors_lost\":\"unknown\""));
        // And it must NOT carry the raw -1 sentinel.
        assertFalse("JSON must not carry the raw -1 vectors_lost value, got:\n" + out,
                out.contains("\"vectors_lost\":-1"));
        // Other fields still render correctly.
        assertTrue(out.contains("\"segment\":\"vidx_race_seg_json\""));
        assertTrue(out.contains("\"removed\":false"));
    }

    /**
     * Missing {@code --segment} flag must exit with usage code 2, not
     * with a stack trace.
     */
    @Test
    public void missingSegmentFlagExits2() {
        int rc = cli.run(new String[]{
            "delete-segment",
            "--server", "localhost:1",
            "--table", "t",
            "--index", "i"
            // intentionally missing --segment
        });
        assertEquals(2, rc);
        String err = stderr();
        assertTrue("stderr must mention the missing option, got:\n" + err,
                err.toLowerCase().contains("segment"));
    }

    // -------------------- harness --------------------

    @FunctionalInterface
    private interface DeleteSegmentHandler {
        DeleteSegmentResponse handle(DeleteSegmentRequest request);
    }

    private static final class FakeDeleteSegmentServer implements AutoCloseable {
        private final DeleteSegmentImpl impl;
        private Server server;

        FakeDeleteSegmentServer(DeleteSegmentHandler handler) {
            this.impl = new DeleteSegmentImpl(handler);
        }

        void start() throws IOException {
            server = ServerBuilder.forPort(0).addService(impl).build().start();
            assertNotNull(server);
        }

        String address() {
            return "localhost:" + server.getPort();
        }

        DeleteSegmentRequest lastRequest() {
            return impl.lastRequest.get();
        }

        @Override
        public void close() throws InterruptedException {
            if (server != null) {
                server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    private static final class DeleteSegmentImpl
            extends IndexingServiceGrpc.IndexingServiceImplBase {
        private final DeleteSegmentHandler handler;
        private final AtomicReference<DeleteSegmentRequest> lastRequest = new AtomicReference<>();

        DeleteSegmentImpl(DeleteSegmentHandler handler) {
            this.handler = handler;
        }

        @Override
        public void deleteSegment(DeleteSegmentRequest request,
                                  StreamObserver<DeleteSegmentResponse> responseObserver) {
            lastRequest.set(request);
            try {
                DeleteSegmentResponse resp = handler.handle(request);
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
            } catch (io.grpc.StatusRuntimeException e) {
                responseObserver.onError(e);
            }
        }
    }
}
