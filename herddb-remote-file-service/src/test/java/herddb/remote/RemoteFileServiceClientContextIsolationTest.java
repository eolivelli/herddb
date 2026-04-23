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

package herddb.remote;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import io.grpc.Context;
import io.grpc.StatusRuntimeException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression for issue #242: the outgoing RPCs of {@link RemoteFileServiceClient}
 * must not inherit the caller's gRPC {@link Context} deadline.
 *
 * <p>Previously, an incoming {@code search} RPC on the Indexing Service installed
 * a thread-local {@code Context} whose deadline (typically 600 s) propagated into
 * every downstream {@code readFileRange} call issued on the same thread. gRPC
 * resolves the effective deadline as the minimum of the stub deadline set via
 * {@code .withDeadlineAfter(...)} and the propagated context deadline, so a
 * 1800 s per-call budget was silently clamped to whatever remained on the
 * search's context — causing {@code DEADLINE_EXCEEDED: context timed out}
 * failures on large (≥300 MB) segment loads.
 *
 * <p>The fix wraps each stub invocation in {@code Context.ROOT.call(...)} so the
 * effective deadline is strictly the client's own configuration. This test
 * installs an already-expired context on the caller and asserts that the call
 * still succeeds. Retries are disabled ({@code maxRetries = 0}) so the bug
 * surfaces on the first attempt and is not masked by a retry scheduled on a
 * fresh thread without the caller's context.
 */
public class RemoteFileServiceClientContextIsolationTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;
    private ScheduledExecutorService contextDeadlineScheduler;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("data").toPath());
        server.start();
        Map<String, Object> config = new HashMap<>();
        // Disable retries so a first-attempt DEADLINE_EXCEEDED is not masked by
        // a retry scheduled on a fresh thread (where the caller's Context
        // thread-local is no longer installed).
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 0);
        client = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()), config);
        contextDeadlineScheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
        if (contextDeadlineScheduler != null) {
            contextDeadlineScheduler.shutdownNow();
        }
    }

    @Test
    public void readFileDoesNotInheritCallerContextDeadline() throws Exception {
        byte[] content = "context isolation readFile".getBytes(StandardCharsets.UTF_8);
        client.writeFile("ts1/ctx/readfile.page", content);

        Context.CancellableContext expired = Context.current()
                .withDeadlineAfter(1, TimeUnit.MILLISECONDS, contextDeadlineScheduler);
        // Wait well past the deadline so gRPC cannot possibly still treat the
        // context as live when the call is initiated.
        Thread.sleep(50);

        Context previous = expired.attach();
        try {
            byte[] read = client.readFile("ts1/ctx/readfile.page");
            assertNotNull("readFile should not inherit caller context deadline", read);
            assertArrayEquals(content, read);
        } finally {
            expired.detach(previous);
            expired.cancel(null);
        }
    }

    @Test
    public void readFileRangeDoesNotInheritCallerContextDeadline() throws Exception {
        // readFileRange is the path that actually fails in production (issue #242).
        // Use a multipart file so the block-routed readFileRange code path is
        // exercised end-to-end.
        byte[] content = "context isolation readFileRange payload".getBytes(StandardCharsets.UTF_8);
        int blockSize = 4 * 1024 * 1024;
        client.writeFileBlock("ts1/ctx/range.page", 0, content);

        Context.CancellableContext expired = Context.current()
                .withDeadlineAfter(1, TimeUnit.MILLISECONDS, contextDeadlineScheduler);
        Thread.sleep(50);

        Context previous = expired.attach();
        try {
            byte[] read = client.readFileRange("ts1/ctx/range.page", 0, content.length, blockSize);
            assertNotNull("readFileRange should not inherit caller context deadline", read);
            assertArrayEquals(content, read);
        } catch (StatusRuntimeException e) {
            // Without the fix, this fails with DEADLINE_EXCEEDED because the
            // stub's .withDeadlineAfter(...) is clamped to the (expired)
            // context deadline. Fail with a clear message for future readers.
            fail("readFileRange inherited caller context deadline (issue #242): " + e);
        } finally {
            expired.detach(previous);
            expired.cancel(null);
        }
    }
}
