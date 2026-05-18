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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression test for issue #584: RemoteFileServiceClient.getUnchecked() had no timeout,
 * and AbstractChannel.processPendingReplyMessagesDeadline() was never invoked because
 * channelIdle() was never called. Together these meant a silently-dead TCP channel
 * (no FIN/RST from the peer) would hang every in-flight CompletableFuture forever.
 *
 * <p>The fix:
 * <ol>
 *   <li>Schedules a periodic {@code processChannelDeadlines()} task that calls
 *       {@code channelIdle()} on every open channel, activating the deadline scanner.</li>
 *   <li>Adds a safety-net timeout to {@code getUnchecked()} so even if the idle-check
 *       mechanism were bypassed the caller is never blocked indefinitely.</li>
 * </ol>
 *
 * <p>The test uses a "black-hole" server: it accepts the TCP connection but never
 * sends any response, simulating the dead-channel scenario described in the issue.
 * Short {@code CONFIG_CLIENT_TIMEOUT} (2 s) and {@code CONFIG_CLIENT_IDLE_CHECK_INTERVAL_MS}
 * (300 ms) keep the wall-clock time well under the 30-second JUnit test timeout.
 */
public class RemoteFileServiceClientDeadChannelTest {

    /** Accepts TCP connections but never reads from or writes to the socket. */
    private ServerSocket blackHole;
    private final List<Socket> accepted = new ArrayList<>();
    private Thread acceptThread;

    @Before
    public void setUp() throws Exception {
        blackHole = new ServerSocket(0);
        acceptThread = new Thread(() -> {
            while (!blackHole.isClosed()) {
                try {
                    accepted.add(blackHole.accept());
                } catch (IOException e) {
                    // ServerSocket closed — loop exits cleanly
                    break;
                }
            }
        }, "blackhole-acceptor");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    @After
    public void tearDown() throws Exception {
        blackHole.close();
        acceptThread.join(2_000);
        for (Socket s : accepted) {
            try {
                s.close();
            } catch (IOException ignored) {
                // best-effort cleanup
            }
        }
    }

    /**
     * Verifies that a {@code readFile} call to a black-hole server completes
     * exceptionally (rather than blocking forever) within a bounded time window
     * driven by the periodic idle / deadline check introduced in issue #584.
     *
     * <p>Expected timing with the test config (timeout=2 s, idle interval=300 ms):
     * <ul>
     *   <li>t=0: request sent, deadline registered at t+2 s</li>
     *   <li>t≈2.1 s: idle check fires and detects the expired deadline →
     *       callback invoked with IOException → future completed exceptionally</li>
     *   <li>No retries (CONFIG_CLIENT_RETRIES=0): outer future also fails</li>
     *   <li>getUnchecked() throws CompletionException</li>
     * </ul>
     */
    @Test(timeout = 30_000)
    public void testDeadChannelTimesOutViaIdleCheck() {
        Map<String, Object> config = new HashMap<>();
        // Short per-request deadline so the test finishes in a few seconds
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_TIMEOUT, 2L);
        // Short idle-check interval so the deadline scanner fires promptly
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_IDLE_CHECK_INTERVAL_MS, 300L);
        // No retries: we only need to verify the first timeout fires
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 0);

        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + blackHole.getLocalPort()), config)) {
            long beforeMs = System.currentTimeMillis();
            try {
                client.readFile("any/path/file.dat");
                fail("Expected an exception from the dead channel but got none");
            } catch (RuntimeException e) {
                long elapsedMs = System.currentTimeMillis() - beforeMs;
                // Must complete within 10 s (well within the 30 s test timeout).
                // A lower-bound assertion is intentionally omitted: the idle scanner
                // fires at a 300 ms granularity, so the exact elapsed time depends on
                // scheduler load, making any specific lower bound fragile on slow CI.
                assertTrue("readFile did not time out within 10 s — idle-check fix may not be working: "
                        + elapsedMs + " ms", elapsedMs < 10_000L);
                // Root cause must be an IOException (not just a generic failure)
                Throwable root = e.getCause() != null ? e.getCause() : e;
                assertTrue("Expected root cause to be IOException but was: " + root.getClass().getName(),
                        root instanceof IOException);
            }
        }
    }

    /**
     * Verifies that the idle-check timeout still works when retries are enabled:
     * each attempt fires the idle scanner, fails, and the client eventually gives up
     * after all retry attempts are exhausted.
     * With CONFIG_CLIENT_TIMEOUT=2 and CONFIG_CLIENT_RETRIES=1, the entire retry
     * chain (2 attempts × ~2 s each + 1 s back-off) should complete in under 15 s.
     */
    @Test(timeout = 30_000)
    public void testDeadChannelWithRetriesStillTimesOut() {
        Map<String, Object> config = new HashMap<>();
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_TIMEOUT, 2L);
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_IDLE_CHECK_INTERVAL_MS, 300L);
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 1);

        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + blackHole.getLocalPort()), config)) {
            long beforeMs = System.currentTimeMillis();
            try {
                client.readFile("any/path/with-retry.dat");
                fail("Expected an exception from the dead channel but got none");
            } catch (RuntimeException e) {
                long elapsedMs = System.currentTimeMillis() - beforeMs;
                // With 2 attempts (initial + 1 retry) and 1 s back-off, expect ~5 s total
                assertTrue("readFile did not time out within 15 s: " + elapsedMs + " ms",
                        elapsedMs < 15_000L);
                // Root cause must be an IOException
                Throwable root = e.getCause() != null ? e.getCause() : e;
                assertTrue("Expected root cause to be IOException but was: " + root.getClass().getName(),
                        root instanceof IOException);
            }
        }
    }

    /**
     * Exercises the {@code getUnchecked()} safety-net timeout path directly.
     * The idle-check interval is set to 24 hours so the
     * {@code processChannelDeadlines()} heartbeat never fires during this test,
     * forcing the safety-net {@code future.get(safetyNetSeconds, …)} to be the
     * mechanism that terminates the wait.
     *
     * <p>With CONFIG_CLIENT_TIMEOUT=1 and CONFIG_CLIENT_RETRIES=0:
     * safety-net = (0 + 3) × 1 = 3 s.
     * The test verifies that the thrown exception is an {@code IOException} whose
     * message contains the string {@code "safety-net"}, proving the
     * {@code TimeoutException} branch in {@code getUnchecked()} was taken.
     */
    @Test(timeout = 30_000)
    public void testSafetyNetTimeoutFires() {
        Map<String, Object> config = new HashMap<>();
        // clientTimeoutSeconds=1; safety-net = (0+3)*1 = 3s
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_TIMEOUT, 1L);
        // 24-hour idle interval effectively disables the heartbeat within this test,
        // so only the safety-net can terminate the blocking future.get().
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_IDLE_CHECK_INTERVAL_MS, 86_400_000L);
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 0);

        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + blackHole.getLocalPort()), config)) {
            long beforeMs = System.currentTimeMillis();
            try {
                client.readFile("any/path/safety-net.dat");
                fail("Expected a safety-net timeout exception");
            } catch (RuntimeException e) {
                long elapsedMs = System.currentTimeMillis() - beforeMs;
                // Safety-net fires after 3 s; allow generous bounds for CI timing variance
                assertTrue("Safety-net fired too quickly (< 2 s): " + elapsedMs + " ms",
                        elapsedMs >= 2_000L);
                assertTrue("Safety-net did not fire within 15 s: " + elapsedMs + " ms",
                        elapsedMs < 15_000L);
                // Walk the cause chain to find the IOException from the safety-net branch
                Throwable t = e;
                IOException safetyNetIoe = null;
                while (t != null) {
                    if (t instanceof IOException && t.getMessage() != null
                            && t.getMessage().contains("safety-net")) {
                        safetyNetIoe = (IOException) t;
                        break;
                    }
                    t = t.getCause();
                }
                assertTrue("Expected an IOException with 'safety-net' in message, "
                        + "but cause chain was: " + causeChain(e),
                        safetyNetIoe != null);
            }
        }
    }

    /** Returns a one-line summary of the exception cause chain for assertion messages. */
    private static String causeChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        while (t != null) {
            if (sb.length() > 0) {
                sb.append(" → ");
            }
            sb.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage());
            t = t.getCause();
        }
        return sb.toString();
    }
}
