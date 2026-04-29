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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for dynamic server list updates in RemoteFileServiceClient.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServiceClientDynamicTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server1;
    private RemoteFileServer server2;

    @Before
    public void setUp() throws Exception {
        Path dataDir1 = folder.newFolder("data1").toPath();
        Path dataDir2 = folder.newFolder("data2").toPath();
        server1 = new RemoteFileServer(0, dataDir1);
        server1.start();
        server2 = new RemoteFileServer(0, dataDir2);
        server2.start();
    }

    @After
    public void tearDown() throws Exception {
        if (server1 != null) {
            server1.stop();
        }
        if (server2 != null) {
            server2.stop();
        }
    }

    @Test
    public void testUpdateServersAddsNewServer() throws Exception {
        String addr1 = "localhost:" + server1.getPort();
        String addr2 = "localhost:" + server2.getPort();

        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Collections.singletonList(addr1))) {

            // Write a file using server1 only
            client.writeFile("test/file1.dat", "hello".getBytes());
            assertNotNull(client.readFile("test/file1.dat"));

            // Update to include both servers
            client.updateServers(Arrays.asList(addr1, addr2));

            // Verify distribution across both servers
            Set<String> serversUsed = new HashSet<>();
            for (int i = 0; i < 20; i++) {
                serversUsed.add(client.getServerForPath("test/file" + i + ".dat"));
            }
            assertEquals("Files should distribute across both servers", 2, serversUsed.size());
        }
    }

    @Test
    public void testUpdateServersRemovesServer() throws Exception {
        String addr1 = "localhost:" + server1.getPort();
        String addr2 = "localhost:" + server2.getPort();

        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Arrays.asList(addr1, addr2))) {

            // Update to only server2
            client.updateServers(Collections.singletonList(addr2));

            // All paths should now route to server2
            for (int i = 0; i < 10; i++) {
                assertEquals(addr2, client.getServerForPath("test/file" + i + ".dat"));
            }
        }
    }

    @Test
    public void testUpdateServersEmptyListKeepsOld() throws Exception {
        String addr1 = "localhost:" + server1.getPort();

        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Collections.singletonList(addr1))) {

            // Empty list should be ignored
            client.updateServers(Collections.emptyList());

            // Should still work
            client.writeFile("test/file.dat", "hello".getBytes());
            assertNotNull(client.readFile("test/file.dat"));
        }
    }

    /**
     * Reproducer for the indexing service startup crash: when the client starts
     * with an empty server list (ZK discovery pending), readFileAsync() must
     * retry with backoff until servers appear, not crash immediately with
     * "Hash ring is empty".
     */
    /**
     * awaitServersReady returns false on timeout when no servers are ever
     * discovered, and true as soon as updateServers is called with a
     * non-empty list from another thread. Guards the #42 cold-boot fix in
     * IndexingServer.bootstrapRemoteStateForTablespace.
     */
    @Test
    public void testAwaitServersReady() throws Exception {
        String addr1 = "localhost:" + server1.getPort();

        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Collections.emptyList())) {

            assertFalse("fresh client should have no servers", client.hasServers());
            long t0 = System.nanoTime();
            assertFalse("no servers → should time out", client.awaitServersReady(200));
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            assertTrue("timeout must be honored, got " + elapsedMs + "ms", elapsedMs >= 150);

            // Populate from another thread while the main thread is blocked.
            Thread populator = new Thread(() -> {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                client.updateServers(Collections.singletonList(addr1));
            }, "populator");
            populator.start();
            try {
                long t1 = System.nanoTime();
                assertTrue("should wake up when servers appear",
                        client.awaitServersReady(5_000));
                long waitMs = (System.nanoTime() - t1) / 1_000_000L;
                assertTrue("should wake up promptly after updateServers, got " + waitMs + "ms",
                        waitMs < 2_000);
                assertTrue(client.hasServers());
            } finally {
                populator.join(5_000);
            }
        }
    }

    @Test
    public void testReadRetriesUntilServerAppears() throws Exception {
        String addr1 = "localhost:" + server1.getPort();

        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Collections.emptyList())) {

            // Start an async read — hash ring is empty, so this must retry
            CompletableFuture<byte[]> readFuture = client.readFileAsync("test/nonexistent.dat");

            // Simulate ZK discovery arriving after a short delay
            Thread.sleep(1500);
            client.updateServers(Collections.singletonList(addr1));

            // The read should eventually succeed (file doesn't exist → null)
            byte[] result = readFuture.get(30, TimeUnit.SECONDS);
            assertNull("Non-existent file should return null", result);
        }
    }

    /**
     * Verify that listFilesAsync also retries on synchronous failures from
     * an initially empty hash ring.
     */
    @Test
    public void testListRetriesUntilServerAppears() throws Exception {
        String addr1 = "localhost:" + server1.getPort();

        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Collections.emptyList())) {

            // Start an async list — hash ring is empty, so this must retry
            CompletableFuture<?> listFuture = client.listFilesAsync("test/");

            // Simulate ZK discovery arriving after a short delay
            Thread.sleep(1500);
            client.updateServers(Collections.singletonList(addr1));

            // The list should eventually succeed
            listFuture.get(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Reproducer for issue #342: when a file-server pod crashes during Phase C
     * checkpoint, in-flight gRPC write calls to that server must fail promptly
     * after {@link RemoteFileServiceClient#updateServers} removes it — not hang
     * for up to {@code clientTimeoutSeconds} (default 30 min).
     *
     * <p>Uses a TCP "black-hole" server (accepts connections, never sends data)
     * so the gRPC HTTP/2 handshake never completes and the write future stays
     * pending indefinitely. After {@code updateServers} removes the black-hole
     * address, {@code shutdownNow()} must cancel the in-flight call within a
     * short window.
     */
    @Test
    public void testRemovedServerWritesCancelledPromptly() throws Exception {
        // Open a "black-hole" TCP server: accepts connections but never sends
        // any bytes, so the gRPC HTTP/2 handshake never completes and any
        // pending RPC hangs until the channel is forcibly shut down.
        try (ServerSocket blackHole = new ServerSocket(0)) {
            final String blackHoleAddr = "localhost:" + blackHole.getLocalPort();
            final String addr1 = "localhost:" + server1.getPort();

            // Accept connections silently in the background.
            Thread acceptor = new Thread(() -> {
                while (!blackHole.isClosed()) {
                    try {
                        Socket s = blackHole.accept();
                        // Keep the socket open but do not write anything —
                        // this simulates a server that is TCP-reachable but
                        // application-layer unresponsive (e.g. OOM-killed process
                        // where the OS still has the port bound briefly).
                        s.setSoTimeout(0);
                    } catch (IOException e) {
                        break; // socket closed; exit
                    }
                }
            }, "black-hole-acceptor");
            acceptor.setDaemon(true);
            acceptor.start();

            try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                    Arrays.asList(addr1, blackHoleAddr))) {

                // Find a path that the consistent-hash ring routes to the black-hole.
                String pathToBlackHole = null;
                for (int i = 0; i < 500; i++) {
                    String candidate = "issue342/probe" + i + ".dat";
                    if (blackHoleAddr.equals(client.getServerForPath(candidate))) {
                        pathToBlackHole = candidate;
                        break;
                    }
                }
                assertNotNull("Could not find a path routing to the black-hole server", pathToBlackHole);

                // Start an async write to the black-hole — this will be pending
                // forever because the channel can never become READY.
                CompletableFuture<Long> writeFuture = client.writeFileAsync(
                        pathToBlackHole, "checkpoint-page-data".getBytes());

                // Give the gRPC machinery a moment to attempt the connection.
                Thread.sleep(300);
                assertFalse("Write to black-hole should not have completed yet",
                        writeFuture.isDone());

                // Remove the black-hole from the server list (mirrors what ZK
                // does when a pod deregisters). shutdownNow() must cancel the
                // in-flight call so the future completes exceptionally, quickly.
                long t0 = System.currentTimeMillis();
                client.updateServers(Collections.singletonList(addr1));

                // The write must fail within a short window — NOT after 30 min.
                try {
                    writeFuture.get(5, TimeUnit.SECONDS);
                    fail("Expected write to black-hole to fail after updateServers removed it");
                } catch (ExecutionException e) {
                    // Expected: the gRPC channel was shut down by shutdownNow().
                } catch (TimeoutException e) {
                    fail("Write to removed server did not fail within 5 s; "
                            + "shutdownNow() may not have cancelled the in-flight call");
                }

                long elapsed = System.currentTimeMillis() - t0;
                assertTrue("Write should fail promptly after server removal (got " + elapsed + " ms)",
                        elapsed < 5_000);
            }
        }
    }

    /**
     * Verify that writeFileAsync returns a failed CompletableFuture (rather
     * than throwing synchronously) when the hash ring is empty.  Writes are
     * not retried, so the future should complete exceptionally.
     */
    @Test
    public void testWriteAsyncFailsGracefullyWithEmptyHashRing() throws Exception {
        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Collections.emptyList())) {

            CompletableFuture<Long> writeFuture = client.writeFileAsync(
                    "test/file.dat", "hello".getBytes());

            // The future must be returned (no synchronous throw) and must
            // complete exceptionally because there are no servers.
            try {
                writeFuture.get(5, TimeUnit.SECONDS);
                fail("Expected an exception from writeFileAsync with empty hash ring");
            } catch (java.util.concurrent.ExecutionException e) {
                // Verify the root cause is the empty hash ring
                assertTrue("Expected IllegalStateException, got: " + e.getCause(),
                        e.getCause() instanceof IllegalStateException);
                assertTrue(e.getCause().getMessage().contains("Hash ring is empty"));
            }
        }
    }
}
