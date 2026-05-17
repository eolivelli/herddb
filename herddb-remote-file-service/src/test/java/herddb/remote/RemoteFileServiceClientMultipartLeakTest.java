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
import static org.junit.Assert.assertTrue;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.proto.Pdu;
import herddb.remote.storage.ObjectStorage;
import herddb.remote.storage.ReadResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Regression test for issue #575: inflight-write semaphore permits must be
 * released immediately when the thread calling
 * {@link RemoteFileServiceClient#writeMultipartFile} is interrupted while
 * {@code CompletableFuture.allOf(...).get()} is blocking with one or more
 * block futures still pending.
 *
 * <p>Before the fix, the {@code InterruptedException} catch in
 * {@code writeMultipartFile} did not call {@code releaseBlockPermits}, so
 * permits for pending blocks were held until the individual futures eventually
 * completed (which could take up to {@code clientTimeoutSeconds}, default
 * 30 minutes). This caused the entire inflight-write semaphore to be
 * exhausted after a handful of interrupted checkpoints, permanently blocking
 * further writes.
 *
 * <p>The fix adds {@code releaseBlockPermits(blockReleasers)} in the
 * {@code InterruptedException} catch so permits are returned eagerly, even
 * though the block futures themselves are still pending.
 *
 * <h3>Test design</h3>
 * <ul>
 *   <li>Both blocks are routed to a mock {@link ObjectStorage} whose
 *       {@code writeBlock} implementation blocks on a {@link CountDownLatch}
 *       ({@code blockLatch}), simulating a hung AWS SDK / MinIO connection.
 *   <li>A second latch ({@code bothBlocksStarted}) counts to 2; the mock
 *       {@code writeBlock} decrements it before blocking. The test waits on
 *       this latch before interrupting the writer, guaranteeing that both
 *       block futures are in-flight (and both permits are held) at the moment
 *       the interrupt is delivered.
 *   <li>The writer thread is interrupted, causing {@code allOf.get()} to
 *       throw {@code InterruptedException}. The fix releases both permits
 *       immediately via {@code releaseBlockPermits}.
 *   <li>The key assertion is made AFTER the writer exits but BEFORE
 *       {@code blockLatch.countDown()} is called — so the block futures are
 *       provably still pending when we check
 *       {@link RemoteFileServiceClient#availableInflightWriteBytes()}.
 *   <li>JVM in-process channels (not TCP) are used so that the server's
 *       request handling is synchronous (no network hop), making the test
 *       reliably fast (well under 1 second on any hardware).
 * </ul>
 */
public class RemoteFileServiceClientMultipartLeakTest {

    /**
     * Per-test timeout: guards against unexpected hangs. The test should
     * finish in well under 1 s; the 30 s cap is a safety net only.
     */
    @Rule
    public Timeout testTimeout = Timeout.seconds(30);

    private herddb.network.netty.NettyChannelAcceptor acceptor;
    private ExecutorService serverReadExec;
    private ExecutorService serverWriteExec;

    @After
    public void tearDown() throws Exception {
        if (acceptor != null) {
            acceptor.close();
        }
        if (serverReadExec != null) {
            serverReadExec.shutdownNow();
        }
        if (serverWriteExec != null) {
            serverWriteExec.shutdownNow();
        }
    }

    /**
     * Verifies that after the thread calling {@code writeMultipartFile} is
     * interrupted while {@code allOf.get()} is blocking, all inflight-write
     * semaphore permits are released immediately — even for blocks whose
     * server-side future has not yet completed.
     *
     * <p>Without the fix the permits would be held until the block futures
     * eventually timed out (up to 30 minutes), permanently exhausting the
     * inflight-write semaphore after a few interrupted checkpoints (issue
     * #575).
     */
    @Test
    public void testInflightPermitsReleasedOnInterrupt() throws Exception {
        int blockSize = 1024; // 1 KiB — tiny, just enough for two blocks
        long maxInflight = 2L * blockSize; // exactly fits two blocks

        // Both blocks hang in writeBlock until the test releases this latch.
        // Simulates a hung AWS SDK / MinIO connection so the block futures
        // remain pending throughout the assertion.
        CountDownLatch blockLatch = new CountDownLatch(1);
        // Counts down (from 2) when each block's writeBlock call starts
        // executing in serverWriteExec. Guarantees both permits are held
        // before the writer is interrupted.
        CountDownLatch bothBlocksStarted = new CountDownLatch(2);

        ObjectStorage customStorage = new ObjectStorage() {
            @Override
            public CompletableFuture<Void> writeBlock(String path, long blockIndex, byte[] content) {
                // Signal that this block's server-side processing has started.
                bothBlocksStarted.countDown();
                // Block until the test releases the latch — keeps both futures pending.
                return CompletableFuture.runAsync(() -> {
                    try {
                        blockLatch.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            @Override
            public CompletableFuture<Void> write(String path, byte[] content) {
                throw new UnsupportedOperationException("not used in this test");
            }

            @Override
            public CompletableFuture<ReadResult> read(String path) {
                throw new UnsupportedOperationException("not used in this test");
            }

            @Override
            public CompletableFuture<ReadResult> readRange(
                    String path, long offset, int length, int blockSizeArg) {
                throw new UnsupportedOperationException("not used in this test");
            }

            @Override
            public CompletableFuture<Boolean> deleteLogical(String path) {
                throw new UnsupportedOperationException("not used in this test");
            }

            @Override
            public CompletableFuture<List<String>> listLogical(String prefix) {
                throw new UnsupportedOperationException("not used in this test");
            }

            @Override
            public CompletableFuture<Boolean> delete(String path) {
                throw new UnsupportedOperationException("not used in this test");
            }

            @Override
            public CompletableFuture<List<String>> list(String prefix) {
                throw new UnsupportedOperationException("not used in this test");
            }

            @Override
            public CompletableFuture<Integer> deleteByPrefix(String prefix) {
                throw new UnsupportedOperationException("not used in this test");
            }

            @Override
            public void close() {
                // nothing to close
            }
        };

        // Start a server wired to the mock ObjectStorage using JVM in-process
        // channels (no TCP). JVM channels make the server's request handling
        // synchronous (no network round-trip), so the test runs in milliseconds.
        serverReadExec = Executors.newCachedThreadPool(daemonFactory("rfs-read"));
        serverWriteExec = Executors.newCachedThreadPool(daemonFactory("rfs-write"));
        RemoteFileServiceImpl serviceImpl = new RemoteFileServiceImpl(
                customStorage, NullStatsLogger.INSTANCE, serverReadExec, serverWriteExec);

        acceptor = new herddb.network.netty.NettyChannelAcceptor("localhost", 0, false);
        // JVM-only mode: no real TCP port is bound; PDU delivery is synchronous
        // so there is no network latency between client and server.
        acceptor.setEnableRealNetwork(false);
        acceptor.setAcceptor(new herddb.network.ServerSideConnectionAcceptor() {
            @Override
            public herddb.network.ServerSideConnection createConnection(Channel netChannel) {
                netChannel.setMessagesReceiver(new ChannelEventListener() {
                    @Override
                    public void requestReceived(Pdu pdu, Channel ch) {
                        if (!serviceImpl.handle(pdu, ch)) {
                            pdu.close();
                        }
                    }
                });
                return () -> 0L;
            }
        });
        acceptor.start();
        int port = boundPort(acceptor);

        // Build the client with a constrained inflight-write window.
        Map<String, Object> config = new HashMap<>();
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES, maxInflight);
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_BLOCK_SIZE, blockSize);

        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + port), config)) {

            assertEquals("All permits must be available before any writes",
                    maxInflight, client.availableInflightWriteBytes());

            // Two-block payload: both blocks will hang in the mock ObjectStorage.
            byte[] data = new byte[2 * blockSize];

            // Capture any IOException thrown by writeMultipartFile.
            IOException[] caught = new IOException[1];

            Thread writer = new Thread(() -> {
                try {
                    client.writeMultipartFile(
                            "test/segment/graph", new ByteArrayInputStream(data), blockSize);
                } catch (IOException e) {
                    caught[0] = e;
                }
            }, "test-writer");
            writer.setDaemon(true);
            writer.start();

            // Wait until both blocks' writeBlock calls have started executing
            // on the server. This guarantees both permits are held and both
            // releaseOnce runnables are registered in blockReleasers before
            // the interrupt is delivered.
            assertTrue("Both blocks must start server-side processing within 5 s",
                    bothBlocksStarted.await(5, TimeUnit.SECONDS));

            // Interrupt the writer. allOf.get() (or the next interruptible
            // point) will throw InterruptedException. The fix's catch block
            // calls releaseBlockPermits(blockReleasers), releasing both permits
            // even though the block futures are still pending.
            writer.interrupt();

            // Wait for the writer thread to exit (should be near-instant).
            writer.join(5000);
            assertFalse("Writer thread must exit promptly after interrupt", writer.isAlive());

            // writeMultipartFile must have thrown an IOException wrapping the
            // InterruptedException.
            assertNotNull("writeMultipartFile must throw after interrupt", caught[0]);

            // KEY REGRESSION ASSERTION (issue #575):
            // Both permits must be released immediately by releaseBlockPermits(),
            // even though the block futures are STILL PENDING (blockLatch has not
            // been released yet). Before the fix, the permits were held until the
            // futures eventually timed out (up to clientTimeoutSeconds = 30 min),
            // permanently exhausting the semaphore after a few interrupted checkpoints.
            assertEquals(
                    "All inflight-write permits must be released immediately after interrupt "
                            + "(issue #575: permits for pending blocks were leaked without the fix)",
                    maxInflight,
                    client.availableInflightWriteBytes());

            // Release the latch now so server threads can complete cleanly.
            // whenComplete callbacks for the block futures will fire but are
            // idempotent no-ops (AtomicBoolean already set by releaseBlockPermits).
            blockLatch.countDown();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static java.util.concurrent.ThreadFactory daemonFactory(String prefix) {
        AtomicInteger ctr = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + ctr.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    private static int boundPort(herddb.network.netty.NettyChannelAcceptor a) throws Exception {
        java.lang.reflect.Field f = herddb.network.netty.NettyChannelAcceptor.class
                .getDeclaredField("channel");
        f.setAccessible(true);
        Object ch = f.get(a);
        if (ch == null) {
            // JVM-only mode: no real TCP port was bound. Return 0 so the client
            // address "localhost:0" triggers the JVM-local lookup in NettyConnector.
            return 0;
        }
        InetSocketAddress addr = (InetSocketAddress) ((io.netty.channel.Channel) ch).localAddress();
        return addr.getPort();
    }
}
