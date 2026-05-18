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
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.proto.Pdu;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Regression test for issue #582: {@link RemoteFileServiceClient} must release
 * the allocated direct {@code ByteBuf buf} when the response-parser lambda
 * throws after the allocation but before returning it to the caller.
 *
 * <p>Covers two code paths that shared the same latent bug:
 * <ul>
 *   <li>{@code doReadFileRangeAsByteBufAsync} — the method observed to leak in
 *       production (bigann-50M gRPC push benchmark, IS pod, issue #581 context).
 *   <li>{@code doReadFileAsByteBufAsync} — identical pattern, fixed proactively.
 * </ul>
 *
 * <h3>How the leak is triggered</h3>
 * The mock server returns a malformed response with {@code found=1} and
 * {@code contentLength=1000} but only 5 bytes of actual payload in the PDU
 * buffer (21 bytes total). When the parse lambda calls
 * {@code PduCodec.ReadFileXxxResponse.readContent(pdu)}, it executes
 * {@code buffer.retainedSlice(16, 1000)} on a buffer whose capacity is only 21
 * bytes. Netty throws {@code IndexOutOfBoundsException} immediately after
 * {@code buf = PooledByteBufAllocator.DEFAULT.directBuffer(1000)} has already
 * been executed. Before the fix, {@code buf} was leaked (refCnt=1, never
 * decremented). After the fix, the {@code finally} block releases it.
 *
 * <h3>Leak detection strategy</h3>
 * The test sums {@link PoolArenaMetric#numActiveAllocations()} across all direct
 * arenas before and after N=10 repeated malformed reads. With the bug the count
 * grows by N; with the fix it stays stable.
 */
public class RemoteFileServiceClientReadFileRangeByteBufLeakTest {

    @BeforeClass
    public static void enableLeakDetection() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    /** Per-test timeout guard — the test should finish in well under 5 s. */
    @Rule
    public Timeout testTimeout = Timeout.seconds(30);

    private herddb.network.netty.NettyChannelAcceptor acceptor;
    private RemoteFileServiceClient client;

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (acceptor != null) {
            acceptor.close();
        }
    }

    // -------------------------------------------------------------------------
    // doReadFileRangeAsByteBufAsync — reported in issue #582
    // -------------------------------------------------------------------------

    /**
     * Verifies that no direct {@code ByteBuf} is leaked when the
     * {@code ReadFileRange} response-parser lambda throws
     * {@link IndexOutOfBoundsException} due to a truncated server response.
     *
     * <p>The malformed response header claims {@code contentLength=1000} but the
     * PDU buffer only has 5 bytes of content, so
     * {@code PduCodec.ReadFileRangeResponse.readContent(pdu)} (specifically
     * {@code retainedSlice(16, 1000)}) throws after {@code buf=directBuffer(1000)}
     * has already been allocated.
     *
     * <p>Repeated 10 times so a per-call leak accumulates to a clearly
     * detectable +10 delta in {@link PoolArenaMetric#numActiveAllocations()}.
     */
    @Test
    public void testReadFileRangeAsByteBufAsync_releasesBufOnParseError() throws Exception {
        startMockServer();

        // Warm up: one request to stabilise any channel-level Netty allocations.
        issueRangeRequest();

        long activeAllocsBefore = totalActiveDirectAllocs();

        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            issueRangeRequest();
        }

        long activeAllocsAfter = totalActiveDirectAllocs();
        assertEquals(
                "direct ByteBuf leaked on ReadFileRange parse error (issue #582): "
                        + iterations + " requests increased active direct allocs from "
                        + activeAllocsBefore + " to " + activeAllocsAfter,
                activeAllocsBefore, activeAllocsAfter);
    }

    // -------------------------------------------------------------------------
    // doReadFileAsByteBufAsync — same latent bug, fixed proactively
    // -------------------------------------------------------------------------

    /**
     * Verifies that no direct {@code ByteBuf} is leaked when the {@code ReadFile}
     * response-parser lambda throws {@link IndexOutOfBoundsException} due to a
     * truncated server response. Same assertion strategy as the
     * {@code ReadFileRange} variant above.
     */
    @Test
    public void testReadFileAsByteBufAsync_releasesBufOnParseError() throws Exception {
        startMockServer();

        // Warm up
        issueFileRequest();

        long activeAllocsBefore = totalActiveDirectAllocs();

        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            issueFileRequest();
        }

        long activeAllocsAfter = totalActiveDirectAllocs();
        assertEquals(
                "direct ByteBuf leaked on ReadFile parse error: "
                        + iterations + " requests increased active direct allocs from "
                        + activeAllocsBefore + " to " + activeAllocsAfter,
                activeAllocsBefore, activeAllocsAfter);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Starts a JVM in-process (no real TCP) mock server that responds to every
     * {@code ReadFile} / {@code ReadFileRange} request with a deliberately
     * malformed PDU, and connects a client with retries disabled (fail fast).
     */
    private void startMockServer() throws Exception {
        acceptor = new herddb.network.netty.NettyChannelAcceptor("localhost", 0, false);
        // JVM-only mode: no real TCP port; PDU delivery is synchronous so the
        // test runs in milliseconds without network latency.
        acceptor.setEnableRealNetwork(false);
        acceptor.setAcceptor(new herddb.network.ServerSideConnectionAcceptor() {
            @Override
            public herddb.network.ServerSideConnection createConnection(Channel netChannel) {
                netChannel.setMessagesReceiver(new ChannelEventListener() {
                    @Override
                    public void requestReceived(Pdu pdu, Channel ch) {
                        try {
                            handleMalformedResponse(pdu, ch);
                        } finally {
                            pdu.close();
                        }
                    }
                });
                return () -> 0L;
            }
        });
        acceptor.start();
        int port = boundPort(acceptor);

        Map<String, Object> config = new HashMap<>();
        // No retries: each malformed response must fail fast so the test
        // doesn't wait for exponential back-off between iterations.
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 0);
        client = new RemoteFileServiceClient(Arrays.asList("localhost:" + port), config);
    }

    /**
     * Sends back a malformed response for a {@code ReadFile} or
     * {@code ReadFileRange} request. Any other request type is silently ignored.
     *
     * <p>The message ID is at PDU offset 3 (after VERSION(1)+FLAGS(1)+TYPE(1)).
     */
    private static void handleMalformedResponse(Pdu pdu, Channel ch) {
        long msgId = pdu.buffer.getLong(3);  // VERSION(1)+FLAGS(1)+TYPE(1) = offset 3
        if (pdu.type == Pdu.TYPE_FS_READ_FILE) {
            ch.sendReplyMessage(msgId, malformedReadFileResponse(msgId));
        } else if (pdu.type == Pdu.TYPE_FS_READ_FILE_RANGE) {
            ch.sendReplyMessage(msgId, malformedReadFileRangeResponse(msgId));
        }
        // Any other request type (e.g., server-info during handshake): ignore.
    }

    /**
     * Builds a malformed {@code ReadFile} response PDU.
     *
     * <p>Wire layout (21 bytes total):
     * VERSION(1)+FLAGS(1)+TYPE(1)+MSGID(8)+found(1)+contentLen(4)+data(5)
     *
     * <p>The parse lambda reads {@code contentLength=1000} and allocates
     * {@code buf=directBuffer(1000)}, then calls
     * {@code PduCodec.ReadFileResponse.readContent(pdu)} which executes
     * {@code retainedSlice(16, 1000)} on a 21-byte buffer →
     * {@code IndexOutOfBoundsException}. The fix releases {@code buf}
     * in the {@code finally} block.
     */
    private static ByteBuf malformedReadFileResponse(long msgId) {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(1 + 1 + 1 + 8 + 1 + 4 + 5);
        buf.writeByte(3);                        // VERSION_3
        buf.writeByte(Pdu.FLAGS_ISRESPONSE);
        buf.writeByte(Pdu.TYPE_FS_READ_FILE);
        buf.writeLong(msgId);
        buf.writeByte(1);                        // found = 1
        buf.writeInt(1000);                      // contentLength = 1000 (lie: only 5 bytes follow)
        buf.writeBytes(new byte[5]);             // truncated content
        return buf;
    }

    /**
     * Builds a malformed {@code ReadFileRange} response PDU.
     * Same wire layout as {@link #malformedReadFileResponse} but with
     * {@code TYPE_FS_READ_FILE_RANGE}.
     */
    private static ByteBuf malformedReadFileRangeResponse(long msgId) {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(1 + 1 + 1 + 8 + 1 + 4 + 5);
        buf.writeByte(3);                              // VERSION_3
        buf.writeByte(Pdu.FLAGS_ISRESPONSE);
        buf.writeByte(Pdu.TYPE_FS_READ_FILE_RANGE);
        buf.writeLong(msgId);
        buf.writeByte(1);                              // found = 1
        buf.writeInt(1000);                            // contentLength = 1000 (lie: only 5 bytes follow)
        buf.writeBytes(new byte[5]);                   // truncated content
        return buf;
    }

    /**
     * Issues one {@code readFileRangeAsByteBufAsync} call against the mock
     * server and waits for it to complete (successfully or exceptionally).
     * The server always sends a malformed response so the future is expected
     * to complete exceptionally.
     */
    private void issueRangeRequest() throws Exception {
        // offset=0, length=100, blockSizeArg=DEFAULT_BLOCK_SIZE → startBlock==endBlock==0
        // so the single-block code path (doReadFileRangeAsByteBufAsync) is taken.
        CompletableFuture<ByteBuf> future = client.readFileRangeAsByteBufAsync(
                "test/leak/data.bin", 0L, 100,
                RemoteFileServiceClient.DEFAULT_BLOCK_SIZE);
        try {
            ByteBuf result = future.get(10, TimeUnit.SECONDS);
            if (result != null) {
                result.release();
            }
        } catch (ExecutionException e) {
            // Expected: malformed response → IndexOutOfBoundsException wrapped here.
        }
    }

    /**
     * Issues one {@code readFileAsByteBufAsync} call and waits for completion.
     */
    private void issueFileRequest() throws Exception {
        CompletableFuture<ByteBuf> future = client.readFileAsByteBufAsync("test/leak/data.bin");
        try {
            ByteBuf result = future.get(10, TimeUnit.SECONDS);
            if (result != null) {
                result.release();
            }
        } catch (ExecutionException e) {
            // Expected: malformed response → IndexOutOfBoundsException wrapped here.
        }
    }

    /** Returns the sum of {@code numActiveAllocations()} across all direct arenas. */
    private static long totalActiveDirectAllocs() {
        return PooledByteBufAllocator.DEFAULT.metric().directArenas().stream()
                .mapToLong(PoolArenaMetric::numActiveAllocations)
                .sum();
    }

    /** Extracts the bound port from a {@link herddb.network.netty.NettyChannelAcceptor}. */
    private static int boundPort(herddb.network.netty.NettyChannelAcceptor a) throws Exception {
        Field f = herddb.network.netty.NettyChannelAcceptor.class.getDeclaredField("channel");
        f.setAccessible(true);
        Object ch = f.get(a);
        if (ch == null) {
            // JVM-only mode: no real TCP port — NettyConnector uses an in-process lookup.
            return 0;
        }
        InetSocketAddress addr = (InetSocketAddress) ((io.netty.channel.Channel) ch).localAddress();
        return addr.getPort();
    }
}
