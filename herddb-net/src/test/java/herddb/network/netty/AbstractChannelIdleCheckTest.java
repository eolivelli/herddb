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
package herddb.network.netty;

import static herddb.network.netty.Utils.buildAckRequest;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.ServerSideConnection;
import herddb.proto.Pdu;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/**
 * Regression test for issue #586: proves that {@link AbstractChannel#channelIdle()} scans
 * {@code pendingReplyMessagesDeadline} and fires expired async-reply callbacks with
 * {@link IOException}.
 *
 * <p>A "no-reply" server accepts TCP connections and reads incoming PDUs, but
 * never sends any response.  The client registers an async-reply callback with a
 * short deadline via {@link Channel#sendRequestWithAsyncReply}; after the deadline
 * expires the test manually calls {@link Channel#channelIdle()}, which must invoke
 * {@code processPendingReplyMessagesDeadline()} and complete the callback exceptionally.
 *
 * <p>This is the foundational unit test for the {@code channelIdle()} mechanism that
 * both {@link herddb.remote.RemoteFileServiceClient} (issue #584) and
 * {@link herddb.client.HDBClient} (issue #586) rely on.
 */
public class AbstractChannelIdleCheckTest {

    @Test(timeout = 15_000)
    public void testChannelIdleFiresExpiredCallback() throws Exception {
        // Server that accepts connections but never sends replies.
        try (NettyChannelAcceptor acceptor = new NettyChannelAcceptor(
                "localhost", NetworkUtils.assignFirstFreePort(), true)) {
            acceptor.setAcceptor(channel -> {
                channel.setMessagesReceiver(new ChannelEventListener() {
                    @Override
                    public void requestReceived(Pdu message, Channel channel) {
                        // Discard the PDU — no reply sent.  Channel stays open.
                        message.close();
                    }

                    @Override
                    public void channelClosed(Channel channel) {
                        // nothing to do
                    }
                });
                return (ServerSideConnection) () -> new Random().nextLong();
            });
            acceptor.start();

            ExecutorService executor = Executors.newCachedThreadPool();
            try (Channel client = NettyConnector.connect(
                    acceptor.getHost(), acceptor.getPort(), true, 0, 0,
                    new ChannelEventListener() {
                        @Override
                        public void channelClosed(Channel channel) {
                            // nothing to do
                        }
                    }, executor, null)) {

                AtomicReference<Throwable> callbackError = new AtomicReference<>();
                CountDownLatch latch = new CountDownLatch(1);

                long requestId = 42L;
                long timeoutMs = 100L; // very short deadline
                ByteBuf request = buildAckRequest((int) requestId);
                client.sendRequestWithAsyncReply(requestId, request, timeoutMs, (pdu, error) -> {
                    callbackError.set(error);
                    latch.countDown();
                });

                // Let the deadline expire before triggering the idle check.
                Thread.sleep(200);

                // Manually trigger the idle check — this is what the HDBClient
                // heartbeat scheduler calls periodically (issue #586).
                client.channelIdle();

                assertTrue("expired async-reply callback must fire within 1 s of channelIdle()",
                        latch.await(1, TimeUnit.SECONDS));
                Throwable err = callbackError.get();
                assertNotNull("callback must have been called with an error", err);
                assertTrue("root cause must be an IOException, got: " + err.getClass().getName(),
                        err instanceof IOException);
            } finally {
                executor.shutdown();
            }
        }
    }

    /**
     * Verifies that a callback whose deadline has NOT yet expired is NOT fired by
     * {@code channelIdle()}.
     */
    @Test(timeout = 15_000)
    public void testChannelIdleDoesNotFireNonExpiredCallback() throws Exception {
        try (NettyChannelAcceptor acceptor = new NettyChannelAcceptor(
                "localhost", NetworkUtils.assignFirstFreePort(), true)) {
            acceptor.setAcceptor(channel -> {
                channel.setMessagesReceiver(new ChannelEventListener() {
                    @Override
                    public void requestReceived(Pdu message, Channel channel) {
                        message.close(); // no reply
                    }

                    @Override
                    public void channelClosed(Channel channel) {
                    }
                });
                return (ServerSideConnection) () -> new Random().nextLong();
            });
            acceptor.start();

            ExecutorService executor = Executors.newCachedThreadPool();
            try (Channel client = NettyConnector.connect(
                    acceptor.getHost(), acceptor.getPort(), true, 0, 0,
                    new ChannelEventListener() {
                        @Override
                        public void channelClosed(Channel channel) {
                        }
                    }, executor, null)) {

                CountDownLatch latch = new CountDownLatch(1);

                long requestId = 99L;
                long timeoutMs = 60_000L; // far-future deadline — must NOT fire
                ByteBuf request = buildAckRequest((int) requestId);
                client.sendRequestWithAsyncReply(requestId, request, timeoutMs, (pdu, error) -> {
                    latch.countDown();
                });

                // Trigger the idle check immediately — deadline has not expired yet.
                client.channelIdle();

                // The callback must NOT have been invoked.
                boolean firedPrematurely = latch.await(300, TimeUnit.MILLISECONDS);
                assertTrue("channelIdle() must NOT fire callbacks whose deadline has not expired",
                        !firedPrematurely);
            } finally {
                executor.shutdown();
            }
        }
    }
}
