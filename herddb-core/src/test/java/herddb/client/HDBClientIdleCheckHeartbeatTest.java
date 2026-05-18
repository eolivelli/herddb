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
package herddb.client;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.model.TableSpace;
import herddb.network.Channel;
import herddb.proto.PduCodec;
import herddb.server.Server;
import herddb.server.StaticClientSideMetadataProvider;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #586: verifies that {@link HDBClient} periodically calls
 * {@link Channel#channelIdle()} on every open channel so that expired per-request
 * deadlines registered by {@code sendRequestWithAsyncReply} are detected and their
 * callbacks are completed with {@link IOException} rather than hanging forever.
 *
 * <p>Strategy: connect to a real HerdDB server (so SASL auth succeeds), then inject
 * a "phantom" pending callback directly onto the underlying channel via
 * {@link Channel#sendRequestWithAsyncReply} with a fake PDU that the server will
 * silently discard (a {@code FLAGS_ISRESPONSE} AckResponse frame whose message-id
 * the server has no pending callback for).  The client's callback is left pending
 * with a 2-second deadline.  With {@code PROPERTY_IDLE_CHECK_INTERVAL_MS=300 ms},
 * the periodic heartbeat detects the expired deadline in ≈ 2.3 s and fires the
 * callback with {@link IOException}.
 */
public class HDBClientIdleCheckHeartbeatTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Verifies that the periodic idle-check heartbeat detects and fails an expired
     * async-reply callback on an otherwise-healthy channel.
     */
    @Test(timeout = 30_000)
    public void testExpiredCallbackFiredByHeartbeat() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();

            // HDBClient with a short heartbeat interval and per-request timeout.
            ClientConfiguration config = new ClientConfiguration(folder.newFolder().toPath());
            config.set(ClientConfiguration.PROPERTY_IDLE_CHECK_INTERVAL_MS, 300L);
            // PROPERTY_TIMEOUT is used as the per-request deadline passed to
            // sendRequestWithAsyncReply; 2 000 ms keeps the test fast.
            config.set(ClientConfiguration.PROPERTY_TIMEOUT, 2_000);

            try (HDBClient client = new HDBClient(config);
                 HDBConnection connection = client.openConnection()) {

                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                assertTrue(connection.waitForTableSpace(TableSpace.DEFAULT, 10_000));

                // Obtain and open the underlying channel.
                ClientSideConnectionPeer peer = connection.getRouteToTableSpace(TableSpace.DEFAULT);
                Channel channel = peer.ensureOpen();
                assertNotNull("channel must be open after waitForTableSpace", channel);

                AtomicReference<Throwable> callbackError = new AtomicReference<>();
                CountDownLatch latch = new CountDownLatch(1);

                // Craft a PDU that the SERVER will silently discard:
                // AckResponse has FLAGS_ISRESPONSE set (not FLAGS_ISREQUEST), so
                // AbstractChannel.processPduResponse() on the server side will look up
                // a callback for this message-id, find none, and return without closing
                // the channel.  The client-side sendRequestWithAsyncReply call registers
                // the callback with key fakeRequestId, which is what the heartbeat will scan.
                long fakeRequestId = 99_999_888L;
                ByteBuf fakePdu = PduCodec.AckResponse.write(fakeRequestId);
                long deadlineMs = 2_000L; // same as PROPERTY_TIMEOUT above

                channel.sendRequestWithAsyncReply(fakeRequestId, fakePdu, deadlineMs, (pdu, error) -> {
                    if (pdu != null) {
                        pdu.close();
                    }
                    callbackError.set(error);
                    latch.countDown();
                });

                // Wait for the heartbeat to detect the expired deadline.
                // With interval=300ms and deadline=2000ms, the heartbeat fires at
                // ~2100ms → callback should complete well within 8 s.
                boolean fired = latch.await(8, TimeUnit.SECONDS);
                assertTrue("idle-check heartbeat must fire the expired callback within 8 s "
                        + "(PROPERTY_IDLE_CHECK_INTERVAL_MS=300 ms, deadline=2 s)", fired);

                Throwable err = callbackError.get();
                assertNotNull("callback must have been invoked with an error", err);
                assertTrue("error must be an IOException (from processPendingReplyMessagesDeadline), "
                        + "got: " + err.getClass().getName() + ": " + err.getMessage(),
                        err instanceof IOException);
            }
        }
    }
}
