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
import static org.junit.Assert.assertNotNull;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.netty.NettyConnector;
import herddb.network.netty.NetworkUtils;
import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import herddb.remote.storage.LocalObjectStorage;
import herddb.remote.storage.ObjectStorage;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link RemoteFileServiceImpl} surfaces a server-side
 * ERROR PDU rather than letting the client time out when the read or
 * write executor lane rejects the dispatched task. The failure mode is
 * realistic: under graceful shutdown an executor's
 * {@code shutdownNow()} causes any subsequent {@code execute(Runnable)}
 * to throw {@link RejectedExecutionException}, and the netty thread
 * delivering inbound PDUs must not let that exception propagate without
 * a reply or the client hangs until {@code clientTimeoutSeconds}
 * (default 30 minutes) elapses.
 */
public class RemoteFileServiceImplDispatchFailureTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ObjectStorage storage;
    private ExecutorService callbackExecutor;
    private MultithreadEventLoopGroup eventLoopGroup;
    private herddb.network.netty.NettyChannelAcceptor acceptor;

    @After
    public void tearDown() throws Exception {
        if (acceptor != null) {
            acceptor.close();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
        }
        if (callbackExecutor != null) {
            callbackExecutor.shutdownNow();
        }
        if (storage != null) {
            storage.close();
        }
    }

    @Test
    public void readDispatchFailureReturnsErrorPdu() throws Exception {
        ThreadPoolExecutor readExec = singleThreadExec("read-lane");
        ThreadPoolExecutor writeExec = singleThreadExec("write-lane");
        Channel client = startServerAndConnect(readExec, writeExec);
        // shutdownNow on the read lane forces execute(...) to throw
        // RejectedExecutionException for the next inbound READ_FILE PDU.
        readExec.shutdownNow();
        long requestId = client.generateRequestId();
        Pdu reply = client.sendMessageWithPduReply(requestId,
                PduCodec.ReadFileRequest.write(requestId, "any/file"),
                TimeUnit.SECONDS.toMillis(5));
        try {
            assertEquals("expected ERROR reply when readExecutor is shut down",
                    Pdu.TYPE_ERROR, reply.type);
            assertNotNull(PduCodec.ErrorResponse.readError(reply));
        } finally {
            reply.close();
            client.close();
            writeExec.shutdownNow();
        }
    }

    @Test
    public void writeDispatchFailureReturnsErrorPdu() throws Exception {
        ThreadPoolExecutor readExec = singleThreadExec("read-lane");
        ThreadPoolExecutor writeExec = singleThreadExec("write-lane");
        Channel client = startServerAndConnect(readExec, writeExec);
        writeExec.shutdownNow();
        long requestId = client.generateRequestId();
        Pdu reply = client.sendMessageWithPduReply(requestId,
                PduCodec.WriteFileRequest.write(requestId, "any/file", new byte[]{1, 2, 3}, 0, 3),
                TimeUnit.SECONDS.toMillis(5));
        try {
            assertEquals("expected ERROR reply when writeExecutor is shut down",
                    Pdu.TYPE_ERROR, reply.type);
            assertNotNull(PduCodec.ErrorResponse.readError(reply));
        } finally {
            reply.close();
            client.close();
            readExec.shutdownNow();
        }
    }

    private Channel startServerAndConnect(ExecutorService readExec, ExecutorService writeExec)
            throws Exception {
        ExecutorService metaExec = Executors.newSingleThreadExecutor();
        storage = new LocalObjectStorage(folder.newFolder("rfs").toPath(),
                metaExec, NullStatsLogger.INSTANCE);
        RemoteFileServiceImpl service = new RemoteFileServiceImpl(storage,
                NullStatsLogger.INSTANCE, readExec, writeExec);

        // Wire a minimal acceptor that delegates inbound PDUs straight to the
        // service dispatcher. Bypasses the production RemoteFileServer because
        // we want to inject the lane executors directly.
        acceptor = new herddb.network.netty.NettyChannelAcceptor("localhost", 0, false);
        acceptor.setEnableJVMNetwork(false);
        acceptor.setAcceptor(new herddb.network.ServerSideConnectionAcceptor() {
            @Override
            public herddb.network.ServerSideConnection createConnection(Channel netChannel) {
                netChannel.setMessagesReceiver(new ChannelEventListener() {
                    @Override
                    public void requestReceived(Pdu pdu, Channel ch) {
                        if (!service.handle(pdu, ch)) {
                            pdu.close();
                            ch.sendReplyMessage(pdu.messageId,
                                    PduCodec.ErrorResponse.write(pdu.messageId,
                                            "unsupported message type " + pdu.type));
                        }
                    }
                });
                return () -> 0;
            }
        });
        acceptor.start();
        int port = boundPort(acceptor);

        callbackExecutor = Executors.newCachedThreadPool();
        eventLoopGroup = NetworkUtils.isEnableEpoolNative()
                ? new EpollEventLoopGroup(1, ioFactory())
                : new NioEventLoopGroup(1, ioFactory());
        return NettyConnector.connectUsingNetwork("localhost", port, false,
                10_000, 0, new ChannelEventListener() {}, callbackExecutor, eventLoopGroup);
    }

    private static int boundPort(herddb.network.netty.NettyChannelAcceptor a) throws Exception {
        java.lang.reflect.Field f = herddb.network.netty.NettyChannelAcceptor.class
                .getDeclaredField("channel");
        f.setAccessible(true);
        Object ch = f.get(a);
        java.net.SocketAddress addr = ((io.netty.channel.Channel) ch).localAddress();
        return ((java.net.InetSocketAddress) addr).getPort();
    }

    private static ThreadFactory ioFactory() {
        return r -> new FastThreadLocalThread(r, "dispatch-failure-test-" + System.identityHashCode(r));
    }

    private static ThreadPoolExecutor singleThreadExec(String name) {
        AtomicInteger ctr = new AtomicInteger();
        ThreadFactory f = r -> {
            Thread t = new Thread(r, name + "-" + ctr.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(), f);
    }
}
