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

package herddb.remote.admin;

import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.netty.NettyConnector;
import herddb.network.netty.NetworkUtils;
import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thin blocking native-Netty client for the {@link RemoteFileServerAdminImpl}
 * admin service. Targets exactly one file-server {@code host:port} selected
 * by the CLI user and speaks the same length-prefixed wire protocol as the
 * data-plane client (issue #425).
 *
 * <pre>{@code
 * try (FileServerAdminClient client = new FileServerAdminClient("localhost:9845", 30)) {
 *     PduCodec.GetServerInfoResponse.Info info = client.getServerInfo();
 *     ...
 * }
 * }</pre>
 *
 * @author enrico.olivelli
 */
public final class FileServerAdminClient implements Closeable {

    private static final int CONNECT_TIMEOUT_MS = 10_000;

    private final String host;
    private final int port;
    private final long timeoutSeconds;
    private final MultithreadEventLoopGroup eventLoopGroup;
    private final ExecutorService callbackExecutor;
    private volatile Channel channel;

    public FileServerAdminClient(String target, long timeoutSeconds) {
        String[] parts = target.split(":", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Bad target (expected host:port): " + target);
        }
        this.host = parts[0];
        this.port = Integer.parseInt(parts[1]);
        this.timeoutSeconds = timeoutSeconds;
        ThreadFactory ioFactory = r -> new FastThreadLocalThread(r,
                "fileserver-admin-io-" + System.identityHashCode(r));
        this.eventLoopGroup = NetworkUtils.isEnableEpoolNative()
                ? new EpollEventLoopGroup(1, ioFactory)
                : new NioEventLoopGroup(1, ioFactory);
        AtomicInteger ctr = new AtomicInteger();
        ThreadFactory cbFactory = r -> {
            Thread t = new FastThreadLocalThread(r, "fileserver-admin-cb-" + ctr.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        this.callbackExecutor = Executors.newCachedThreadPool(cbFactory);
    }

    private Channel channel() throws IOException {
        Channel c = this.channel;
        if (c != null && c.isValid() && !c.isClosed()) {
            return c;
        }
        synchronized (this) {
            c = this.channel;
            if (c != null && c.isValid() && !c.isClosed()) {
                return c;
            }
            // The previous channel (if any) is broken — close it explicitly so
            // its socket/threads aren't leaked across reconnects in long-lived
            // CLI sessions.
            if (c != null) {
                try {
                    c.close();
                } catch (RuntimeException ignored) {
                    // Broad catch: best-effort drain of a half-broken channel.
                }
            }
            ChannelEventListener listener = new ChannelEventListener() {};
            int socketTimeoutSeconds = 0;
            this.channel = NettyConnector.connectUsingNetwork(host, port, false,
                    CONNECT_TIMEOUT_MS, socketTimeoutSeconds,
                    listener, callbackExecutor, eventLoopGroup);
            return this.channel;
        }
    }

    /**
     * Returns a snapshot of server identity, JVM stats, and both cache layers.
     */
    public PduCodec.GetServerInfoResponse.Info getServerInfo() throws IOException {
        Channel ch = channel();
        long requestId = ch.generateRequestId();
        long timeoutMs = TimeUnit.SECONDS.toMillis(timeoutSeconds);
        try {
            Pdu reply = ch.sendMessageWithPduReply(requestId,
                    PduCodec.GetServerInfoRequest.write(requestId), timeoutMs);
            try {
                if (reply.type == Pdu.TYPE_ERROR) {
                    throw new IOException("GetServerInfo failed: "
                            + PduCodec.ErrorResponse.readError(reply));
                }
                return PduCodec.GetServerInfoResponse.read(reply);
            } finally {
                reply.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during GetServerInfo", e);
        } catch (TimeoutException e) {
            throw new IOException("GetServerInfo timed out", e);
        }
    }

    /**
     * Resizes the on-disk cache LRU to {@code newMaxBytes}. Returns the
     * previous and new limits. The change is not persistent.
     */
    public ResizeResult resizeDiskCache(long newMaxBytes) throws IOException {
        Channel ch = channel();
        long requestId = ch.generateRequestId();
        long timeoutMs = TimeUnit.SECONDS.toMillis(timeoutSeconds);
        try {
            Pdu reply = ch.sendMessageWithPduReply(requestId,
                    PduCodec.ResizeDiskCacheRequest.write(requestId, newMaxBytes), timeoutMs);
            try {
                if (reply.type == Pdu.TYPE_ERROR) {
                    throw new IOException("ResizeDiskCache failed: "
                            + PduCodec.ErrorResponse.readError(reply));
                }
                return new ResizeResult(
                        PduCodec.ResizeDiskCacheResponse.readPreviousMaxBytes(reply),
                        PduCodec.ResizeDiskCacheResponse.readNewMaxBytes(reply));
            } finally {
                reply.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during ResizeDiskCache", e);
        } catch (TimeoutException e) {
            throw new IOException("ResizeDiskCache timed out", e);
        }
    }

    @Override
    public void close() {
        Channel c = this.channel;
        if (c != null) {
            try {
                c.close();
            } catch (RuntimeException ignored) {
                // Broad catch: best-effort socket close.
            }
            this.channel = null;
        }
        eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        callbackExecutor.shutdown();
        try {
            callbackExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Outcome of a disk-cache resize. */
    public static final class ResizeResult {
        public final long previousMaxBytes;
        public final long newMaxBytes;

        public ResizeResult(long previousMaxBytes, long newMaxBytes) {
            this.previousMaxBytes = previousMaxBytes;
            this.newMaxBytes = newMaxBytes;
        }
    }
}
