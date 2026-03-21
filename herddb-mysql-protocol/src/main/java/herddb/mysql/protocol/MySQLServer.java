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

package herddb.mysql.protocol;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MySQL protocol server backed by Netty.
 * Supports both TCP and Unix domain socket listeners.
 */
public class MySQLServer {

    private static final Logger LOG = Logger.getLogger(MySQLServer.class.getName());

    private final String host;
    private final int port;
    private final String socketPath;
    private final MySQLCommandHandler commandHandler;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    private EventLoopGroup unixBossGroup;
    private EventLoopGroup unixWorkerGroup;
    private Channel unixServerChannel;

    public MySQLServer(String host, int port, MySQLCommandHandler commandHandler) {
        this(host, port, null, commandHandler);
    }

    public MySQLServer(String host, int port, String socketPath, MySQLCommandHandler commandHandler) {
        this.host = host;
        this.port = port;
        this.socketPath = socketPath;
        this.commandHandler = commandHandler;
    }

    private ChannelInitializer<Channel> newChannelInitializer() {
        return new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ch.pipeline().addLast(new MySQLPacketDecoder());
                ch.pipeline().addLast(new MySQLConnectionHandler(commandHandler));
            }
        };
    }

    /**
     * Start the server and bind to the configured host/port and optional Unix socket.
     *
     * @throws InterruptedException if interrupted while binding
     */
    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(newChannelInitializer())
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true);

        serverChannel = bootstrap.bind(host, port).sync().channel();
        LOG.log(Level.INFO, "MySQL protocol server started on {0}:{1}",
                new Object[]{host, getPort()});

        if (socketPath != null && !socketPath.isEmpty()) {
            startUnixSocket();
        }
    }

    private void startUnixSocket() throws InterruptedException {
        if (!Epoll.isAvailable()) {
            LOG.log(Level.WARNING,
                    "Unix domain socket requested at {0} but epoll is not available: {1}",
                    new Object[]{socketPath, Epoll.unavailabilityCause()});
            return;
        }

        // Delete stale socket file if it exists
        File socketFile = new File(socketPath);
        if (socketFile.exists()) {
            socketFile.delete();
        }

        unixBossGroup = new EpollEventLoopGroup(1);
        unixWorkerGroup = new EpollEventLoopGroup();

        ServerBootstrap unixBootstrap = new ServerBootstrap();
        unixBootstrap.group(unixBossGroup, unixWorkerGroup)
                .channel(EpollServerDomainSocketChannel.class)
                .childHandler(newChannelInitializer());

        unixServerChannel = unixBootstrap.bind(new DomainSocketAddress(socketPath)).sync().channel();
        LOG.log(Level.INFO, "MySQL protocol server listening on Unix socket {0}", socketPath);
    }

    /**
     * Get the actual port the server is listening on.
     * Useful when binding to port 0 for tests.
     */
    public int getPort() {
        if (serverChannel != null) {
            return ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }
        return port;
    }

    /**
     * Get the Unix domain socket path, or null if not configured.
     */
    public String getSocketPath() {
        return socketPath;
    }

    /**
     * Stop the server and release all resources.
     */
    public void close() {
        if (unixServerChannel != null) {
            unixServerChannel.close().syncUninterruptibly();
        }
        if (unixBossGroup != null) {
            unixBossGroup.shutdownGracefully();
        }
        if (unixWorkerGroup != null) {
            unixWorkerGroup.shutdownGracefully();
        }
        if (socketPath != null) {
            File socketFile = new File(socketPath);
            if (socketFile.exists()) {
                socketFile.delete();
            }
        }
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        LOG.log(Level.INFO, "MySQL protocol server stopped");
    }
}
