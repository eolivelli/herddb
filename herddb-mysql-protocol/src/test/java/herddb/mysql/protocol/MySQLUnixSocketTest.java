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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests for MySQL protocol server Unix domain socket support.
 */
public class MySQLUnixSocketTest {

    private MySQLServer server;
    private String socketPath;

    private final MySQLCommandHandler handler = new MySQLCommandHandler() {
        @Override
        public boolean authenticate(String username, byte[] scrambledPassword, byte[] challenge, String database) {
            return true;
        }

        @Override
        public CompletableFuture<QueryResult> executeQuery(long connectionId, String sql) {
            if (sql.trim().equalsIgnoreCase("SELECT 1")) {
                ResultSetResponse rs = new ResultSetResponse(
                        Collections.singletonList(
                                new ColumnDef("1", "", "", MySQLColumnType.MYSQL_TYPE_LONGLONG, 1)
                        ),
                        Collections.singletonList(new Object[]{"1"})
                );
                return CompletableFuture.completedFuture(rs);
            }
            return CompletableFuture.completedFuture(
                    new OkResult(0, 0, ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT));
        }

        @Override
        public void useDatabase(long connectionId, String database) {
        }

        @Override
        public void connectionClosed(long connectionId) {
        }
    };

    @Before
    public void setUp() throws Exception {
        Assume.assumeTrue("Epoll not available, skipping Unix socket test", Epoll.isAvailable());
        File tempFile = File.createTempFile("herddb-mysql-test", ".sock");
        tempFile.delete(); // Netty needs the path to not exist
        socketPath = tempFile.getAbsolutePath();
        server = new MySQLServer("localhost", 0, socketPath, handler);
        server.start();
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    public void testUnixSocketAcceptsConnection() throws Exception {
        // Connect via Netty epoll domain socket client and verify we receive
        // the MySQL handshake greeting (starts with protocol version 10)
        EpollEventLoopGroup group = new EpollEventLoopGroup(1);
        try {
            CompletableFuture<ByteBuf> handshakeReceived = new CompletableFuture<>();

            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(EpollDomainSocketChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                    handshakeReceived.complete(msg.retain());
                                }
                            });
                        }
                    });

            Channel client = bootstrap.connect(new DomainSocketAddress(socketPath)).sync().channel();

            ByteBuf response = handshakeReceived.get(5, TimeUnit.SECONDS);
            try {
                // MySQL packet: 3 bytes length + 1 byte sequence + payload
                // Payload starts with protocol version (10)
                assertTrue("Response too short", response.readableBytes() > 4);
                int len = response.readUnsignedMediumLE();
                int seq = response.readUnsignedByte();
                assertEquals("Sequence number should be 0", 0, seq);
                int protocolVersion = response.readUnsignedByte();
                assertEquals("Protocol version should be 10", 10, protocolVersion);
            } finally {
                response.release();
            }

            client.close().sync();
        } finally {
            group.shutdownGracefully().sync();
        }
    }

    @Test
    public void testSocketFileCleanedUpOnClose() throws Exception {
        File socketFile = new File(socketPath);
        assertTrue("Socket file should exist while server is running", socketFile.exists());
        server.close();
        server = null;
        assertFalse("Socket file should be deleted after close", socketFile.exists());
    }
}
