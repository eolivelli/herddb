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
import static org.junit.Assert.assertTrue;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.netty.NettyConnector;
import herddb.network.netty.NetworkUtils;
import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Server-side tests for the {@code DELETE_FILES} batch PDU (issue #398).
 * Hits {@link RemoteFileServiceImpl} directly via a raw {@link Channel}
 * connection so the per-path outcome semantics
 * (success / not-found / error) are nailed down independently of the
 * client-side fan-out logic in {@link RemoteFileServiceClient}.
 */
public class RemoteFileServiceImplDeleteFilesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient writeClient;
    private MultithreadEventLoopGroup eventLoopGroup;
    private ExecutorService callbackExecutor;
    private Channel channel;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("rfs").toPath());
        server.start();
        writeClient = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()));
        eventLoopGroup = NetworkUtils.isEnableEpoolNative()
                ? new EpollEventLoopGroup(1)
                : new NioEventLoopGroup(1);
        callbackExecutor = Executors.newCachedThreadPool();
        channel = NettyConnector.connectUsingNetwork(
                "localhost", server.getPort(), false, 10_000, 0,
                new ChannelEventListener() {}, callbackExecutor, eventLoopGroup);
    }

    @After
    public void tearDown() throws Exception {
        if (channel != null) {
            channel.close();
        }
        if (writeClient != null) {
            writeClient.close();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
        }
        if (callbackExecutor != null) {
            callbackExecutor.shutdownNow();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void emptyRequestReturnsEmptyResponse() throws Exception {
        List<PduCodec.DeleteFilesResponse.Outcome> outcomes = sendDeleteFiles(Collections.emptyList());
        assertEquals(0, outcomes.size());
    }

    @Test
    public void perPathOutcomesPreserveInputOrder() throws Exception {
        // Three pre-existing paths, three missing paths, interleaved. The server
        // must return one outcome per input path with deleted=true/false, in the
        // SAME ORDER as the request.
        writeClient.writeFile("server/exists-1", new byte[]{1});
        writeClient.writeFile("server/exists-2", new byte[]{2});
        writeClient.writeFile("server/exists-3", new byte[]{3});
        List<String> input = Arrays.asList(
                "server/exists-1",
                "server/missing-a",
                "server/exists-2",
                "server/missing-b",
                "server/exists-3",
                "server/missing-c"
        );
        List<PduCodec.DeleteFilesResponse.Outcome> outcomes = sendDeleteFiles(input);
        assertEquals(input.size(), outcomes.size());
        for (int i = 0; i < input.size(); i++) {
            PduCodec.DeleteFilesResponse.Outcome outcome = outcomes.get(i);
            assertEquals("outcome path must match input order at index " + i,
                    input.get(i), outcome.path);
            assertTrue("no per-path error expected, got " + outcome.error,
                    outcome.error.isEmpty());
            if (input.get(i).startsWith("server/exists-")) {
                assertTrue("existing path should be reported deleted=true: " + input.get(i),
                        outcome.deleted);
            } else {
                assertFalse("missing path should be reported deleted=false: " + input.get(i),
                        outcome.deleted);
            }
        }
        assertTrue("server/ should be empty after batch delete",
                writeClient.listFiles("server/").isEmpty());
    }

    private List<PduCodec.DeleteFilesResponse.Outcome> sendDeleteFiles(List<String> paths)
            throws Exception {
        long requestId = channel.generateRequestId();
        Pdu reply = channel.sendMessageWithPduReply(requestId,
                PduCodec.DeleteFilesRequest.write(requestId, paths),
                TimeUnit.SECONDS.toMillis(30));
        try {
            if (reply.type == Pdu.TYPE_ERROR) {
                throw new IllegalStateException("server returned ERROR: "
                        + PduCodec.ErrorResponse.readError(reply));
            }
            assertEquals(Pdu.TYPE_FS_DELETE_FILES, reply.type);
            return PduCodec.DeleteFilesResponse.readOutcomes(reply);
        } finally {
            reply.close();
        }
    }
}
