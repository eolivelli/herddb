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
import herddb.network.netty.NettyChannelAcceptor;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.util.internal.PlatformDependent;
import java.lang.reflect.Field;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that the auto-sized block-cache budget is derived from Netty's
 * direct-memory ceiling and that the Netty worker / metadata executor pool
 * sizes can be configured independently of {@code ioThreads}.
 *
 * <p>Issue #425 moved the Netty worker group from being a server-owned
 * field to living inside {@link NettyChannelAcceptor#workerGroup}. The
 * test reaches into the acceptor reflectively because the acceptor does
 * not expose a public worker-group accessor, and the file-server's CONFIG
 * keys are the source of truth for the pool sizes.
 */
public class RemoteFileServerThreadPoolConfigTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;

    @After
    public void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void defaultBlockCacheBudgetIsQuarterOfDirectMemoryOrHeap() {
        long actual = RemoteFileServer.defaultBlockCacheMaxBytes();
        long maxDirect = PlatformDependent.maxDirectMemory();
        long expected;
        if (maxDirect > 0) {
            expected = Math.max(1, maxDirect / 4);
        } else {
            long heap = Runtime.getRuntime().maxMemory();
            if (heap == Long.MAX_VALUE) {
                heap = Runtime.getRuntime().totalMemory();
            }
            expected = Math.max(1, heap / 4);
        }
        assertEquals(expected, actual);
    }

    @Test
    public void splitsNettyWorkerAndMetadataExecutorPools() throws Exception {
        Properties props = new Properties();
        props.setProperty(RemoteFileServer.CONFIG_NETTY_WORKER_THREADS, "3");
        props.setProperty(RemoteFileServer.CONFIG_METADATA_EXECUTOR_THREADS, "2");
        // ioThreads passed in is intentionally a different value to prove the
        // new keys override it.
        server = new RemoteFileServer("localhost", 0,
                folder.newFolder("data").toPath(), 7, props);
        server.start();

        MultithreadEventLoopGroup workerGroup = readWorkerGroup(server);
        ThreadPoolExecutor metadataExecutor =
                readField(server, "metadataExecutor", ThreadPoolExecutor.class);

        assertEquals("Netty worker group must honour fileserver.netty.worker.threads",
                3, workerGroup.executorCount());
        assertEquals("metadataExecutor must honour fileserver.metadata.executor.threads",
                2, metadataExecutor.getCorePoolSize());
    }

    @Test
    public void omittingNewKeysFallsBackToIoThreads() throws Exception {
        server = new RemoteFileServer("localhost", 0,
                folder.newFolder("data").toPath(), 5, new Properties());
        server.start();

        MultithreadEventLoopGroup workerGroup = readWorkerGroup(server);
        ThreadPoolExecutor metadataExecutor =
                readField(server, "metadataExecutor", ThreadPoolExecutor.class);

        assertEquals("Netty worker group should default to ioThreads",
                5, workerGroup.executorCount());
        assertEquals("metadataExecutor should default to ioThreads",
                5, metadataExecutor.getCorePoolSize());
    }

    private static MultithreadEventLoopGroup readWorkerGroup(RemoteFileServer s) throws Exception {
        NettyChannelAcceptor acceptor = readField(s, "acceptor", NettyChannelAcceptor.class);
        Field f = NettyChannelAcceptor.class.getDeclaredField("workerGroup");
        f.setAccessible(true);
        return (MultithreadEventLoopGroup) f.get(acceptor);
    }

    private static <T> T readField(Object target, String name, Class<T> type) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        Object value = f.get(target);
        return type.cast(value);
    }
}
