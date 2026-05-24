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
import static org.junit.Assert.assertTrue;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.ServerSideConnection;
import herddb.network.ServerSideConnectionAcceptor;
import herddb.network.netty.NettyChannelAcceptor;
import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import herddb.remote.storage.CachingObjectStorage;
import herddb.remote.storage.LocalObjectStorage;
import herddb.remote.storage.ObjectStorage;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #650: end-to-end test for the {@code prefetchFileRange} RPC.
 *
 * <p>Boots a {@link RemoteFileServiceImpl} over a
 * {@link CachingObjectStorage} wrapping a {@link LocalObjectStorage}, then
 * pre-writes a 12 MiB object directly into the inner local storage (the S3
 * simulation) and issues {@code prefetchFileRangeAsync} for every cache-
 * block-aligned offset.
 *
 * <p>For every prefetched block we verify:
 * <ul>
 *   <li>{@link CachingObjectStorage#isInCache(String)} returns {@code true}
 *       under the {@code path#{blockIndex}} key — the disk-cache entry was
 *       populated by the server's {@link ObjectStorage#prefetchRange};</li>
 *   <li>the {@code rfs_prefetch_requests} counter increments by exactly the
 *       number of prefetch RPCs dispatched;</li>
 *   <li>the prefetch response carries no payload bytes (the future resolves
 *       to {@code Boolean.TRUE}).</li>
 * </ul>
 */
public class RemoteFileServiceImplPrefetchTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final int BLOCK_SIZE = 4 * 1024 * 1024;

    private ExecutorService metadataExecutor;
    private ThreadPoolExecutor readExec;
    private ThreadPoolExecutor writeExec;
    private LocalObjectStorage innerStorage;
    private CachingObjectStorage cachingStorage;
    private NettyChannelAcceptor acceptor;
    private RemoteFileServiceClient client;
    private TestStatsProvider statsProvider;

    @Before
    public void setUp() throws Exception {
        metadataExecutor = Executors.newSingleThreadExecutor();
        readExec = singleThreadExec("rfs-prefetch-read-lane");
        writeExec = singleThreadExec("rfs-prefetch-write-lane");

        Path innerDir = folder.newFolder("inner").toPath();
        innerStorage = new LocalObjectStorage(innerDir, metadataExecutor);
        Path cacheDir = folder.newFolder("cache").toPath();
        cachingStorage = new CachingObjectStorage(innerStorage, cacheDir,
                metadataExecutor, 256L * 1024 * 1024);

        statsProvider = new TestStatsProvider();
        RemoteFileServiceImpl service = new RemoteFileServiceImpl(cachingStorage,
                statsProvider.getStatsLogger(""), readExec, writeExec);

        acceptor = new NettyChannelAcceptor("localhost", 0, false);
        acceptor.setEnableJVMNetwork(false);
        acceptor.setAcceptor(new ServerSideConnectionAcceptor() {
            @Override
            public ServerSideConnection createConnection(Channel netChannel) {
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

        // Cap inflight bytes generously — the test's request sizes are
        // well below 4 MiB but the client validates that the cap is
        // always >= block size.
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_BLOCK_SIZE, BLOCK_SIZE);
        client = new RemoteFileServiceClient(
                List.of("localhost:" + port), cfg);
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (acceptor != null) {
            acceptor.close();
        }
        if (cachingStorage != null) {
            cachingStorage.close();
        }
        if (readExec != null) {
            readExec.shutdownNow();
        }
        if (writeExec != null) {
            writeExec.shutdownNow();
        }
        if (metadataExecutor != null) {
            metadataExecutor.shutdownNow();
        }
    }

    private static String blockKey(String path, long blockIndex) {
        return path + "#" + blockIndex;
    }

    private long prefetchRequestsCounter() {
        return statsProvider.getStatsLogger("").scope("rfs")
                .getCounter("prefetch_requests").get().longValue();
    }

    @Test
    public void prefetchPopulatesEveryBlockOnServerDiskCache() throws Exception {
        // 3-block file: 12 MiB.
        int fileSize = BLOCK_SIZE * 3;
        byte[] payload = new byte[fileSize];
        for (int i = 0; i < fileSize; i++) {
            payload[i] = (byte) (i & 0xFF);
        }
        String path = "ts/uuid/multipart/graph";
        // Pre-write the object straight into the inner storage so the
        // CachingObjectStorage has not seen it yet — fresh cache.
        innerStorage.write(path, payload).get(5, TimeUnit.SECONDS);

        long beforePrefetch = prefetchRequestsCounter();

        // Issue one prefetch per block.
        long blockCount = (fileSize + BLOCK_SIZE - 1L) / BLOCK_SIZE;
        for (long b = 0; b < blockCount; b++) {
            long offset = b * (long) BLOCK_SIZE;
            int length = (int) Math.min((long) BLOCK_SIZE, fileSize - offset);
            Boolean ok = client.prefetchFileRangeAsync(path, offset, length, BLOCK_SIZE)
                    .get(15, TimeUnit.SECONDS);
            assertEquals("prefetch must report TRUE for an existing object", Boolean.TRUE, ok);
        }

        // Every block-cache entry under "path#{N}" must be resident.
        for (long b = 0; b < blockCount; b++) {
            assertTrue("server cache must hold block " + b + " after prefetch",
                    cachingStorage.isInCache(blockKey(path, b)));
        }

        long after = prefetchRequestsCounter();
        assertEquals("rfs.prefetch_requests must increment once per dispatched RPC",
                blockCount, after - beforePrefetch);
    }

    @Test
    public void prefetchOfMissingObjectReportsNotFoundAndDoesNotPopulateCache() throws Exception {
        // Issue #650 review follow-up: NOT_FOUND is wired end-to-end through
        // the prefetch RPC. Per the {@link ObjectStorage#prefetchRange}
        // tri-state contract:
        //   TRUE  = admitted into cache
        //   FALSE = NOT_FOUND (no admit, no cache entry)
        //   exceptional = transient error
        // The file-server emits STATUS_NOT_FOUND on the wire; the client maps
        // it to {@code Boolean.FALSE}. Callers (the IS prewarm path) use this
        // to surface the missing-object case in their per-block accounting
        // ({@code rfs_prefetch_not_found} on the server, the {@code notFound}
        // counter inside {@code prewarmMultipartIndexFile} on the IS).
        String path = "ts/uuid/multipart/missing";
        long before = prefetchRequestsCounter();

        Boolean ok = client.prefetchFileRangeAsync(path, 0L, 1024, BLOCK_SIZE)
                .get(15, TimeUnit.SECONDS);
        assertEquals("prefetch of a missing object must resolve to FALSE "
                + "(NOT_FOUND wired end-to-end)",
                Boolean.FALSE, ok);

        // The cache must NOT hold any block entry for the missing object —
        // a NOT_FOUND inner readRange does not admit a cache entry.
        assertTrue("server cache must not contain a phantom block entry "
                + "for a missing object",
                !cachingStorage.isInCache(blockKey(path, 0L)));
        assertEquals("rfs.prefetch_requests must still increment on a miss",
                before + 1, prefetchRequestsCounter());
    }

    private static int boundPort(NettyChannelAcceptor a) throws Exception {
        java.lang.reflect.Field f = NettyChannelAcceptor.class.getDeclaredField("channel");
        f.setAccessible(true);
        Object ch = f.get(a);
        java.net.SocketAddress addr = ((io.netty.channel.Channel) ch).localAddress();
        return ((java.net.InetSocketAddress) addr).getPort();
    }

    private static ThreadPoolExecutor singleThreadExec(String name) {
        java.util.concurrent.atomic.AtomicInteger ctr =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.ThreadFactory f = r -> {
            Thread t = new Thread(r, name + "-" + ctr.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(), f);
    }
}
