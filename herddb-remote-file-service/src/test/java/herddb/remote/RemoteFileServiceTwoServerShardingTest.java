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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
import java.nio.file.Path;
import java.util.Arrays;
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
 * Issue #650: verifies block-level sharding when TWO
 * {@link RemoteFileServiceImpl} replicas back the same logical object
 * (shared S3 simulation).
 *
 * <p>Each server wraps its own {@link CachingObjectStorage} but both inner
 * {@link LocalObjectStorage} instances point at the SAME on-disk directory
 * — that directory is the "S3 bucket" both servers see. A single
 * {@link RemoteFileServiceClient} is wired against both servers; the client's
 * {@link ConsistentHashRouter} maps each {@code (path, blockIndex)} to one of
 * the two replicas.
 *
 * <p>After pre-writing a multi-block object directly into the shared
 * directory and issuing a {@code readFileRange} for every cache-block-aligned
 * offset, we assert:
 * <ul>
 *   <li>each request landed on the server predicted by the
 *       {@link ConsistentHashRouter};</li>
 *   <li>the per-server {@code rfs_readrange_requests} counters sum to the
 *       total number of blocks served;</li>
 *   <li>each server's {@link CachingObjectStorage} holds only its predicted
 *       partition of blocks — never any block routed to the peer.</li>
 * </ul>
 */
public class RemoteFileServiceTwoServerShardingTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final int BLOCK_SIZE = 4 * 1024;

    /** Per-server state. */
    private static final class ServerHandle implements AutoCloseable {
        final String address;
        final NettyChannelAcceptor acceptor;
        final CachingObjectStorage caching;
        final TestStatsProvider stats;
        final ThreadPoolExecutor readExec;
        final ThreadPoolExecutor writeExec;
        final ExecutorService meta;

        ServerHandle(String address, NettyChannelAcceptor acceptor,
                     CachingObjectStorage caching, TestStatsProvider stats,
                     ThreadPoolExecutor readExec, ThreadPoolExecutor writeExec,
                     ExecutorService meta) {
            this.address = address;
            this.acceptor = acceptor;
            this.caching = caching;
            this.stats = stats;
            this.readExec = readExec;
            this.writeExec = writeExec;
            this.meta = meta;
        }

        long counter(String name) {
            return stats.getStatsLogger("").scope("rfs")
                    .getCounter(name).get().longValue();
        }

        @Override
        public void close() throws Exception {
            try {
                acceptor.close();
            } finally {
                try {
                    caching.close();
                } finally {
                    readExec.shutdownNow();
                    writeExec.shutdownNow();
                    meta.shutdownNow();
                }
            }
        }
    }

    private Path sharedBucketDir;
    private LocalObjectStorage sharedInnerForPreWrite;
    private ExecutorService preWriteMeta;
    private ServerHandle server1;
    private ServerHandle server2;
    private RemoteFileServiceClient client;

    @Before
    public void setUp() throws Exception {
        // The "S3 bucket": one on-disk directory both servers see.
        sharedBucketDir = folder.newFolder("shared-bucket").toPath();
        preWriteMeta = Executors.newSingleThreadExecutor();
        sharedInnerForPreWrite = new LocalObjectStorage(sharedBucketDir, preWriteMeta);

        server1 = startServer("s1", sharedBucketDir);
        server2 = startServer("s2", sharedBucketDir);

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_BLOCK_SIZE, BLOCK_SIZE);
        client = new RemoteFileServiceClient(
                List.of(server1.address, server2.address), cfg);
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server1 != null) {
            server1.close();
        }
        if (server2 != null) {
            server2.close();
        }
        if (sharedInnerForPreWrite != null) {
            sharedInnerForPreWrite.close();
        }
        if (preWriteMeta != null) {
            preWriteMeta.shutdownNow();
        }
    }

    @Test
    public void readFileRangeRoutesEachBlockToPredictedServer() throws Exception {
        // Use 32 blocks so the consistent-hash distribution is overwhelmingly
        // likely to spread at least one block to each server, even with the
        // narrow address-string variance of two on-localhost replicas.
        int blockCount = 32;
        int fileSize = BLOCK_SIZE * blockCount;
        byte[] payload = new byte[fileSize];
        for (int i = 0; i < fileSize; i++) {
            payload[i] = (byte) (i & 0xFF);
        }
        String path = "ts/uuid/multipart/graph";
        // Pre-write directly into the shared bucket (visible to both servers).
        sharedInnerForPreWrite.write(path, payload).get(10, TimeUnit.SECONDS);

        long s1ReadRangeBefore = server1.counter("readrange_requests");
        long s2ReadRangeBefore = server2.counter("readrange_requests");

        // Predict routing via a local instance of the same router.
        ConsistentHashRouter predictor = new ConsistentHashRouter(
                List.of(server1.address, server2.address));
        int predictedS1 = 0;
        int predictedS2 = 0;
        for (long b = 0; b < blockCount; b++) {
            String routeKey = path + "#block" + b;
            String predicted = predictor.getServer(routeKey);
            if (predicted.equals(server1.address)) {
                predictedS1++;
            } else if (predicted.equals(server2.address)) {
                predictedS2++;
            }
        }
        // Both must be > 0 — otherwise the hash-routing assertion is
        // trivially satisfied. With 32 blocks and 150 virtual nodes per
        // server the probability of either side being empty is negligible.
        assertTrue("router must spread some blocks to server1: " + predictedS1
                + "/" + blockCount, predictedS1 > 0);
        assertTrue("router must spread some blocks to server2: " + predictedS2
                + "/" + blockCount, predictedS2 > 0);

        // Issue readFileRange for every block.
        for (long b = 0; b < blockCount; b++) {
            long offset = b * (long) BLOCK_SIZE;
            int length = (int) Math.min((long) BLOCK_SIZE, fileSize - offset);
            byte[] slice = client.readFileRange(path, offset, length, BLOCK_SIZE);
            assertNotNull("read of block " + b + " must succeed", slice);
            assertArrayEquals("block " + b + " content must match",
                    Arrays.copyOfRange(payload, (int) offset, (int) offset + length), slice);
        }

        long s1ReadRangeDelta = server1.counter("readrange_requests") - s1ReadRangeBefore;
        long s2ReadRangeDelta = server2.counter("readrange_requests") - s2ReadRangeBefore;

        // Per-server counters reflect the predicted share exactly.
        assertEquals("server1 must have served exactly its predicted partition",
                (long) predictedS1, s1ReadRangeDelta);
        assertEquals("server2 must have served exactly its predicted partition",
                (long) predictedS2, s2ReadRangeDelta);
        assertEquals("total readrange RPCs must equal the block count",
                (long) blockCount, s1ReadRangeDelta + s2ReadRangeDelta);

        // Each server's disk cache must hold ONLY its predicted blocks.
        for (long b = 0; b < blockCount; b++) {
            String routeKey = path + "#block" + b;
            String predicted = predictor.getServer(routeKey);
            String blockCacheKey = path + "#" + b;
            if (predicted.equals(server1.address)) {
                assertTrue("server1 cache must hold block " + b,
                        server1.caching.isInCache(blockCacheKey));
                assertFalse("server2 cache must NOT hold block " + b,
                        server2.caching.isInCache(blockCacheKey));
            } else {
                assertTrue("server2 cache must hold block " + b,
                        server2.caching.isInCache(blockCacheKey));
                assertFalse("server1 cache must NOT hold block " + b,
                        server1.caching.isInCache(blockCacheKey));
            }
        }
    }

    private ServerHandle startServer(String namePrefix, Path sharedDir) throws Exception {
        ExecutorService meta = Executors.newSingleThreadExecutor();
        LocalObjectStorage inner = new LocalObjectStorage(sharedDir, meta);
        Path cacheDir = folder.newFolder(namePrefix + "-cache").toPath();
        CachingObjectStorage caching = new CachingObjectStorage(inner, cacheDir, meta,
                64L * 1024 * 1024);
        TestStatsProvider stats = new TestStatsProvider();
        ThreadPoolExecutor readExec = singleThreadExec(namePrefix + "-read");
        ThreadPoolExecutor writeExec = singleThreadExec(namePrefix + "-write");
        RemoteFileServiceImpl service = new RemoteFileServiceImpl(caching,
                stats.getStatsLogger(""), readExec, writeExec);

        NettyChannelAcceptor acceptor = new NettyChannelAcceptor("localhost", 0, false);
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
        String address = "localhost:" + port;
        return new ServerHandle(address, acceptor, caching, stats, readExec, writeExec, meta);
    }

    static int boundPort(NettyChannelAcceptor a) throws Exception {
        java.lang.reflect.Field f = NettyChannelAcceptor.class.getDeclaredField("channel");
        f.setAccessible(true);
        Object ch = f.get(a);
        java.net.SocketAddress addr = ((io.netty.channel.Channel) ch).localAddress();
        return ((java.net.InetSocketAddress) addr).getPort();
    }

    static ThreadPoolExecutor singleThreadExec(String name) {
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
