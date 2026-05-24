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
import herddb.remote.storage.ObjectStorage;
import herddb.remote.storage.ReadResult;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #650: verifies that {@code prefetchFileRange} prewarm followed by
 * {@code readFileRange} for every block results in ZERO additional inner
 * {@code readRange} calls — the prewarm achieves a cold-start cache hit
 * rate of 100%.
 *
 * <p>Two {@link RemoteFileServiceImpl} replicas share the same on-disk
 * "S3 bucket"; each replica wraps its own {@link CachingObjectStorage}
 * decorating a per-replica {@link CountingInnerStorage} that records every
 * {@link ObjectStorage#readRange} the cache layer escalates to it.
 *
 * <p>The test:
 * <ol>
 *   <li>pre-writes a multi-block object directly into the shared bucket;</li>
 *   <li>issues {@code prefetchFileRangeAsync} for every block — the routing
 *       follows the same {@link ConsistentHashRouter} as the read RPCs, so
 *       prewarm primes the correct server's cache per block;</li>
 *   <li>asserts each server's cache holds only its predicted partition;</li>
 *   <li>asserts the {@code rfs_prefetch_requests} counters sum to the total
 *       block count and each server got its predicted share;</li>
 *   <li>captures the pre-read {@code readRange} call counts and issues a
 *       full {@code readFileRange} pass over every block — those reads must
 *       trigger ZERO further inner.readRange calls (every block is already
 *       resident in the per-replica disk cache).</li>
 * </ol>
 */
public class RemoteFileServicePrefetchTwoServerShardingTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final int BLOCK_SIZE = 4 * 1024;

    /**
     * Wrapper around {@link LocalObjectStorage} that counts how many
     * {@code readRange} calls the {@link CachingObjectStorage} above it
     * escalates to it. Used to assert "zero further reads after prewarm".
     */
    static final class CountingInnerStorage implements ObjectStorage {
        private final LocalObjectStorage delegate;
        final AtomicInteger readRangeCalls = new AtomicInteger();

        CountingInnerStorage(LocalObjectStorage delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<Void> write(String path, byte[] content) {
            return delegate.write(path, content);
        }

        @Override
        public CompletableFuture<ReadResult> read(String path) {
            return delegate.read(path);
        }

        @Override
        public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
            readRangeCalls.incrementAndGet();
            return delegate.readRange(path, offset, length, blockSize);
        }

        @Override
        public CompletableFuture<Boolean> delete(String path) {
            return delegate.delete(path);
        }

        @Override
        public CompletableFuture<List<String>> list(String prefix) {
            return delegate.list(prefix);
        }

        @Override
        public CompletableFuture<java.lang.Integer> deleteByPrefix(String prefix) {
            return delegate.deleteByPrefix(prefix);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }

    static final class ServerHandle implements AutoCloseable {
        final String address;
        final NettyChannelAcceptor acceptor;
        final CachingObjectStorage caching;
        final CountingInnerStorage countingInner;
        final TestStatsProvider stats;
        final ThreadPoolExecutor readExec;
        final ThreadPoolExecutor writeExec;
        final ExecutorService meta;

        ServerHandle(String address, NettyChannelAcceptor acceptor,
                     CachingObjectStorage caching, CountingInnerStorage countingInner,
                     TestStatsProvider stats,
                     ThreadPoolExecutor readExec, ThreadPoolExecutor writeExec,
                     ExecutorService meta) {
            this.address = address;
            this.acceptor = acceptor;
            this.caching = caching;
            this.countingInner = countingInner;
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
    public void prefetchPrimesEveryBlockSoFollowupReadsHitWithoutInnerCalls() throws Exception {
        // Use a 32-block file. With two servers and 150 virtual ring entries
        // each, the per-block routing distribution can be quite skewed for a
        // small block count (the live-server addresses differ only in port).
        // 32 blocks is enough to guarantee both servers get at least one block
        // with overwhelming probability while still keeping the test fast.
        int blockCount = 32;
        int fileSize = BLOCK_SIZE * blockCount;
        byte[] payload = new byte[fileSize];
        for (int i = 0; i < fileSize; i++) {
            payload[i] = (byte) (i & 0xFF);
        }
        String path = "ts/uuid/multipart/graph";
        sharedInnerForPreWrite.write(path, payload).get(10, TimeUnit.SECONDS);

        // Predict routing via a fresh router with the same server list.
        ConsistentHashRouter predictor = new ConsistentHashRouter(
                List.of(server1.address, server2.address));
        int predictedS1 = 0;
        int predictedS2 = 0;
        for (long b = 0; b < blockCount; b++) {
            String routeKey = path + "#block" + b;
            if (predictor.getServer(routeKey).equals(server1.address)) {
                predictedS1++;
            } else {
                predictedS2++;
            }
        }
        // Both servers should hold at least one predicted block — otherwise
        // the routing assertion is trivially satisfied. With 32 blocks the
        // probability of either being zero is tiny; if this ever fires,
        // bump blockCount further.
        assertTrue("expected mixed routing across 32 blocks; got s1=" + predictedS1
                + "/" + blockCount, predictedS1 > 0);
        assertTrue("expected mixed routing across 32 blocks; got s2=" + predictedS2
                + "/" + blockCount, predictedS2 > 0);

        long s1PrefetchBefore = server1.counter("prefetch_requests");
        long s2PrefetchBefore = server2.counter("prefetch_requests");

        // Issue one prefetch per block.
        for (long b = 0; b < blockCount; b++) {
            long offset = b * (long) BLOCK_SIZE;
            int length = (int) Math.min((long) BLOCK_SIZE, fileSize - offset);
            Boolean ok = client.prefetchFileRangeAsync(path, offset, length, BLOCK_SIZE)
                    .get(15, TimeUnit.SECONDS);
            assertEquals("prefetch of block " + b + " must report TRUE", Boolean.TRUE, ok);
        }

        long s1PrefetchDelta = server1.counter("prefetch_requests") - s1PrefetchBefore;
        long s2PrefetchDelta = server2.counter("prefetch_requests") - s2PrefetchBefore;
        assertEquals("prewarm routing must match readFileRange routing on server1",
                (long) predictedS1, s1PrefetchDelta);
        assertEquals("prewarm routing must match readFileRange routing on server2",
                (long) predictedS2, s2PrefetchDelta);
        assertEquals("total prefetch RPCs must equal block count",
                (long) blockCount, s1PrefetchDelta + s2PrefetchDelta);

        // Each server's cache must hold ONLY its predicted partition.
        for (long b = 0; b < blockCount; b++) {
            String routeKey = path + "#block" + b;
            String predicted = predictor.getServer(routeKey);
            String blockCacheKey = path + "#" + b;
            if (predicted.equals(server1.address)) {
                assertTrue("server1 cache must hold block " + b + " after prewarm",
                        server1.caching.isInCache(blockCacheKey));
                assertFalse("server2 cache must NOT hold block " + b,
                        server2.caching.isInCache(blockCacheKey));
            } else {
                assertTrue("server2 cache must hold block " + b + " after prewarm",
                        server2.caching.isInCache(blockCacheKey));
                assertFalse("server1 cache must NOT hold block " + b,
                        server1.caching.isInCache(blockCacheKey));
            }
        }

        // Snapshot the inner-storage readRange counters BEFORE the follow-up
        // readFileRange pass so we can assert "zero further calls".
        int s1InnerBefore = server1.countingInner.readRangeCalls.get();
        int s2InnerBefore = server2.countingInner.readRangeCalls.get();

        // Read every block — every request must hit the per-replica disk
        // cache; the wrapped inner storage must see ZERO new readRange calls.
        for (long b = 0; b < blockCount; b++) {
            long offset = b * (long) BLOCK_SIZE;
            int length = (int) Math.min((long) BLOCK_SIZE, fileSize - offset);
            byte[] slice = client.readFileRange(path, offset, length, BLOCK_SIZE);
            assertNotNull("post-prewarm read of block " + b + " must succeed", slice);
            assertArrayEquals("post-prewarm block " + b + " must match payload",
                    Arrays.copyOfRange(payload, (int) offset, (int) offset + length), slice);
        }

        int s1InnerDelta = server1.countingInner.readRangeCalls.get() - s1InnerBefore;
        int s2InnerDelta = server2.countingInner.readRangeCalls.get() - s2InnerBefore;
        assertEquals("server1 must serve every predicted block from its disk cache (zero new inner calls)",
                0, s1InnerDelta);
        assertEquals("server2 must serve every predicted block from its disk cache (zero new inner calls)",
                0, s2InnerDelta);
    }

    private ServerHandle startServer(String namePrefix, Path sharedDir) throws Exception {
        ExecutorService meta = Executors.newSingleThreadExecutor();
        LocalObjectStorage local = new LocalObjectStorage(sharedDir, meta);
        CountingInnerStorage counting = new CountingInnerStorage(local);
        Path cacheDir = folder.newFolder(namePrefix + "-cache").toPath();
        CachingObjectStorage caching = new CachingObjectStorage(counting, cacheDir, meta,
                64L * 1024 * 1024);
        TestStatsProvider stats = new TestStatsProvider();
        ThreadPoolExecutor readExec = RemoteFileServiceTwoServerShardingTest
                .singleThreadExec(namePrefix + "-read");
        ThreadPoolExecutor writeExec = RemoteFileServiceTwoServerShardingTest
                .singleThreadExec(namePrefix + "-write");
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
        int port = RemoteFileServiceTwoServerShardingTest.boundPort(acceptor);
        String address = "localhost:" + port;
        return new ServerHandle(address, acceptor, caching, counting, stats, readExec, writeExec, meta);
    }
}
