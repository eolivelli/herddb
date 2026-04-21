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
import static org.junit.Assert.assertTrue;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.ByteArrayInputStream;
import java.util.List;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies the client-side read metrics
 * ({@code rfs_client_read_requests}/{@code rfs_client_read_bytes}), the shared
 * {@link SegmentBlockCache} integration, and the per-request counters driven
 * by {@link VectorSearchRequestContext}.
 *
 * <p>This covers the issue #196 scope (per-request readFileRange observability)
 * plus the two pre-existing counter bugs fixed in {@code ensureBlockLoaded}:
 * the double-incremented {@code clientReadRequests} and the
 * {@code clientReadBytes.inc()} that counted requests instead of bytes.
 */
public class RemoteRandomAccessReaderCacheAndMetricsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final int BLOCK_SIZE = 64;

    private RemoteFileServer server;
    private RemoteFileServiceClient client;
    private TestStatsProvider statsProvider;
    private TestStatsProvider.TestStatsLogger statsLogger;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("data").toPath());
        server.start();
        client = new RemoteFileServiceClient(List.of("localhost:" + server.getPort()));
        statsProvider = new TestStatsProvider();
        statsLogger = statsProvider.getStatsLogger("");
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        server.stop();
        VectorSearchRequestContext.end();
    }

    private static byte[] seqBytes(int length) {
        byte[] b = new byte[length];
        for (int i = 0; i < length; i++) {
            b[i] = (byte) (i & 0xFF);
        }
        return b;
    }

    private long counter(String scope, String name) {
        return statsLogger.scope(scope).getCounter(name).get().longValue();
    }

    @Test
    public void clientReadCountersReflectActualRpcCallsAndBytes() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE * 3);
        client.writeMultipartFile("ts/idx/one", new ByteArrayInputStream(data), BLOCK_SIZE);

        RemoteRandomAccessReader reader = new RemoteRandomAccessReader(
                client, "ts/idx/one", data.length, BLOCK_SIZE, BLOCK_SIZE,
                statsLogger, SegmentBlockCache.disabled());
        // Sequential read over 3 blocks — should trigger exactly 3 RPCs.
        byte[] buf = new byte[BLOCK_SIZE * 3];
        reader.readFully(buf);
        reader.close();

        long requests = counter("rfs.client", "read_requests");
        long bytes = counter("rfs.client", "read_bytes");
        assertEquals("one RPC per block (no double-counting)", 3L, requests);
        assertEquals("read_bytes reflects actual bytes, not request count",
                (long) BLOCK_SIZE * 3, bytes);
    }

    @Test
    public void perRequestContextIsPopulatedByReadsInsideBeginEnd() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE * 2);
        client.writeMultipartFile("ts/idx/ctx", new ByteArrayInputStream(data), BLOCK_SIZE);

        RemoteRandomAccessReader reader = new RemoteRandomAccessReader(
                client, "ts/idx/ctx", data.length, BLOCK_SIZE, BLOCK_SIZE,
                statsLogger, SegmentBlockCache.disabled());
        VectorSearchRequestContext ctx = VectorSearchRequestContext.begin();
        try {
            byte[] buf = new byte[BLOCK_SIZE * 2];
            reader.readFully(buf);
        } finally {
            reader.close();
        }
        assertEquals(2L, ctx.getReadFileRangeCalls());
        assertEquals((long) BLOCK_SIZE * 2, ctx.getReadFileRangeBytes());
        assertTrue("wait time should be positive", ctx.getReadFileRangeWaitNanos() > 0L);
        // With no cache configured every read counts as a miss.
        assertEquals(0L, ctx.getCacheHits());
        assertEquals(2L, ctx.getCacheMisses());
        VectorSearchRequestContext.end();
    }

    @Test
    public void perRequestContextIsNotTouchedOutsideBeginEnd() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE);
        client.writeMultipartFile("ts/idx/no-ctx", new ByteArrayInputStream(data), BLOCK_SIZE);

        RemoteRandomAccessReader reader = new RemoteRandomAccessReader(
                client, "ts/idx/no-ctx", data.length, BLOCK_SIZE, BLOCK_SIZE,
                statsLogger, SegmentBlockCache.disabled());
        byte[] buf = new byte[BLOCK_SIZE];
        reader.readFully(buf);
        reader.close();
        // No exception means ensureBlockLoaded safely handles a null context.
    }

    @Test
    public void secondReaderShareCacheAndSeesHits() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE * 3);
        client.writeMultipartFile("ts/idx/shared", new ByteArrayInputStream(data), BLOCK_SIZE);

        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);

        ReaderSupplier supplier = new RemoteRandomAccessReader.Supplier(
                client, "ts/idx/shared", data.length, BLOCK_SIZE, BLOCK_SIZE,
                statsLogger, cache);

        // Warm the cache on reader #1 — 3 misses → 3 RPCs.
        RemoteRandomAccessReader r1 = (RemoteRandomAccessReader) supplier.get();
        byte[] buf = new byte[BLOCK_SIZE * 3];
        r1.readFully(buf);
        r1.close();
        long requestsAfterFirstReader = counter("rfs.client", "read_requests");
        assertEquals(3L, requestsAfterFirstReader);

        // Reader #2: same segment, same byte ranges. All reads should be hits.
        RemoteRandomAccessReader r2 = (RemoteRandomAccessReader) supplier.get();
        VectorSearchRequestContext ctx = VectorSearchRequestContext.begin();
        byte[] buf2 = new byte[BLOCK_SIZE * 3];
        r2.readFully(buf2);
        r2.close();
        long requestsAfterSecondReader = counter("rfs.client", "read_requests");
        assertEquals("no additional RPCs — all served from cache",
                requestsAfterFirstReader, requestsAfterSecondReader);
        assertEquals("3 cache hits for the 3 re-read blocks", 3L, ctx.getCacheHits());
        assertEquals(0L, ctx.getCacheMisses());
        // The per-request accumulator still tracks the logical reads.
        assertEquals(3L, ctx.getReadFileRangeCalls());
        assertEquals((long) BLOCK_SIZE * 3, ctx.getReadFileRangeBytes());
        VectorSearchRequestContext.end();

        assertNotNull("reader returned non-null bytes", buf2);
        assertTrue("final block payload correct", buf2[BLOCK_SIZE * 3 - 1] == buf[BLOCK_SIZE * 3 - 1]);
    }

    @Test
    public void cacheInvalidatePathForcesReloadFromRemote() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE * 2);
        client.writeMultipartFile("ts/idx/inv", new ByteArrayInputStream(data), BLOCK_SIZE);

        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        ReaderSupplier supplier = new RemoteRandomAccessReader.Supplier(
                client, "ts/idx/inv", data.length, BLOCK_SIZE, BLOCK_SIZE,
                statsLogger, cache);

        RemoteRandomAccessReader r1 = (RemoteRandomAccessReader) supplier.get();
        byte[] buf = new byte[BLOCK_SIZE * 2];
        r1.readFully(buf);
        r1.close();
        long requestsAfterWarm = counter("rfs.client", "read_requests");
        assertEquals(2L, requestsAfterWarm);

        cache.invalidatePath("ts/idx/inv");

        // After invalidation, next read must refetch from remote.
        RemoteRandomAccessReader r2 = (RemoteRandomAccessReader) supplier.get();
        byte[] buf2 = new byte[BLOCK_SIZE * 2];
        r2.readFully(buf2);
        r2.close();
        long requestsAfterInvalidate = counter("rfs.client", "read_requests");
        assertEquals("invalidation forced 2 more RPCs",
                requestsAfterWarm + 2, requestsAfterInvalidate);
    }
}
