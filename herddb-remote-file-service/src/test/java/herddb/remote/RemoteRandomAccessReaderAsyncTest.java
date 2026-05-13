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
import io.netty.util.ResourceLeakDetector;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link RemoteRandomAccessReader#readRangeAsync}: byte-equivalence
 * with the sync path, position-invariance, and safe concurrent interleaving
 * with synchronous reads on the same reader.
 *
 * <p>The Netty {@link ResourceLeakDetector} is set to PARANOID for this test
 * class so that any {@link io.netty.buffer.ByteBuf} released with the wrong
 * reference count (or not released at all) triggers an assertion error.
 */
public class RemoteRandomAccessReaderAsyncTest {

    @BeforeClass
    public static void enableLeakDetector() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;

    /** Block size used for write (multipart chunk) AND for reader buffer. */
    private static final int BLOCK_SIZE = 64;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("data").toPath());
        server.start();
        client = new RemoteFileServiceClient(List.of("localhost:" + server.getPort()));
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        server.stop();
    }

    /** Build a sequential byte array: byte[i] = (byte)(i & 0xFF). */
    private static byte[] seqBytes(int length) {
        byte[] b = new byte[length];
        for (int i = 0; i < length; i++) {
            b[i] = (byte) (i & 0xFF);
        }
        return b;
    }

    // -----------------------------------------------------------------------
    // readRangeAsync returns identical bytes to seek+readFully
    // -----------------------------------------------------------------------

    @Test
    public void testSingleBlockReadMatchesSync() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE * 3);
        client.writeMultipartFile("ts/idx/g1", new ByteArrayInputStream(data), BLOCK_SIZE);

        try (RemoteRandomAccessReader reader =
                new RemoteRandomAccessReader(client, "ts/idx/g1", data.length, BLOCK_SIZE)) {

            // Read a range entirely within the first block
            int off = 5;
            int len = 20;
            ByteBuffer async = reader.readRangeAsync(off, len).get(5, TimeUnit.SECONDS);
            byte[] asyncBytes = toArray(async);

            byte[] syncBytes = new byte[len];
            reader.seek(off);
            reader.readFully(syncBytes);

            assertArrayEquals("async and sync must return identical bytes", syncBytes, asyncBytes);
        }
    }

    @Test
    public void testCrossBlockReadMatchesSync() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE * 4);
        client.writeMultipartFile("ts/idx/g2", new ByteArrayInputStream(data), BLOCK_SIZE);

        try (RemoteRandomAccessReader reader =
                new RemoteRandomAccessReader(client, "ts/idx/g2", data.length, BLOCK_SIZE)) {

            // Range that straddles the boundary between block 1 and block 2
            int off = BLOCK_SIZE - 10;
            int len = 30; // 10 bytes from block 1 + 20 from block 2
            ByteBuffer async = reader.readRangeAsync(off, len).get(5, TimeUnit.SECONDS);
            byte[] asyncBytes = toArray(async);

            byte[] syncBytes = new byte[len];
            reader.seek(off);
            reader.readFully(syncBytes);

            assertArrayEquals("cross-block async and sync must be identical", syncBytes, asyncBytes);
        }
    }

    @Test
    public void testThreeBlockReadMatchesSync() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE * 5);
        client.writeMultipartFile("ts/idx/g3", new ByteArrayInputStream(data), BLOCK_SIZE);

        try (RemoteRandomAccessReader reader =
                new RemoteRandomAccessReader(client, "ts/idx/g3", data.length, BLOCK_SIZE)) {

            // Range spanning 3 blocks: starts near end of block 0, ends near start of block 2
            int off = BLOCK_SIZE - 5;
            int len = BLOCK_SIZE + 15; // 5 from block 0 + BLOCK_SIZE from block 1 + 10 from block 2
            ByteBuffer async = reader.readRangeAsync(off, len).get(5, TimeUnit.SECONDS);
            byte[] asyncBytes = toArray(async);

            byte[] syncBytes = new byte[len];
            reader.seek(off);
            reader.readFully(syncBytes);

            assertArrayEquals("three-block async and sync must be identical", syncBytes, asyncBytes);
        }
    }

    @Test
    public void testReadAtEndOfFileMatchesSync() throws Exception {
        // Use a file that ends mid-block so the last block is smaller than BLOCK_SIZE
        byte[] data = seqBytes(BLOCK_SIZE * 2 + 13);
        client.writeMultipartFile("ts/idx/g4", new ByteArrayInputStream(data), BLOCK_SIZE);

        try (RemoteRandomAccessReader reader =
                new RemoteRandomAccessReader(client, "ts/idx/g4", data.length, BLOCK_SIZE)) {

            // Read the last 13 bytes (the partial trailing block)
            int off = BLOCK_SIZE * 2;
            int len = 13;
            ByteBuffer async = reader.readRangeAsync(off, len).get(5, TimeUnit.SECONDS);
            byte[] asyncBytes = toArray(async);

            byte[] syncBytes = new byte[len];
            reader.seek(off);
            reader.readFully(syncBytes);

            assertArrayEquals("end-of-file async and sync must be identical", syncBytes, asyncBytes);
        }
    }

    // -----------------------------------------------------------------------
    // readRangeAsync must NOT change getPosition()
    // -----------------------------------------------------------------------

    @Test
    public void testReadRangeAsyncDoesNotChangePosition() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE * 3);
        client.writeMultipartFile("ts/idx/pos", new ByteArrayInputStream(data), BLOCK_SIZE);

        try (RemoteRandomAccessReader reader =
                new RemoteRandomAccessReader(client, "ts/idx/pos", data.length, BLOCK_SIZE)) {

            reader.seek(42);
            long posBefore = reader.getPosition();

            // Fire an async read at a completely different range
            reader.readRangeAsync(BLOCK_SIZE + 7, 15).get(5, TimeUnit.SECONDS);

            long posAfter = reader.getPosition();
            assertEquals("readRangeAsync must NOT change position", posBefore, posAfter);
        }
    }

    @Test
    public void testMultipleAsyncReadsDoNotChangePosition() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE * 4);
        client.writeMultipartFile("ts/idx/pos2", new ByteArrayInputStream(data), BLOCK_SIZE);

        try (RemoteRandomAccessReader reader =
                new RemoteRandomAccessReader(client, "ts/idx/pos2", data.length, BLOCK_SIZE)) {

            reader.seek(100);
            long posBefore = reader.getPosition();

            // Fire several async reads at different ranges
            List<CompletableFuture<ByteBuffer>> futures = new ArrayList<>();
            futures.add(reader.readRangeAsync(0, 10));
            futures.add(reader.readRangeAsync(BLOCK_SIZE - 3, 20));
            futures.add(reader.readRangeAsync(BLOCK_SIZE * 2 + 5, 8));
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(10, TimeUnit.SECONDS);

            assertEquals("multiple readRangeAsync calls must NOT change position", posBefore,
                    reader.getPosition());
        }
    }

    // -----------------------------------------------------------------------
    // Concurrent async reads alongside a synchronous readFully loop
    // -----------------------------------------------------------------------

    /**
     * Launches 16 concurrent {@code readRangeAsync} calls on one reader while a
     * synchronous {@code readFully} loop runs on the same instance from a separate
     * thread. Both paths must return bytes that match the ground-truth data array,
     * no {@link io.netty.util.IllegalReferenceCountException} may fire, and no
     * {@link ByteBuf} must be leaked (enforced by PARANOID leak detector).
     */
    @Test
    public void testConcurrentAsyncAndSyncReads() throws Exception {
        final int numBlocks = 8;
        byte[] data = seqBytes(BLOCK_SIZE * numBlocks);
        client.writeMultipartFile("ts/idx/conc", new ByteArrayInputStream(data), BLOCK_SIZE);

        SegmentBlockCache cache = new SegmentBlockCache(BLOCK_SIZE * 4L);
        AtomicReference<Throwable> syncError = new AtomicReference<>();
        CountDownLatch startLatch = new CountDownLatch(1);

        // Synchronous reader thread: repeatedly seeks to position 0 and reads the
        // entire file, verifying every byte.
        ExecutorService syncExec = Executors.newSingleThreadExecutor(
                r -> new Thread(r, "sync-reader"));
        try (RemoteRandomAccessReader reader = new RemoteRandomAccessReader(
                client, "ts/idx/conc", data.length, BLOCK_SIZE, BLOCK_SIZE,
                NullStatsLogger.INSTANCE, cache)) {

            Future<?> syncFuture = syncExec.submit(() -> {
                try {
                    startLatch.await();
                    for (int pass = 0; pass < 4; pass++) {
                        reader.seek(0);
                        byte[] buf = new byte[data.length];
                        reader.readFully(buf);
                        for (int i = 0; i < buf.length; i++) {
                            if (buf[i] != data[i]) {
                                throw new AssertionError(
                                        "sync mismatch at byte " + i);
                            }
                        }
                    }
                } catch (Throwable t) {
                    syncError.set(t);
                }
            });

            // Async reader: 16 concurrent readRangeAsync calls at various offsets
            startLatch.countDown();
            List<CompletableFuture<Void>> asyncFutures = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                final int off = (i * (BLOCK_SIZE / 2)) % (data.length - BLOCK_SIZE);
                final int len = BLOCK_SIZE / 2 + (i % 4);
                asyncFutures.add(reader.readRangeAsync(off, len).thenAccept(bb -> {
                    byte[] got = toArray(bb);
                    for (int j = 0; j < got.length; j++) {
                        if (got[j] != data[off + j]) {
                            throw new RuntimeException(
                                    "async mismatch at byte " + (off + j));
                        }
                    }
                }));
            }
            CompletableFuture.allOf(asyncFutures.toArray(new CompletableFuture[0]))
                    .get(30, TimeUnit.SECONDS);

            syncFuture.get(10, TimeUnit.SECONDS);
        } finally {
            syncExec.shutdown();
            syncExec.awaitTermination(5, TimeUnit.SECONDS);
            cache.cleanUp();
        }

        Throwable syncErr = syncError.get();
        if (syncErr != null) {
            throw new AssertionError("sync reader failed: " + syncErr.getMessage(), syncErr);
        }
    }

    // -----------------------------------------------------------------------
    // Block-cache interaction
    // -----------------------------------------------------------------------

    @Test
    public void testAsyncReadHitsBlockCacheAfterSyncFill() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE * 2);
        client.writeMultipartFile("ts/idx/cache", new ByteArrayInputStream(data), BLOCK_SIZE);

        SegmentBlockCache cache = new SegmentBlockCache(BLOCK_SIZE * 4L);
        try (RemoteRandomAccessReader reader = new RemoteRandomAccessReader(
                client, "ts/idx/cache", data.length, BLOCK_SIZE, BLOCK_SIZE,
                NullStatsLogger.INSTANCE, cache)) {

            // Warm the cache synchronously
            reader.seek(0);
            byte[] tmp = new byte[BLOCK_SIZE];
            reader.readFully(tmp);
            long hitsAfterSync = cache.hitCount();

            // Async read of the same block must be a cache hit
            reader.readRangeAsync(0, 10).get(5, TimeUnit.SECONDS);
            assertEquals("async read of warmed block must be a cache hit",
                    hitsAfterSync + 1, cache.hitCount());
        }
        cache.cleanUp();
    }

    @Test
    public void testAsyncReadWithDisabledCacheMatchesSync() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE * 3);
        client.writeMultipartFile("ts/idx/nocache", new ByteArrayInputStream(data), BLOCK_SIZE);

        // Disabled cache: pass-through, every read goes to the network
        try (RemoteRandomAccessReader reader = new RemoteRandomAccessReader(
                client, "ts/idx/nocache", data.length, BLOCK_SIZE)) {

            int off = BLOCK_SIZE + 5;
            int len = 20;
            ByteBuffer async = reader.readRangeAsync(off, len).get(5, TimeUnit.SECONDS);
            byte[] asyncBytes = toArray(async);

            byte[] syncBytes = new byte[len];
            reader.seek(off);
            reader.readFully(syncBytes);

            assertArrayEquals("no-cache async and sync must be identical", syncBytes, asyncBytes);
        }
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    @Test
    public void testReadRangeAsyncZeroLengthReturnsEmpty() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE);
        client.writeMultipartFile("ts/idx/zero", new ByteArrayInputStream(data), BLOCK_SIZE);

        try (RemoteRandomAccessReader reader =
                new RemoteRandomAccessReader(client, "ts/idx/zero", data.length, BLOCK_SIZE)) {

            ByteBuffer result = reader.readRangeAsync(0, 0).get(5, TimeUnit.SECONDS);
            assertEquals("zero-length read should return empty ByteBuffer", 0, result.remaining());
        }
    }

    @Test
    public void testReadRangeAsyncPastEofFails() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE);
        client.writeMultipartFile("ts/idx/eof", new ByteArrayInputStream(data), BLOCK_SIZE);

        try (RemoteRandomAccessReader reader =
                new RemoteRandomAccessReader(client, "ts/idx/eof", data.length, BLOCK_SIZE)) {

            CompletableFuture<ByteBuffer> future = reader.readRangeAsync(BLOCK_SIZE - 5, 20);
            try {
                future.get(5, TimeUnit.SECONDS);
                assertFalse("should have thrown ExecutionException", true);
            } catch (ExecutionException e) {
                // expected: read past end of file
            }
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static byte[] toArray(ByteBuffer bb) {
        byte[] out = new byte[bb.remaining()];
        bb.get(out);
        return out;
    }
}
