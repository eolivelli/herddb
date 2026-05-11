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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ResourceLeakDetector;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for RemoteFileServiceClient ByteBuf API methods.
 * Exercises new ByteBuf-based write/read methods and verifies reference counting.
 *
 * @author test
 */
public class RemoteFileServiceClientByteBufTest {

    @BeforeClass
    public static void enableLeakDetection() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("data").toPath());
        server.start();
        client = new RemoteFileServiceClient(Arrays.asList("localhost:" + server.getPort()));
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        server.stop();
    }

    /**
     * Test writeFile(String, ByteBuf) with a heap ByteBuf.
     * Verifies zero-copy write, reference counting, and round-trip correctness.
     */
    @Test
    public void testWriteFileWithHeapByteBuf() {
        String path = "test/writeFile/heap/data.bin";
        String content = "Hello ByteBuf World!";
        byte[] expected = content.getBytes(StandardCharsets.UTF_8);

        // Allocate from pool (heap buffer)
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(256);
        assertEquals("New buffer should have refCnt=1", 1, buf.refCnt());
        buf.writeBytes(expected);

        // Write via client
        client.writeFile(path, buf);
        assertEquals("Buffer should still be valid after writeFile", 1, buf.refCnt());

        // Release and verify refCnt drops to 0
        buf.release();
        assertEquals("Released buffer should have refCnt=0", 0, buf.refCnt());

        // Verify round-trip via byte[] API
        byte[] read = client.readFile(path);
        assertArrayEquals("Round-trip content mismatch", expected, read);
    }

    /**
     * Test writeFile(String, ByteBuf) with a direct ByteBuf.
     * Verifies that direct buffers can be used with zero-copy via nioBuffer().
     */
    @Test
    public void testWriteFileWithDirectByteBuf() {
        String path = "test/writeFile/direct/data.bin";
        String content = "Direct ByteBuf Content";
        byte[] expected = content.getBytes(StandardCharsets.UTF_8);

        // Allocate direct buffer from pool
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(256);
        assertEquals("New buffer should have refCnt=1", 1, buf.refCnt());
        buf.writeBytes(expected);

        // Write via client
        client.writeFile(path, buf);
        assertEquals("Buffer should still be valid after writeFile", 1, buf.refCnt());

        // Release
        buf.release();
        assertEquals("Released buffer should have refCnt=0", 0, buf.refCnt());

        // Verify round-trip
        byte[] read = client.readFile(path);
        assertArrayEquals("Round-trip content mismatch", expected, read);
    }

    /**
     * Test writeFileAsync(String, ByteBuf) with async completion.
     * Verifies that the buffer lifetime is properly managed across async calls.
     */
    @Test
    public void testWriteFileAsyncWithByteBuf() throws Exception {
        String path = "test/writeFileAsync/data.bin";
        String content = "Async ByteBuf Write";
        byte[] expected = content.getBytes(StandardCharsets.UTF_8);

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(256);
        assertEquals(1, buf.refCnt());
        buf.writeBytes(expected);

        // Write asynchronously
        CompletableFuture<Long> future = client.writeFileAsync(path, buf);
        Long writtenSize = future.get();
        assertEquals("Written size should match content length", expected.length, writtenSize.longValue());

        // Release after async completion
        buf.release();
        assertEquals(0, buf.refCnt());

        // Verify round-trip
        byte[] read = client.readFile(path);
        assertArrayEquals(expected, read);
    }

    /**
     * Test readFileAsByteBuf(String) returns a pooled ByteBuf with correct reference counting.
     * Tests the happy path where file exists.
     */
    @Test
    public void testReadFileAsByteBufFound() {
        String path = "test/readFileAsByteBuf/existing.bin";
        String content = "ByteBuf Read Test Content";
        byte[] expected = content.getBytes(StandardCharsets.UTF_8);

        // Write via existing byte[] API
        client.writeFile(path, expected);

        // Read via new ByteBuf API
        ByteBuf result = client.readFileAsByteBuf(path);
        assertNotNull("Result should not be null for existing file", result);
        assertEquals("New ByteBuf should have refCnt=1", 1, result.refCnt());

        // Verify content
        byte[] read = new byte[result.readableBytes()];
        result.readBytes(read);
        assertArrayEquals("Content should match", expected, read);

        // Release and verify refCnt drops to 0
        result.release();
        assertEquals("Released buffer should have refCnt=0", 0, result.refCnt());
    }

    /**
     * Test readFileAsByteBuf(String) returns null when file does not exist.
     * Verifies no ByteBuf is allocated for non-existent files.
     */
    @Test
    public void testReadFileAsByteBufNotFound() {
        String path = "test/readFileAsByteBuf/nonexistent.bin";

        // Read non-existent file
        ByteBuf result = client.readFileAsByteBuf(path);
        assertNull("Result should be null for non-existent file", result);
        // No refCnt to check — no buffer was allocated
    }

    /**
     * Test readFileAsByteBufAsync(String) with async completion.
     * Verifies proper reference counting in async context.
     */
    @Test
    public void testReadFileAsByteBufAsync() throws Exception {
        String path = "test/readFileAsByteBufAsync/data.bin";
        String content = "Async Read ByteBuf";
        byte[] expected = content.getBytes(StandardCharsets.UTF_8);

        // Write known content
        client.writeFile(path, expected);

        // Read asynchronously
        CompletableFuture<ByteBuf> future = client.readFileAsByteBufAsync(path);
        ByteBuf result = future.get();
        assertNotNull("Result should not be null", result);
        assertEquals("New ByteBuf should have refCnt=1", 1, result.refCnt());

        // Verify content
        byte[] read = new byte[result.readableBytes()];
        result.readBytes(read);
        assertArrayEquals(expected, read);

        // Release
        result.release();
        assertEquals(0, result.refCnt());
    }

    /**
     * Test writeFileBlock(String, long, ByteBuf) for multipart writes.
     * Verifies that individual blocks can be written and read back via readFileRange.
     */
    @Test
    public void testWriteFileBlockWithByteBuf() {
        String path = "test/multipart/blockwrite.bin";
        String blockContent = "This is block data content";
        byte[] expected = blockContent.getBytes(StandardCharsets.UTF_8);

        // Allocate and write a block
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(256);
        assertEquals(1, buf.refCnt());
        buf.writeBytes(expected);

        // Write as a single block
        client.writeFileBlock(path, 0L, buf);
        assertEquals(1, buf.refCnt());

        // Release after write
        buf.release();
        assertEquals(0, buf.refCnt());

        // Verify by reading back via readFileRange
        byte[] read = client.readFileRange(path, 0, expected.length, Math.max(expected.length, 1024));
        assertArrayEquals("Block content should match", expected, read);
    }

    /**
     * Test writeMultipartFile with exactly blockSize-aligned data.
     * Verifies the internal pooled ByteBuf buffer works correctly for multi-block writes.
     */
    @Test
    public void testWriteMultipartFileAlignedBlocks() throws IOException {
        String path = "test/multipart/aligned.bin";
        int blockSize = 1024; // 1 KB blocks
        int numBlocks = 3;

        // Create content that is exactly 3 blocks
        byte[] totalContent = new byte[blockSize * numBlocks];
        for (int i = 0; i < totalContent.length; i++) {
            totalContent[i] = (byte) (i % 256);
        }

        // Write via multipart
        ByteArrayInputStream in = new ByteArrayInputStream(totalContent);
        long writtenBytes = client.writeMultipartFile(path, in, blockSize);
        assertEquals("Written bytes should match total content", totalContent.length, writtenBytes);

        // Verify each block via readFileRange
        for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
            long offset = (long) blockIdx * blockSize;
            byte[] read = client.readFileRange(path, offset, blockSize, blockSize);
            byte[] expected = Arrays.copyOfRange(totalContent, (int) offset, (int) offset + blockSize);
            assertArrayEquals("Block " + blockIdx + " content mismatch", expected, read);
        }
    }

    /**
     * Test writeMultipartFile with partial last block.
     * Verifies that the pooled buffer correctly handles non-aligned final blocks
     * (no Arrays.copyOf overhead in the new implementation).
     */
    @Test
    public void testWriteMultipartFilePartialLastBlock() throws IOException {
        String path = "test/multipart/partial.bin";
        int blockSize = 1024;
        int totalSize = blockSize * 2 + 512; // 2.5 blocks

        // Create content
        byte[] totalContent = new byte[totalSize];
        for (int i = 0; i < totalContent.length; i++) {
            totalContent[i] = (byte) (i % 256);
        }

        // Write
        ByteArrayInputStream in = new ByteArrayInputStream(totalContent);
        long writtenBytes = client.writeMultipartFile(path, in, blockSize);
        assertEquals("Written bytes should match total content", totalSize, writtenBytes);

        // Verify full blocks and partial block
        long offset = 0;
        byte[] block1 = client.readFileRange(path, offset, blockSize, blockSize);
        assertArrayEquals("Block 1", Arrays.copyOfRange(totalContent, 0, blockSize), block1);

        offset += blockSize;
        byte[] block2 = client.readFileRange(path, offset, blockSize, blockSize);
        assertArrayEquals("Block 2", Arrays.copyOfRange(totalContent, blockSize, 2 * blockSize), block2);

        offset += blockSize;
        int partialSize = 512;
        byte[] partialBlock = client.readFileRange(path, offset, partialSize, blockSize);
        assertArrayEquals("Partial block", Arrays.copyOfRange(totalContent, 2 * blockSize, 2 * blockSize + partialSize),
            partialBlock);
    }

    /**
     * Test single-byte write/read round-trip with ByteBuf.
     * Edge case: minimal buffer usage.
     */
    @Test
    public void testWriteReadSingleByteWithByteBuf() {
        String path = "test/singlebyte/data.bin";
        byte[] content = {42};

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(8);
        buf.writeByte(42);
        client.writeFile(path, buf);
        buf.release();

        byte[] read = client.readFile(path);
        assertArrayEquals(content, read);
    }

    /**
     * Test large ByteBuf write/read (to verify pooling for larger allocations).
     * Allocates a 1 MB buffer.
     */
    @Test
    public void testWriteReadLargeByteBuffWithByteBuf() {
        String path = "test/large/1mb.bin";
        int size = 1 * 1024 * 1024; // 1 MB
        byte[] content = new byte[size];
        for (int i = 0; i < size; i++) {
            content[i] = (byte) (i % 256);
        }

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(size);
        buf.writeBytes(content);
        client.writeFile(path, buf);
        buf.release();

        byte[] read = client.readFile(path);
        assertArrayEquals(content, read);
    }

    // ---------------------------------------------------------------------
    // Issue #246 — in-flight read-byte budget
    // ---------------------------------------------------------------------

    /**
     * Default construction exposes the documented default byte budget
     * ({@link RemoteFileServiceClient#DEFAULT_CLIENT_MAX_INFLIGHT_READ_BYTES}).
     */
    @Test
    public void testInflightReadBytes_defaultConfig() {
        assertEquals("default max inflight read bytes",
                RemoteFileServiceClient.DEFAULT_CLIENT_MAX_INFLIGHT_READ_BYTES,
                client.maxInflightReadBytes());
        assertEquals("all bytes available at steady state",
                RemoteFileServiceClient.DEFAULT_CLIENT_MAX_INFLIGHT_READ_BYTES,
                client.availableInflightReadBytes());
    }

    /**
     * A custom {@code remote.file.client.max.inflight.read.bytes} config
     * value is honoured, and {@code availableInflightReadBytes()} starts
     * equal to the cap before any read is issued.
     */
    @Test
    public void testInflightReadBytes_customConfig() throws Exception {
        long customBytes = 16L * 1024 * 1024; // 16 MiB
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_READ_BYTES, customBytes);
        try (RemoteFileServiceClient tuned = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()), cfg)) {
            assertEquals("configured cap is applied", customBytes, tuned.maxInflightReadBytes());
            assertEquals("all bytes available at idle",
                    customBytes, tuned.availableInflightReadBytes());
        }
    }

    /** A non-positive byte budget is rejected at construction. */
    @Test
    public void testInflightReadBytes_zeroConfigRejected() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_READ_BYTES, 0L);
        try {
            new RemoteFileServiceClient(
                    Arrays.asList("localhost:" + server.getPort()), cfg).close();
            fail("expected IllegalArgumentException for non-positive byte budget");
        } catch (IllegalArgumentException expected) {
            assertTrue("message names the property",
                    expected.getMessage().contains(
                            RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_READ_BYTES));
        }
    }

    /**
     * A byte budget below {@code blockSize} is rejected: a single full-block
     * read would immediately deadlock.
     */
    @Test
    public void testInflightReadBytes_belowBlockSizeRejected() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_BLOCK_SIZE, 4096);
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_READ_BYTES, 2048L);
        try {
            new RemoteFileServiceClient(
                    Arrays.asList("localhost:" + server.getPort()), cfg).close();
            fail("expected IllegalArgumentException when budget < blockSize");
        } catch (IllegalArgumentException expected) {
            assertTrue("message names the properties",
                    expected.getMessage().contains(
                            RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_READ_BYTES));
        }
    }

    /**
     * After a successful {@code readFileAsByteBufAsync} the byte reservation
     * is released — a subsequent call sees the full budget again.
     */
    @Test
    public void testInflightReadBytes_releasedAfterReadFile() throws Exception {
        String path = "test/permits/readFile.bin";
        client.writeFile(path, "payload".getBytes(StandardCharsets.UTF_8));

        long before = client.availableInflightReadBytes();
        ByteBuf buf = client.readFileAsByteBufAsync(path).get();
        assertNotNull(buf);
        try {
            long after = client.availableInflightReadBytes();
            assertEquals("reservation is released on onCompleted", before, after);
        } finally {
            buf.release();
        }
    }

    /**
     * After a successful {@code readFileRangeAsByteBufAsync} against a
     * multipart file the byte reservation is released. Exercises the hot
     * path that issue #246 targets.
     */
    @Test
    public void testInflightReadBytes_releasedAfterReadFileRange() throws Exception {
        String path = "test/permits/readFileRange.bin";
        int blockSize = 4096;
        byte[] content = new byte[blockSize * 2];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) (i % 256);
        }
        client.writeMultipartFile(path, new ByteArrayInputStream(content), blockSize);

        long before = client.availableInflightReadBytes();
        ByteBuf buf = client.readFileRangeAsByteBufAsync(path, 0, blockSize, blockSize).get();
        assertNotNull(buf);
        try {
            long after = client.availableInflightReadBytes();
            assertEquals("reservation is released on onCompleted", before, after);
        } finally {
            ReferenceCountUtil.safeRelease(buf);
        }
    }

    /**
     * A burst of {@code readFileRange} calls issued sequentially from the
     * test thread must never reveal a negative budget and must return the
     * full cap by the time every call settles. The acquires happen serially
     * on the calling thread; the underlying RPC completions overlap on the
     * Netty event-loop pool. End-to-end smoke test that acquire/release
     * are balanced when many futures are outstanding at once. (The
     * "concurrent" in the previous name referred to the RPC completion
     * order, not the acquisition order — see issue #477).
     */
    @Test
    public void testInflightReadBytes_serialBurstReturnsReservations() throws Exception {
        String path = "test/permits/serial-burst.bin";
        int blockSize = 4096;
        byte[] content = new byte[blockSize * 8];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) i;
        }
        client.writeMultipartFile(path, new ByteArrayInputStream(content), blockSize);

        long max = client.maxInflightReadBytes();
        int burst = 32;
        @SuppressWarnings("unchecked")
        CompletableFuture<ByteBuf>[] futures = new CompletableFuture[burst];
        for (int i = 0; i < burst; i++) {
            futures[i] = client.readFileRangeAsByteBufAsync(path, 0, blockSize, blockSize);
        }
        for (CompletableFuture<ByteBuf> f : futures) {
            ByteBuf b = f.get(30, TimeUnit.SECONDS);
            if (b != null) {
                ReferenceCountUtil.safeRelease(b);
            }
            assertTrue("available bytes must never exceed cap",
                    client.availableInflightReadBytes() <= max);
            assertTrue("available bytes must never go negative",
                    client.availableInflightReadBytes() >= 0);
        }
        assertEquals("all bytes returned at the end of the burst",
                max, client.availableInflightReadBytes());
    }

    /**
     * A small byte cap that still admits one full-block read restores the
     * budget after the read settles.
     */
    @Test
    public void testInflightReadBytes_smallBudgetRoundTrip() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        // 4 MiB budget — exactly one default block. Any smaller and the
        // constructor refuses (see testInflightReadBytes_belowBlockSizeRejected).
        long budget = RemoteFileServiceClient.DEFAULT_BLOCK_SIZE;
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_READ_BYTES, budget);
        try (RemoteFileServiceClient tuned = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()), cfg)) {
            String path = "test/permits/single.bin";
            tuned.writeFile(path, "one".getBytes(StandardCharsets.UTF_8));
            assertEquals(budget, tuned.availableInflightReadBytes());
            ByteBuf buf = tuned.readFileAsByteBufAsync(path).get();
            assertNotNull(buf);
            buf.release();
            assertEquals("budget restored after single read completes",
                    budget, tuned.availableInflightReadBytes());
        }
    }

    // ---------------------------------------------------------------------
    // Issue #246 — error-path coverage for the in-flight budget release
    // ---------------------------------------------------------------------

    /**
     * File-not-found on {@code readFileAsByteBufAsync} completes normally
     * with {@code null} (no error) — the reservation must still be
     * released. Without proper release, repeated not-found polls would
     * starve the budget.
     */
    @Test
    public void testInflightReadBytes_releasedAfterReadFileNotFound() throws Exception {
        long before = client.availableInflightReadBytes();
        ByteBuf buf = client.readFileAsByteBufAsync("test/permits/missing.bin").get();
        assertNull("missing file returns null", buf);
        assertEquals("reservation released on onCompleted(null)",
                before, client.availableInflightReadBytes());
    }

    /**
     * File-not-found on {@code readFileRangeAsByteBufAsync} also completes
     * normally with {@code null}; exercise the hot path's release.
     */
    @Test
    public void testInflightReadBytes_releasedAfterReadFileRangeNotFound() throws Exception {
        long before = client.availableInflightReadBytes();
        ByteBuf buf = client.readFileRangeAsByteBufAsync(
                "test/permits/missing-range.bin", 0, 4096,
                RemoteFileServiceClient.DEFAULT_BLOCK_SIZE).get();
        assertNull("missing multipart file returns null", buf);
        assertEquals("reservation released on onCompleted(null)",
                before, client.availableInflightReadBytes());
    }

    /**
     * When the remote file service is unreachable the retry loop eventually
     * gives up — each attempt must release its reservation so the budget
     * returns to the configured cap.
     */
    @Test
    public void testInflightReadBytes_releasedAfterBackendUnreachable() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 1);
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_TIMEOUT, 2L);
        // Point at a port where nothing is listening.
        try (RemoteFileServiceClient broken = new RemoteFileServiceClient(
                Arrays.asList("localhost:1"), cfg)) {
            long before = broken.availableInflightReadBytes();
            try {
                broken.readFileRangeAsByteBufAsync("some/path", 0, 4096,
                        RemoteFileServiceClient.DEFAULT_BLOCK_SIZE)
                        .get(60, TimeUnit.SECONDS);
                fail("expected the read to fail against an unreachable backend");
            } catch (java.util.concurrent.ExecutionException expected) {
                // propagated from onError
            }
            assertEquals("reservation restored after retries exhausted",
                    before, broken.availableInflightReadBytes());
        }
    }

    /**
     * Mixed burst of hits and misses: permits must net to zero in-flight
     * regardless of which branch each RPC takes.
     */
    @Test
    public void testInflightReadBytes_concurrentMixedOutcomesReleaseReservations()
            throws Exception {
        String presentPath = "test/permits/mixed-present.bin";
        int blockSize = 4096;
        byte[] content = new byte[blockSize * 2];
        client.writeMultipartFile(presentPath, new ByteArrayInputStream(content), blockSize);

        long max = client.maxInflightReadBytes();
        int burst = 16;
        @SuppressWarnings("unchecked")
        CompletableFuture<ByteBuf>[] futures = new CompletableFuture[burst];
        for (int i = 0; i < burst; i++) {
            // Half the calls target a non-existent file (server answers not-found
            // normally), the other half hit the real multipart file.
            String target = (i % 2 == 0) ? presentPath : "test/permits/mixed-missing-" + i;
            futures[i] = client.readFileRangeAsByteBufAsync(target, 0, blockSize, blockSize);
        }
        for (CompletableFuture<ByteBuf> f : futures) {
            ByteBuf b = f.get(30, TimeUnit.SECONDS);
            if (b != null) {
                ReferenceCountUtil.safeRelease(b);
            }
        }
        assertEquals("every reservation released regardless of hit/miss",
                max, client.availableInflightReadBytes());
    }

    /**
     * Error paths must not just net out across the aggregate — each
     * individual RPC must release its reservation before the next starts,
     * or a tiny budget would deadlock after the first failure. Uses a
     * budget just big enough for one read to prove release-on-failure is
     * immediate.
     */
    @Test
    public void testInflightReadBytes_smallBudgetSurvivesRepeatedNotFound() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        long budget = RemoteFileServiceClient.DEFAULT_BLOCK_SIZE;
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_READ_BYTES, budget);
        try (RemoteFileServiceClient tuned = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()), cfg)) {
            for (int i = 0; i < 5; i++) {
                ByteBuf buf = tuned.readFileAsByteBufAsync(
                        "test/permits/absent-" + i).get(30, TimeUnit.SECONDS);
                assertNull(buf);
                assertEquals("budget restored after each not-found",
                        budget, tuned.availableInflightReadBytes());
            }
        }
    }

    // ---------------------------------------------------------------------
    // Issue #468 — in-flight write-byte budget
    //
    // Symmetric to the issue #246 read-byte budget above: every
    // writeFile / writeFileBlock / writeMultipartFile call reserves bytes
    // from a Semaphore before launching the RPC, so a multipart compaction
    // write cannot fan out hundreds of in-flight blocks at once and starve
    // search reads sharing the same Netty event-loop pool / file server.
    // ---------------------------------------------------------------------

    /**
     * Default construction exposes the documented default write-byte budget
     * ({@link RemoteFileServiceClient#DEFAULT_CLIENT_MAX_INFLIGHT_WRITE_BYTES}).
     */
    @Test
    public void testInflightWriteBytes_defaultConfig() {
        assertEquals("default max inflight write bytes",
                RemoteFileServiceClient.DEFAULT_CLIENT_MAX_INFLIGHT_WRITE_BYTES,
                client.maxInflightWriteBytes());
        assertEquals("all bytes available at steady state",
                RemoteFileServiceClient.DEFAULT_CLIENT_MAX_INFLIGHT_WRITE_BYTES,
                client.availableInflightWriteBytes());
    }

    /**
     * A custom {@code remote.file.client.max.inflight.write.bytes} config
     * value is honoured, and {@code availableInflightWriteBytes()} starts
     * equal to the cap before any write is issued.
     */
    @Test
    public void testInflightWriteBytes_customConfig() throws Exception {
        long customBytes = 16L * 1024 * 1024; // 16 MiB
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES, customBytes);
        try (RemoteFileServiceClient tuned = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()), cfg)) {
            assertEquals("configured cap is applied", customBytes, tuned.maxInflightWriteBytes());
            assertEquals("all bytes available at idle",
                    customBytes, tuned.availableInflightWriteBytes());
        }
    }

    /** A non-positive byte budget is rejected at construction. */
    @Test
    public void testInflightWriteBytes_zeroConfigRejected() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES, 0L);
        try {
            new RemoteFileServiceClient(
                    Arrays.asList("localhost:" + server.getPort()), cfg).close();
            fail("expected IllegalArgumentException for non-positive byte budget");
        } catch (IllegalArgumentException expected) {
            assertTrue("message names the property",
                    expected.getMessage().contains(
                            RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES));
        }
    }

    /**
     * A byte budget below {@code blockSize} is rejected: a single full-block
     * write would immediately deadlock.
     */
    @Test
    public void testInflightWriteBytes_belowBlockSizeRejected() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_BLOCK_SIZE, 4096);
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES, 2048L);
        try {
            new RemoteFileServiceClient(
                    Arrays.asList("localhost:" + server.getPort()), cfg).close();
            fail("expected IllegalArgumentException when budget < blockSize");
        } catch (IllegalArgumentException expected) {
            assertTrue("message names the properties",
                    expected.getMessage().contains(
                            RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES));
        }
    }

    /**
     * After a successful {@code writeFile(byte[])} the byte reservation is
     * released — a subsequent call sees the full budget again.
     */
    @Test
    public void testInflightWriteBytes_releasedAfterWriteFile() throws Exception {
        String path = "test/permits/write/file.bin";
        long before = client.availableInflightWriteBytes();
        client.writeFile(path, "payload".getBytes(StandardCharsets.UTF_8));
        long after = client.availableInflightWriteBytes();
        assertEquals("reservation is released on completion", before, after);
    }

    /**
     * After a successful {@code writeFile(ByteBuf)} the byte reservation
     * is released — proves the ByteBuf overload also wires the
     * acquire/release pair.
     */
    @Test
    public void testInflightWriteBytes_releasedAfterWriteFileByteBuf() throws Exception {
        String path = "test/permits/write/file-bytebuf.bin";
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(64);
        try {
            buf.writeBytes("payload".getBytes(StandardCharsets.UTF_8));
            long before = client.availableInflightWriteBytes();
            client.writeFile(path, buf);
            long after = client.availableInflightWriteBytes();
            assertEquals("reservation is released on completion", before, after);
        } finally {
            ReferenceCountUtil.safeRelease(buf);
        }
    }

    /**
     * After a {@code writeFileBlock(byte[])} the reservation is released —
     * exercises the hot path that issue #468 targets.
     */
    @Test
    public void testInflightWriteBytes_releasedAfterWriteFileBlock() throws Exception {
        String path = "test/permits/write/block.bin";
        byte[] block = new byte[4096];
        long before = client.availableInflightWriteBytes();
        client.writeFileBlock(path, 0L, block);
        long after = client.availableInflightWriteBytes();
        assertEquals("reservation is released on completion", before, after);
    }

    /**
     * After a {@code writeMultipartFile} of N blocks the reservation is
     * released. This is the path used by Phase B of compaction —
     * the primary regression gate for issue #468.
     */
    @Test
    public void testInflightWriteBytes_releasedAfterWriteMultipart() throws Exception {
        String path = "test/permits/write/multipart.bin";
        int blockSize = 4096;
        byte[] content = new byte[blockSize * 4];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) (i % 256);
        }
        long before = client.availableInflightWriteBytes();
        long written = client.writeMultipartFile(path,
                new ByteArrayInputStream(content), blockSize);
        assertEquals("multipart wrote all bytes", content.length, written);
        long after = client.availableInflightWriteBytes();
        assertEquals("every block reservation released after multipart write",
                before, after);
    }

    /**
     * A burst of {@code writeFileBlock} calls issued sequentially from the
     * test thread must never reveal a negative budget and must return the
     * full cap by the time every call settles. The acquires happen serially
     * on the calling thread; the underlying RPC completions overlap on the
     * Netty event-loop pool. Symmetric to the read-side serial-burst test.
     * For the truly-concurrent acquisition path see
     * {@link #testInflightWriteBytes_concurrentMultipartWritersShareBudget}.
     */
    @Test
    public void testInflightWriteBytes_serialBurstReturnsReservations() throws Exception {
        long max = client.maxInflightWriteBytes();
        int burst = 32;
        int payloadSize = 4096;
        @SuppressWarnings("unchecked")
        CompletableFuture<Void>[] futures = new CompletableFuture[burst];
        for (int i = 0; i < burst; i++) {
            byte[] payload = new byte[payloadSize];
            futures[i] = client.writeFileBlockAsync(
                    "test/permits/write/burst-" + i, 0L, payload);
        }
        for (CompletableFuture<Void> f : futures) {
            f.get(30, TimeUnit.SECONDS);
            assertTrue("available bytes must never exceed cap",
                    client.availableInflightWriteBytes() <= max);
            assertTrue("available bytes must never go negative",
                    client.availableInflightWriteBytes() >= 0);
        }
        assertEquals("all bytes returned at the end of the burst",
                max, client.availableInflightWriteBytes());
    }

    /**
     * A small byte cap that still admits one full-block write restores the
     * budget after the write settles.
     */
    @Test
    public void testInflightWriteBytes_smallBudgetRoundTrip() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        // 4 MiB budget — exactly one default block. Any smaller and the
        // constructor refuses (see testInflightWriteBytes_belowBlockSizeRejected).
        long budget = RemoteFileServiceClient.DEFAULT_BLOCK_SIZE;
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES, budget);
        try (RemoteFileServiceClient tuned = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()), cfg)) {
            assertEquals(budget, tuned.availableInflightWriteBytes());
            tuned.writeFile("test/permits/write/single.bin",
                    "one".getBytes(StandardCharsets.UTF_8));
            assertEquals("budget restored after single write completes",
                    budget, tuned.availableInflightWriteBytes());
        }
    }

    /**
     * When the remote file service is unreachable the underlying RPC
     * eventually fails — each attempt must release its reservation so the
     * budget returns to the configured cap. Writes are not retried by the
     * client (see {@code RemoteFileServiceClient.writeFileAsync}'s
     * "Writes are not idempotent — no retry." comment), so a single attempt
     * is enough; we still want to prove the failure-path release.
     */
    @Test
    public void testInflightWriteBytes_releasedAfterBackendUnreachable() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 1);
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_TIMEOUT, 2L);
        // Point at a port where nothing is listening.
        try (RemoteFileServiceClient broken = new RemoteFileServiceClient(
                Arrays.asList("localhost:1"), cfg)) {
            long before = broken.availableInflightWriteBytes();
            try {
                broken.writeFileBlockAsync("some/path", 0L, new byte[4096])
                        .get(60, TimeUnit.SECONDS);
                fail("expected the write to fail against an unreachable backend");
            } catch (java.util.concurrent.ExecutionException expected) {
                // propagated from sendRequest
            }
            assertEquals("reservation restored after backend-unreachable",
                    before, broken.availableInflightWriteBytes());
        }
    }

    /**
     * The behaviour the bug fix promises: with a budget tight enough to
     * admit only one block at a time, a multipart write whose total
     * payload exceeds the budget still completes — the driver thread
     * naturally serialises by blocking on {@code acquire()} and unblocking
     * each time a previous block's RPC completes and releases its bytes.
     * This is the test that regresses if the acquire/release plumbing
     * in {@code writeFileBlockAsync} ever leaks or is bypassed.
     */
    @Test
    public void testInflightWriteBytes_smallBudgetSerializesMultipartBlocks() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        // Use a small block size so the test stays cheap; budget == one block.
        int blockSize = 4096;
        long budget = blockSize;
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_BLOCK_SIZE, blockSize);
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES, budget);
        try (RemoteFileServiceClient tuned = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()), cfg)) {
            // Eight blocks — eight forced acquire/release round-trips.
            byte[] content = new byte[blockSize * 8];
            for (int i = 0; i < content.length; i++) {
                content[i] = (byte) i;
            }
            long written = tuned.writeMultipartFile(
                    "test/permits/write/serialised.bin",
                    new ByteArrayInputStream(content), blockSize);
            assertEquals("multipart wrote all bytes despite the tight budget",
                    content.length, written);
            assertEquals("budget restored after the serialised multipart write",
                    budget, tuned.availableInflightWriteBytes());
            // Round-trip: read back to assert the file is structurally sound.
            byte[] firstBlock = tuned.readFileRange(
                    "test/permits/write/serialised.bin", 0L, blockSize, blockSize);
            assertNotNull(firstBlock);
            assertEquals(blockSize, firstBlock.length);
        }
    }

    /**
     * Empty-payload writes (issue #100's fast path: an empty multipart file
     * still creates block 0 with an empty buffer) must not decrement the
     * write-byte budget — otherwise an idle workload that creates many
     * empty marker files would exhaust the cap. Specifically tests the
     * {@code Unpooled.EMPTY_BUFFER} call site in
     * {@code RemoteFileDataStorageManager.writeAsMultipart}.
     */
    @Test
    public void testInflightWriteBytes_emptyPayloadSkipsReservation() throws Exception {
        long before = client.availableInflightWriteBytes();
        client.writeFileBlock("test/permits/write/empty.bin", 0L,
                Unpooled.EMPTY_BUFFER);
        long after = client.availableInflightWriteBytes();
        assertEquals("empty payload neither acquires nor leaks", before, after);
    }

    /**
     * The byte budget is per-client, not per-call. Two concurrent
     * {@code writeMultipartFile} drivers must serialise on the shared
     * semaphore when their combined demand exceeds the cap.
     *
     * <p>Each thread writes a 32-block file via {@code writeMultipartFile}
     * with {@code blockSize} = 4 KiB and a per-client cap of 4 blocks.
     * The combined steady-state demand (2 × 32 = 64 acquires) must be
     * admitted in batches of at most 4 blocks across both writers. A
     * {@link CountDownLatch} fences out a degenerate ordering where one
     * driver acquires + releases all 32 blocks before the other has even
     * started; both threads only call {@code writeMultipartFile} after the
     * latch reaches zero.
     *
     * <p>A third "observer" thread polls
     * {@link RemoteFileServiceClient#availableInflightWriteBytes()} in a
     * tight loop while the writers are running and records the minimum
     * value seen. If the budget were per-call (each call had its own
     * fresh {@code Semaphore(budget)}, with the client-level semaphore
     * never debited), the observer would always see the full cap. The
     * test fails if the observer never sees the available bytes drop
     * below {@code budget} — that is the regression gate that catches a
     * bug where the budget is mistakenly reset to per-call (issue #477).
     */
    @Test(timeout = 120_000)
    public void testInflightWriteBytes_concurrentMultipartWritersShareBudget() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        int blockSize = 4096;
        long budget = 4L * blockSize;
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_BLOCK_SIZE, blockSize);
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES, budget);
        try (RemoteFileServiceClient tuned = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()), cfg)) {
            int blocksPerFile = 32;
            int writers = 2;
            CountDownLatch startLatch = new CountDownLatch(writers);
            AtomicLong observedMinDuringWrites = new AtomicLong(budget + 1); // sentinel
            AtomicBoolean observerRunning = new AtomicBoolean(true);
            // writers + 1 for the observer.
            ExecutorService exec = Executors.newFixedThreadPool(writers + 1);
            try {
                Future<?> observer = exec.submit(() -> {
                    while (observerRunning.get()) {
                        long now = tuned.availableInflightWriteBytes();
                        observedMinDuringWrites.accumulateAndGet(now, Math::min);
                        Thread.onSpinWait();
                    }
                });

                @SuppressWarnings("unchecked")
                Future<Long>[] futures = new Future[writers];
                for (int i = 0; i < writers; i++) {
                    final int writerId = i;
                    futures[i] = exec.submit(() -> {
                        // Both threads count down then await — neither starts
                        // its multipart write until both threads exist, so
                        // contention is real.
                        startLatch.countDown();
                        if (!startLatch.await(30, TimeUnit.SECONDS)) {
                            throw new IllegalStateException(
                                    "writer rendezvous timed out");
                        }
                        byte[] content = new byte[blockSize * blocksPerFile];
                        for (int j = 0; j < content.length; j++) {
                            content[j] = (byte) ((writerId << 4) | (j & 0x0f));
                        }
                        return tuned.writeMultipartFile(
                                "test/permits/write/concurrent-" + writerId + ".bin",
                                new ByteArrayInputStream(content), blockSize);
                    });
                }
                long expected = (long) blockSize * blocksPerFile;
                for (int i = 0; i < writers; i++) {
                    assertEquals("writer " + i + " wrote the expected bytes",
                            expected, futures[i].get(60, TimeUnit.SECONDS).longValue());
                }
                // Stop observer cleanly before reading its result.
                observerRunning.set(false);
                observer.get(10, TimeUnit.SECONDS);

                long observedMin = observedMinDuringWrites.get();
                // Regression gate for the per-call budget bug: if the
                // budget were per-call rather than per-client, the observer
                // would never see the shared semaphore drop below cap.
                assertTrue("observer must have seen contention "
                                + "(min observed=" + observedMin + ", budget=" + budget
                                + "); the per-client byte budget appears to have "
                                + "regressed to per-call",
                        observedMin < budget);
                assertTrue("observed budget never went negative; observed="
                                + observedMin,
                        observedMin >= 0);
                assertEquals("budget fully restored after both writers finish",
                        budget, tuned.availableInflightWriteBytes());
            } finally {
                observerRunning.set(false);
                exec.shutdown();
                exec.awaitTermination(30, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * An interrupted caller blocked in
     * {@code Semaphore.acquireUninterruptibly} keeps waiting (no
     * {@code InterruptedException} leaks out), the budget is correctly
     * restored when the call completes, and the interrupt status is
     * preserved (issue #477, mirrors the {@code acquireUninterruptibly}
     * contract documented at the call site in
     * {@code RemoteFileServiceClient}).
     *
     * <p>This test exercises the saturated path explicitly: it drains
     * the write-byte semaphore via reflection (the field is
     * {@code private} but the test sits in the same package, so this is
     * a deliberate package-private back door, not a contract bypass)
     * and only then issues the interrupted {@code writeFileBlockAsync}
     * call. With the budget empty, the helper falls through from
     * {@code tryAcquire} to {@code acquireUninterruptibly} and parks
     * the caller — exactly the slow-path code under test. The test
     * waits for the caller to enter {@code Thread.State.WAITING}
     * before releasing the drained permits, so a regression that
     * makes the helper interrupt-sensitive (e.g. switching to plain
     * {@code acquire()}) would surface as an early thread death with
     * the captured throwable populated.
     *
     * <p>A benign warm-up write is issued first so the test exercises
     * the budget helper rather than Netty's lazy connect-and-sync path
     * ({@code NettyConnector.connectUsingNetwork} calls {@code .sync()}
     * on the bootstrap promise, which propagates
     * {@code InterruptedException} out as an IOException — a different
     * code path from the one this test pins).
     */
    @Test(timeout = 120_000)
    public void testInflightWriteBytes_acquireSurvivesInterrupt() throws Exception {
        // Warm-up: open the write-plane Netty channel so the test
        // exercises the budget helper, not Netty's connect-and-sync path.
        client.writeFileBlock("test/permits/write/interrupt-warmup.bin", 0L,
                new byte[1]);

        // Reflectively access the write-byte semaphore so we can drain
        // it independently of the public API. Same-package test, so
        // setAccessible is a deliberate package-private back door.
        Field semField = RemoteFileServiceClient.class.getDeclaredField("inflightWriteBytes");
        semField.setAccessible(true);
        Semaphore writeSem = (Semaphore) semField.get(client);
        int drained = writeSem.drainPermits();
        assertTrue("drained at least the configured cap",
                drained >= client.maxInflightWriteBytes()
                        || drained == Integer.MAX_VALUE);

        AtomicReference<Throwable> threadError = new AtomicReference<>();
        AtomicReference<Boolean> interruptStatusAfterCall = new AtomicReference<>();
        AtomicReference<CompletableFuture<Void>> futureRef = new AtomicReference<>();
        CountDownLatch aboutToCall = new CountDownLatch(1);
        Thread caller = new Thread(() -> {
            try {
                Thread.currentThread().interrupt();
                aboutToCall.countDown();
                CompletableFuture<Void> future = client.writeFileBlockAsync(
                        "test/permits/write/interrupt.bin", 0L, new byte[4096]);
                futureRef.set(future);
                interruptStatusAfterCall.set(Thread.currentThread().isInterrupted());
                // Catch Throwable so AssertionError or any other Error from
                // the helper still surfaces in threadError instead of being
                // silently swallowed by the daemon thread (per CLAUDE.md
                // exception-handling guidance).
            } catch (Throwable t) {
                threadError.set(t);
            } finally {
                Thread.interrupted(); // never leak interrupt state from this thread
            }
        }, "interrupt-test-caller");
        caller.setDaemon(true);
        caller.start();

        try {
            assertTrue("caller reached writeFileBlockAsync entry",
                    aboutToCall.await(10, TimeUnit.SECONDS));
            // Wait for the caller to park inside acquireUninterruptibly.
            // LockSupport.park() puts the thread into Thread.State.WAITING.
            long deadlineNanos = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(10);
            while (caller.getState() != Thread.State.WAITING
                    && caller.getState() != Thread.State.TIMED_WAITING) {
                if (System.nanoTime() > deadlineNanos) {
                    fail("caller did not enter WAITING state within 10s; "
                            + "current state=" + caller.getState()
                            + ", error=" + threadError.get());
                }
                Thread.sleep(10);
            }
            // Sanity-check: caller hasn't already returned, and no
            // exception escaped the budget helper.
            assertTrue("caller still alive (parked on acquireUninterruptibly)",
                    caller.isAlive());
            assertNull("budget helper must not throw on an interrupted thread",
                    threadError.get());
            assertNull("future must not be set yet (caller still parked)",
                    futureRef.get());
        } finally {
            // Always release the drained permits so the caller can
            // proceed even if an assertion failed above.
            writeSem.release(drained);
        }

        caller.join(10_000);
        assertFalse("caller terminated within 10s after permits restored",
                caller.isAlive());
        assertNull("acquireUninterruptibly path did not throw",
                threadError.get());
        assertEquals("interrupt status was preserved across acquireUninterruptibly",
                Boolean.TRUE, interruptStatusAfterCall.get());

        CompletableFuture<Void> future = futureRef.get();
        assertNotNull("writeFileBlockAsync returned a future", future);
        future.get(30, TimeUnit.SECONDS);
        assertEquals("budget fully restored after the interrupted call",
                client.maxInflightWriteBytes(),
                client.availableInflightWriteBytes());
    }

    // -----------------------------------------------------------------
    // Issue #523 — oversized single-shot write (payload > cap) must not
    // deadlock and must restore the budget exactly.
    // -----------------------------------------------------------------

    /**
     * Regression test for issue #523: a single {@code writeFile} whose
     * payload exceeds the configured inflight-write cap used to block
     * forever on {@code Semaphore.acquireUninterruptibly(bytes)} because a
     * semaphore can never grant more permits than its initial count.
     *
     * <p>The fix caps the acquisition to {@code min(bytes, writePermits)} and
     * acquires on the slow path in {@link RemoteFileServiceClient#blockSize}-
     * sized chunks. This test uses a cap equal to exactly one block (4 MiB)
     * and a payload of two blocks (8 MiB); before the fix the test would hang
     * until the 30-second {@code @Test(timeout)} fires.
     */
    @Test(timeout = 30_000)
    public void testInflightWriteBytes_oversizedWriteDoesNotDeadlock() throws Exception {
        int blockSize = RemoteFileServiceClient.DEFAULT_BLOCK_SIZE;
        long cap = blockSize; // 1 block = 4 MiB cap
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_BLOCK_SIZE, blockSize);
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES, cap);
        try (RemoteFileServiceClient tuned = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()), cfg)) {
            // 2× cap — with the old code this would deadlock:
            // acquireUninterruptibly(2*cap) on a semaphore with cap permits.
            byte[] payload = new byte[blockSize * 2];
            Arrays.fill(payload, (byte) 0xAB);
            tuned.writeFile("test/permits/write/oversized-nodead.bin", payload);
            assertEquals("budget restored to cap (not to raw bytes) after oversized write",
                    cap, tuned.availableInflightWriteBytes());
        }
    }

    /**
     * After an oversized {@code writeFile} (payload &gt; cap) completes, the
     * inflight-write budget must be restored to exactly {@code cap} — not to
     * the raw payload size. Before the fix, {@code reserveInflightWriteBytes}
     * released {@code bytes} permits (the raw argument) rather than the number
     * actually acquired ({@code min(bytes, writePermits)}), which would have
     * over-inflated the semaphore and made subsequent reads of
     * {@link RemoteFileServiceClient#availableInflightWriteBytes()} return a
     * value larger than the configured cap.
     */
    @Test(timeout = 30_000)
    public void testInflightWriteBytes_oversizedWriteReleasesExactAcquired() throws Exception {
        int blockSize = RemoteFileServiceClient.DEFAULT_BLOCK_SIZE;
        long cap = blockSize; // 1 block = 4 MiB cap
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_BLOCK_SIZE, blockSize);
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES, cap);
        try (RemoteFileServiceClient tuned = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()), cfg)) {
            assertEquals("budget starts at cap", cap, tuned.availableInflightWriteBytes());
            // Write 3× cap to amplify any over-inflation: if release used raw
            // bytes instead of acquired, available would jump to 3*cap after
            // the first call and 6*cap after the second.
            byte[] payload = new byte[blockSize * 3];
            Arrays.fill(payload, (byte) 0xCD);
            tuned.writeFile("test/permits/write/oversized-release1.bin", payload);
            assertEquals("budget must equal cap after first oversized write (not 3*cap)",
                    cap, tuned.availableInflightWriteBytes());
            tuned.writeFile("test/permits/write/oversized-release2.bin", payload);
            assertEquals("budget must equal cap after second oversized write (not 6*cap)",
                    cap, tuned.availableInflightWriteBytes());
        }
    }
}
