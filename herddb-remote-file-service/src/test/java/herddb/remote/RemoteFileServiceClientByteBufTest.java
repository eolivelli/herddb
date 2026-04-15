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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
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
}
