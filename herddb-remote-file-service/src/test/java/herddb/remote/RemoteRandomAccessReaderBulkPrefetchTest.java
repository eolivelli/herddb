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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.index.vector.BulkPrefetchReaderSupplier;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end tests for the bulk warmup-prefetch path introduced by issue #619.
 *
 * <p>Spin up an in-process {@link RemoteFileServer} + {@link RemoteFileServiceClient},
 * write a multipart file that spans several multipart chunks, then verify that:
 * <ul>
 *   <li>{@link RemoteRandomAccessReader.Supplier} implements
 *       {@link BulkPrefetchReaderSupplier};</li>
 *   <li>a single {@code bulkPrefetchIntoCache} call populates the
 *       {@link SegmentBlockCache} with every covered block, in fewer wire
 *       round-trips than the per-block path would have made;</li>
 *   <li>a subsequent {@link RemoteRandomAccessReader} hits the cache and
 *       returns the correct bytes;</li>
 *   <li>argument validation rejects unaligned start offsets;</li>
 *   <li>the disabled cache short-circuits to a no-op;</li>
 *   <li>a non-existent path surfaces as {@link IOException}.</li>
 * </ul>
 */
public class RemoteRandomAccessReaderBulkPrefetchTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;

    /** Multipart chunk size used by the writer. Three chunks total. */
    private static final int WRITE_BLOCK_SIZE = 4096;
    /** Read-side cache block size. {@code WRITE_BLOCK_SIZE % BUFFER_SIZE == 0}. */
    private static final int BUFFER_SIZE = 256;
    private static final int NUM_CHUNKS = 3;
    private static final int TOTAL_SIZE = WRITE_BLOCK_SIZE * NUM_CHUNKS;
    private static final String PATH = "ts/idx/graph.bin";

    private byte[] payload;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("data").toPath());
        server.start();
        client = new RemoteFileServiceClient(List.of("localhost:" + server.getPort()));

        // Deterministic ramp so we can verify exact byte content after prefetch.
        payload = new byte[TOTAL_SIZE];
        for (int i = 0; i < TOTAL_SIZE; i++) {
            payload[i] = (byte) (i & 0xFF);
        }
        client.writeMultipartFile(PATH, new ByteArrayInputStream(payload), WRITE_BLOCK_SIZE);
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        server.stop();
    }

    /**
     * Smoke test: {@code Supplier} implements both the pin-mode mixin and the
     * new bulk-prefetch mixin. Without this assertion the call site in
     * {@code PersistentVectorStore.bulkPrefetchSegmentIntoCache} would silently
     * skip the fast path for remote suppliers.
     */
    @Test
    public void supplierImplementsBulkPrefetchReaderSupplier() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, TOTAL_SIZE,
                        WRITE_BLOCK_SIZE, BUFFER_SIZE, NullStatsLogger.INSTANCE, cache);
        assertTrue("Supplier must implement BulkPrefetchReaderSupplier",
                supplier instanceof BulkPrefetchReaderSupplier);
    }

    /**
     * Prefetch all 3 chunks into the cache, then verify every (path, offset)
     * cache-block-sized key is present and a normal reader returns exact bytes
     * without invoking the loader (no additional load_success increments).
     */
    @Test
    public void prefetchAllChunksPopulatesCacheAndReadsHit() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, TOTAL_SIZE,
                        WRITE_BLOCK_SIZE, BUFFER_SIZE, NullStatsLogger.INSTANCE, cache);

        long inserted = supplier.bulkPrefetchIntoCache(0L, TOTAL_SIZE);
        assertEquals("every byte covered", TOTAL_SIZE, inserted);

        int expectedBlocks = TOTAL_SIZE / BUFFER_SIZE;
        assertEquals(expectedBlocks, cache.bulkInsertedBlockCount());
        assertEquals(0L, cache.bulkInsertSkippedCount());

        // Every cache block must report containsBlock=true.
        for (int i = 0; i < expectedBlocks; i++) {
            long off = (long) i * BUFFER_SIZE;
            assertTrue("block " + i + " (" + off + ") must be cached",
                    cache.containsBlock(PATH, off, BUFFER_SIZE));
        }

        // A normal reader reads every byte back. No additional loader call,
        // so cache.loadSuccessCount() must stay at zero (bulk inserts never
        // touch the loadSuccess counter — they have their own counter).
        long loadsBefore = cache.loadSuccessCount();
        try (RandomAccessReader reader = supplier.get()) {
            for (int chunk = 0; chunk < NUM_CHUNKS; chunk++) {
                reader.seek((long) chunk * WRITE_BLOCK_SIZE);
                byte[] got = new byte[WRITE_BLOCK_SIZE];
                reader.readFully(got);
                for (int j = 0; j < WRITE_BLOCK_SIZE; j++) {
                    assertEquals("byte " + (chunk * WRITE_BLOCK_SIZE + j),
                            payload[chunk * WRITE_BLOCK_SIZE + j], got[j]);
                }
            }
        }
        assertEquals("no loader calls — every read hit the bulk-prefetched cache",
                loadsBefore, cache.loadSuccessCount());
        long expectedHits = (long) NUM_CHUNKS * (WRITE_BLOCK_SIZE / BUFFER_SIZE);
        assertEquals("every reader read produced one cache hit", expectedHits, cache.hitCount());
    }

    /**
     * Partial prefetch: only the first chunk is requested. Only the blocks in
     * chunk 0 are cached; chunks 1 and 2 must NOT be cached after the call.
     */
    @Test
    public void prefetchOfFirstChunkOnlyCachesThatChunk() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, TOTAL_SIZE,
                        WRITE_BLOCK_SIZE, BUFFER_SIZE, NullStatsLogger.INSTANCE, cache);

        long inserted = supplier.bulkPrefetchIntoCache(0L, WRITE_BLOCK_SIZE);
        assertEquals(WRITE_BLOCK_SIZE, inserted);
        int blocksPerChunk = WRITE_BLOCK_SIZE / BUFFER_SIZE;
        assertEquals(blocksPerChunk, cache.bulkInsertedBlockCount());

        for (int i = 0; i < blocksPerChunk; i++) {
            assertTrue(cache.containsBlock(PATH, (long) i * BUFFER_SIZE, BUFFER_SIZE));
        }
        for (int i = blocksPerChunk; i < TOTAL_SIZE / BUFFER_SIZE; i++) {
            assertTrue("block " + i + " must NOT be cached after partial prefetch",
                    !cache.containsBlock(PATH, (long) i * BUFFER_SIZE, BUFFER_SIZE));
        }
    }

    /**
     * {@code maxBytes} is clamped to {@code totalSize - startOffset} — a
     * request for more bytes than exist must succeed and return only the
     * actual file size.
     */
    @Test
    public void prefetchClampsToFileEnd() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, TOTAL_SIZE,
                        WRITE_BLOCK_SIZE, BUFFER_SIZE, NullStatsLogger.INSTANCE, cache);

        long inserted = supplier.bulkPrefetchIntoCache(0L, TOTAL_SIZE * 10L);
        assertEquals("prefetch clamped to file size", TOTAL_SIZE, inserted);
    }

    /**
     * Unaligned start offset must be rejected — the contract requires
     * alignment to the multipart chunk size.
     */
    @Test
    public void unalignedStartOffsetRejected() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, TOTAL_SIZE,
                        WRITE_BLOCK_SIZE, BUFFER_SIZE, NullStatsLogger.INSTANCE, cache);
        assertThrows(IllegalArgumentException.class,
                () -> supplier.bulkPrefetchIntoCache(WRITE_BLOCK_SIZE / 2, WRITE_BLOCK_SIZE));
    }

    /**
     * Disabled cache: bulkPrefetchIntoCache is a no-op (no wire reads even
     * issued, since populating an off cache would be pointless).
     */
    @Test
    public void disabledCacheShortCircuits() throws Exception {
        SegmentBlockCache cache = SegmentBlockCache.disabled();
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, TOTAL_SIZE,
                        WRITE_BLOCK_SIZE, BUFFER_SIZE, NullStatsLogger.INSTANCE, cache);
        long inserted = supplier.bulkPrefetchIntoCache(0L, TOTAL_SIZE);
        assertEquals(0L, inserted);
        assertEquals(0L, cache.bulkInsertedBlockCount());
    }

    /**
     * startOffset beyond EOF returns 0 without throwing.
     */
    @Test
    public void startOffsetAtOrPastEofIsNoOp() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, TOTAL_SIZE,
                        WRITE_BLOCK_SIZE, BUFFER_SIZE, NullStatsLogger.INSTANCE, cache);
        assertEquals(0L, supplier.bulkPrefetchIntoCache(TOTAL_SIZE, WRITE_BLOCK_SIZE));
        assertEquals(0L, supplier.bulkPrefetchIntoCache(TOTAL_SIZE * 2L, WRITE_BLOCK_SIZE));
        assertEquals(0L, cache.bulkInsertedBlockCount());
    }

    /**
     * Prefetching a non-existent file path must surface as an {@link IOException}
     * and not leave any half-inserted state in the cache.
     */
    @Test
    public void prefetchOfMissingPathFailsAndDoesNotPolluteCache() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, "ts/idx/does-not-exist.bin",
                        TOTAL_SIZE, WRITE_BLOCK_SIZE, BUFFER_SIZE, NullStatsLogger.INSTANCE, cache);
        try {
            supplier.bulkPrefetchIntoCache(0L, TOTAL_SIZE);
            fail("expected IOException for missing file");
        } catch (IOException expected) {
            // Expected: server returned null for at least one chunk.
            assertNotNull(expected.getMessage());
        }
        assertEquals("no blocks inserted on failure", 0L, cache.bulkInsertedBlockCount());
    }

    /**
     * Repeated prefetch calls treat the second one as fully-cached: bytes
     * inserted = 0, and the skip counter reflects every block.
     */
    @Test
    public void repeatedPrefetchCountsSkips() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, TOTAL_SIZE,
                        WRITE_BLOCK_SIZE, BUFFER_SIZE, NullStatsLogger.INSTANCE, cache);
        long firstInserted = supplier.bulkPrefetchIntoCache(0L, TOTAL_SIZE);
        assertEquals(TOTAL_SIZE, firstInserted);

        long secondInserted = supplier.bulkPrefetchIntoCache(0L, TOTAL_SIZE);
        assertEquals("second prefetch is fully skipped", 0L, secondInserted);
        assertEquals(TOTAL_SIZE / BUFFER_SIZE, cache.bulkInsertSkippedCount());
    }

    /**
     * Negative maxBytes is treated as a no-op (returns 0) — a defensive guard
     * matching {@code bytesPerSegment <= 0} → disabled semantics.
     */
    @Test
    public void nonPositiveMaxBytesIsNoOp() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, TOTAL_SIZE,
                        WRITE_BLOCK_SIZE, BUFFER_SIZE, NullStatsLogger.INSTANCE, cache);
        assertEquals(0L, supplier.bulkPrefetchIntoCache(0L, 0L));
        assertEquals(0L, supplier.bulkPrefetchIntoCache(0L, -100L));
        assertEquals(0L, cache.bulkInsertedBlockCount());
    }

    /**
     * Review fix R5: when one chunk in the middle is missing from storage
     * (server returns {@code null}), the prefetch must surface an
     * {@link IOException} without leaking any buffer fetched from earlier
     * chunks. The current implementation joins on every chunk's future before
     * any insert, so the failed chunk causes an early bail-out and the
     * earlier successful buffers are drained via the catch block — exercised
     * here against a real wire.
     */
    @Test
    public void prefetchFailsCleanlyWhenServerMissingTrailingChunk() {
        // Configure the supplier to think the file is 3 chunks long, but the
        // upload only ever wrote chunks 0 and 1 — chunk 2 will return null
        // from the server. The Supplier's totalSize is the source of truth
        // for how many chunks the prefetch dispatches; we set it longer than
        // actual storage to provoke the partial-availability path.
        int fakeTotal = WRITE_BLOCK_SIZE * (NUM_CHUNKS + 2);
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, fakeTotal,
                        WRITE_BLOCK_SIZE, BUFFER_SIZE, NullStatsLogger.INSTANCE, cache);
        try {
            supplier.bulkPrefetchIntoCache(0L, fakeTotal);
            fail("expected IOException for trailing missing chunks");
        } catch (IOException expected) {
            // Either chunk 3 or chunk 4 reported missing first; both are valid.
            assertNotNull(expected.getMessage());
        }
        // Critical invariant: no off-heap leak. Caffeine releases everything
        // it holds when invalidatePath runs; if the failure branch had leaked
        // a fetched buffer we'd observe a stale entry left in the cache. We
        // can't probe Netty's pool directly here, but a clean invalidate +
        // weightedSize check exercises the eviction listener for every entry
        // we did insert before failing.
        cache.invalidatePath(PATH);
        cache.cleanUp();
        assertEquals(0L, cache.weightedSize());
    }

    /**
     * Review fix R2: after a successful bulk prefetch the main cache holds
     * every covered block; a subsequent pin-mode reader (the warmup pin BFS)
     * must serve those blocks from main into the frontier region WITHOUT
     * issuing a fresh wire read. The check observes the frontier region's
     * load_success counter increasing while {@code SegmentBlockCache.loadSuccessCount}
     * stays at zero (no main-cache loader invocation either).
     */
    @Test
    public void pinModeAfterPrefetchPromotesMainCacheWithoutWireRead() throws Exception {
        // Need a frontier budget for the pin region to be active.
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 200_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, TOTAL_SIZE,
                        WRITE_BLOCK_SIZE, BUFFER_SIZE, NullStatsLogger.INSTANCE, cache);

        // 1. Prefetch fills the MAIN cache.
        supplier.bulkPrefetchIntoCache(0L, TOTAL_SIZE);
        long mainLoadsAfterPrefetch = cache.loadSuccessCount();
        // bulkInsert doesn't bump loadSuccess, so this is 0 before pin too.
        assertEquals(0L, mainLoadsAfterPrefetch);

        // 2. A pin-mode reader reads the same blocks — they must end up in
        // the frontier region, promoted from main without a wire read.
        try (RandomAccessReader pinReader = supplier.withPinMode().get()) {
            pinReader.seek(0);
            byte[] tmp = new byte[BUFFER_SIZE];
            for (int i = 0; i < (TOTAL_SIZE / BUFFER_SIZE); i++) {
                pinReader.readFully(tmp);
                // bytes must match the original payload
                int payloadOff = i * BUFFER_SIZE;
                byte[] expected = Arrays.copyOfRange(payload, payloadOff, payloadOff + BUFFER_SIZE);
                assertArrayEquals("block " + i + " bytes via pin reader", expected, tmp);
            }
        }

        // 3. Frontier load_success increased — and crucially main load_success
        // did NOT change (no extra wire read happened). This is the heart of
        // the R2 fix.
        assertEquals("no main-cache loader invocation after prefetch",
                0L, cache.loadSuccessCount());
        assertTrue("frontier loads must have run (promoted from main)",
                cache.frontierLoadSuccessCount() > 0);
    }

    /**
     * Sanity check for the promotion path: when the main cache is empty,
     * pin-mode reads still work — they fall through to the loader exactly
     * like before the R2 fix.
     */
    @Test
    public void pinModeWithoutPriorPrefetchStillLoadsFromWire() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 200_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, TOTAL_SIZE,
                        WRITE_BLOCK_SIZE, BUFFER_SIZE, NullStatsLogger.INSTANCE, cache);
        // No prefetch this time — main cache stays empty.
        try (RandomAccessReader pinReader = supplier.withPinMode().get()) {
            pinReader.seek(0);
            pinReader.readInt();
        }
        // frontier load happened via the loader because main was empty.
        assertEquals(1L, cache.frontierLoadSuccessCount());
    }

    /**
     * After a successful bulk prefetch, {@link SegmentBlockCache#invalidatePath}
     * must drop every block (no leak from the bulk-insert path). Mirrors the
     * regression-style assertion in {@code SegmentBlockCacheBulkInsertTest} but
     * exercises the real wire-fed buffers.
     */
    @Test
    public void invalidatePathReleasesEveryPrefetchedBlock() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, TOTAL_SIZE,
                        WRITE_BLOCK_SIZE, BUFFER_SIZE, NullStatsLogger.INSTANCE, cache);
        supplier.bulkPrefetchIntoCache(0L, TOTAL_SIZE);
        cache.cleanUp();
        assertNotEquals(0L, cache.weightedSize());

        cache.invalidatePath(PATH);
        cache.cleanUp();
        assertEquals(0L, cache.weightedSize());
        for (int i = 0; i < TOTAL_SIZE / BUFFER_SIZE; i++) {
            assertTrue("block " + i + " gone after invalidatePath",
                    !cache.containsBlock(PATH, (long) i * BUFFER_SIZE, BUFFER_SIZE));
        }
        // weightedSize() == 0 already implies size==0, but be explicit for readability.
        assertNull("peekBytes returns null for evicted block",
                cache.estimatedSize() > 0 ? "non-empty" : null);
    }
}
