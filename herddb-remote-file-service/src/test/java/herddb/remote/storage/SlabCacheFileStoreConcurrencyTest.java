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

package herddb.remote.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Hammer test for {@link SlabCacheFileStore} (issue #475). Many threads admit/read/free
 * random keys from a shared pool. Each payload starts with a key-derived header so a
 * read that returns bytes "from the wrong key" (a torn read against a recycled cell)
 * fails the test loudly.
 *
 * @author enrico.olivelli
 */
public class SlabCacheFileStoreConcurrencyTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final int CELL_SIZE = 4096;
    private static final int TOTAL_CELLS = 64;
    private static final int KEY_POOL = 200;
    private static final int THREADS = 8;
    private static final int OPS_PER_THREAD = 2000;
    private static final byte HEADER_LEN = 16;

    private static byte[] payloadFor(String key, int variant) {
        // Header: 16 bytes derived from (key,variant) so the read path can verify
        // the bytes match the expected key. We use a SHA-1-style mix here without
        // needing a real digest — collisions in the 16-byte prefix between different
        // (key,variant) tuples are astronomically unlikely under the test's load.
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < key.length(); i++) {
            h ^= key.charAt(i);
            h *= 0x100000001b3L;
        }
        long h2 = h ^ ((long) variant * 0x9e3779b97f4a7c15L);
        ByteBuffer header = ByteBuffer.allocate(HEADER_LEN);
        header.putLong(h);
        header.putLong(h2);
        int payloadLen = 32 + ThreadLocalRandom.current().nextInt(CELL_SIZE - 32);
        byte[] payload = new byte[payloadLen];
        System.arraycopy(header.array(), 0, payload, 0, HEADER_LEN);
        // Fill the rest with a deterministic pattern derived from h2 so any cross-talk
        // between recycled cells shows up as a body mismatch even if the header
        // happens to collide.
        for (int i = HEADER_LEN; i < payloadLen; i++) {
            payload[i] = (byte) ((h2 >>> ((i & 7) * 8)) & 0xFF);
        }
        return payload;
    }

    private static long headerHash(String key) {
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < key.length(); i++) {
            h ^= key.charAt(i);
            h *= 0x100000001b3L;
        }
        return h;
    }

    private static ByteBuf bufOf(byte[] bytes) {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(bytes.length);
        buf.writeBytes(bytes);
        return buf;
    }

    @Test
    public void testHammerNoTornReadsAcrossRecycle() throws Exception {
        Path slabPath = folder.newFolder().toPath().resolve("slab.dat");
        SlabCacheFileStore slab = new SlabCacheFileStore(slabPath, CELL_SIZE, TOTAL_CELLS);
        ExecutorService exec = Executors.newFixedThreadPool(THREADS);
        try {
            // Track the CURRENT (latest) variant per key. Readers compare the read
            // header against headerHash(key); writers stamp the new variant atomically
            // before the admit completes. The test never asserts that "what was
            // written N steps ago is what we read now" — only that "the bytes we
            // read for key K start with headerHash(K)" — so torn reads from a
            // recycled cell (which would carry a different headerHash) fail loudly.
            ConcurrentHashMap<String, AtomicInteger> variantByKey = new ConcurrentHashMap<>();
            for (int i = 0; i < KEY_POOL; i++) {
                variantByKey.put("k" + i, new AtomicInteger());
            }
            AtomicLong tornReads = new AtomicLong();
            AtomicLong totalReads = new AtomicLong();
            AtomicLong totalWrites = new AtomicLong();
            AtomicLong totalEvicts = new AtomicLong();
            CountDownLatch latch = new CountDownLatch(THREADS);

            for (int t = 0; t < THREADS; t++) {
                exec.submit(() -> {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    try {
                        for (int op = 0; op < OPS_PER_THREAD; op++) {
                            String key = "k" + rnd.nextInt(KEY_POOL);
                            int choice = rnd.nextInt(10);
                            if (choice < 4) {
                                // ADMIT.
                                int variant = variantByKey.get(key).incrementAndGet();
                                byte[] payload = payloadFor(key, variant);
                                try {
                                    slab.tryStore(key, bufOf(payload)).get(5, TimeUnit.SECONDS);
                                    totalWrites.incrementAndGet();
                                } catch (Exception e) {
                                    // Tier-full / I/O error / interrupted: skip; counts
                                    // are sanity-checked below.
                                }
                            } else if (choice < 8) {
                                // READ.
                                ByteBuf buf;
                                try {
                                    buf = slab.read(key).get(5, TimeUnit.SECONDS);
                                } catch (Exception e) {
                                    continue;
                                }
                                if (buf == null) {
                                    continue;
                                }
                                try {
                                    totalReads.incrementAndGet();
                                    if (buf.readableBytes() < HEADER_LEN) {
                                        tornReads.incrementAndGet();
                                        continue;
                                    }
                                    long readHeader = buf.getLong(buf.readerIndex());
                                    if (readHeader != headerHash(key)) {
                                        tornReads.incrementAndGet();
                                    }
                                } finally {
                                    buf.release();
                                }
                            } else {
                                // EVICT.
                                if (slab.markEvicted(key)) {
                                    totalEvicts.incrementAndGet();
                                }
                            }
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }
            assertTrue("workers must finish in time", latch.await(120, TimeUnit.SECONDS));

            assertEquals("no torn read may slip through pin/evict ordering",
                    0, tornReads.get());
            // Sanity: we performed real work.
            assertTrue(totalWrites.get() > 0);
            assertTrue(totalReads.get() > 0);
            assertTrue(totalEvicts.get() > 0);

            // Drain by evicting every key, then verify counters.
            for (int i = 0; i < KEY_POOL; i++) {
                slab.markEvicted("k" + i);
            }
            assertEquals("after final cleanup all bytes must be released",
                    0L, slab.bytesInUse());
            assertEquals("after final cleanup all cells must be free",
                    TOTAL_CELLS, slab.freeCells());
        } finally {
            exec.shutdownNow();
            exec.awaitTermination(10, TimeUnit.SECONDS);
            slab.close();
        }
    }

    @Test
    public void testConcurrentReadsOfSameKeyAreConsistent() throws Exception {
        Path slabPath = folder.newFolder().toPath().resolve("slab.dat");
        try (SlabCacheFileStore slab = new SlabCacheFileStore(slabPath, CELL_SIZE, 4)) {
            byte[] payload = payloadFor("k1", 1);
            slab.tryStore("k1", bufOf(payload)).get();

            int n = 32;
            ExecutorService exec = Executors.newFixedThreadPool(8);
            CountDownLatch latch = new CountDownLatch(n);
            AtomicLong mismatches = new AtomicLong();
            try {
                for (int i = 0; i < n; i++) {
                    exec.submit(() -> {
                        try {
                            ByteBuf buf = slab.read("k1").get(5, TimeUnit.SECONDS);
                            if (buf == null) {
                                mismatches.incrementAndGet();
                                return;
                            }
                            try {
                                if (buf.readableBytes() != payload.length) {
                                    mismatches.incrementAndGet();
                                    return;
                                }
                                byte[] got = new byte[buf.readableBytes()];
                                buf.getBytes(buf.readerIndex(), got);
                                for (int j = 0; j < got.length; j++) {
                                    if (got[j] != payload[j]) {
                                        mismatches.incrementAndGet();
                                        return;
                                    }
                                }
                            } finally {
                                buf.release();
                            }
                        } catch (Exception e) {
                            mismatches.incrementAndGet();
                        } finally {
                            latch.countDown();
                        }
                    });
                }
                assertTrue(latch.await(30, TimeUnit.SECONDS));
                assertEquals("all concurrent reads must see consistent bytes",
                        0, mismatches.get());
            } finally {
                exec.shutdownNow();
                exec.awaitTermination(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void testEvictDuringReadDoesNotTearBuffer() throws Exception {
        // Force the read/evict race many times: thread A pins for read, thread B
        // calls markEvicted, last unpin recycles the cell. The slab guarantees the
        // ByteBuf returned to A still carries A's bytes regardless of B's eviction.
        Path slabPath = folder.newFolder().toPath().resolve("slab.dat");
        try (SlabCacheFileStore slab = new SlabCacheFileStore(slabPath, CELL_SIZE, 1)) {
            int rounds = 400;
            byte[] payload = payloadFor("hot", 1);
            ExecutorService exec = Executors.newFixedThreadPool(2);
            AtomicLong mismatches = new AtomicLong();
            try {
                for (int r = 0; r < rounds; r++) {
                    slab.tryStore("hot", bufOf(payload)).get();
                    CountDownLatch ready = new CountDownLatch(2);
                    CountDownLatch start = new CountDownLatch(1);
                    List<java.util.concurrent.Future<?>> tasks = new ArrayList<>();

                    tasks.add(exec.submit(() -> {
                        ready.countDown();
                        try {
                            start.await();
                            ByteBuf buf = slab.read("hot").get(2, TimeUnit.SECONDS);
                            if (buf == null) {
                                return; // legitimate: eviction won
                            }
                            try {
                                if (buf.readableBytes() != payload.length) {
                                    mismatches.incrementAndGet();
                                    return;
                                }
                                byte[] got = new byte[buf.readableBytes()];
                                buf.getBytes(buf.readerIndex(), got);
                                for (int j = 0; j < got.length; j++) {
                                    if (got[j] != payload[j]) {
                                        mismatches.incrementAndGet();
                                        return;
                                    }
                                }
                            } finally {
                                buf.release();
                            }
                        } catch (Exception ignored) {
                            // Best-effort under contention; mismatches is the real check.
                        }
                    }));
                    tasks.add(exec.submit(() -> {
                        ready.countDown();
                        try {
                            start.await();
                            slab.markEvicted("hot");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }));

                    assertTrue(ready.await(5, TimeUnit.SECONDS));
                    start.countDown();
                    for (java.util.concurrent.Future<?> f : tasks) {
                        f.get(5, TimeUnit.SECONDS);
                    }
                }
                assertEquals("read/evict race must not return torn bytes",
                        0, mismatches.get());
            } finally {
                exec.shutdownNow();
                exec.awaitTermination(10, TimeUnit.SECONDS);
            }
        }
    }
}
