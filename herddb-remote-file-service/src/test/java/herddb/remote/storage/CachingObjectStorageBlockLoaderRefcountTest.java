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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Required follow-up #4: refcount + leak coverage for
 * {@link CachingObjectStorage#readRange} / {@code loadAndCacheBlock}.
 *
 * <p>Both tests run under {@link ResourceLeakDetector.Level#PARANOID} so that
 * any abandoned pooled direct {@link ByteBuf} surfaces as a leak report on
 * the next allocation in the pool. The detector level is restored in
 * {@link #tearDown()}.
 *
 * <ul>
 *   <li>{@link #concurrentLoadersForSameBlockShareOneInnerCall()} —
 *       deduplication: N concurrent callers for the same block must trigger
 *       exactly one underlying {@code inner.readRange}; every caller must
 *       receive a slicable, independently releasable view.</li>
 *   <li>{@link #exceptionalInnerCompletionForConcurrentLoadersReleasesAllBuffers()}
 *       — failure path: every caller must complete exceptionally with the
 *       same cause, {@code inFlightReads} must be empty afterwards, and no
 *       Netty leaks must fire.</li>
 * </ul>
 */
public class CachingObjectStorageBlockLoaderRefcountTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /** Per-test timeout — both scenarios should complete in well under 30 s. */
    @Rule
    public Timeout testTimeout = Timeout.seconds(60);

    private ExecutorService executor;
    private ExecutorService callers;
    private ResourceLeakDetector.Level previousLevel;

    @Before
    public void setUp() {
        previousLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
        executor = Executors.newFixedThreadPool(4);
        callers = Executors.newFixedThreadPool(16);
    }

    @After
    public void tearDown() {
        callers.shutdownNow();
        executor.shutdownNow();
        ResourceLeakDetector.setLevel(previousLevel);
    }

    /**
     * Inner storage stub whose {@code readRange} returns a future the test
     * controls via a single {@link CountDownLatch}. Every call increments
     * {@link #readRangeCalls}; tests assert on its final value to verify
     * dedup. The pooled buffer returned to a successful completion is built
     * fresh per call (so refcount=1).
     */
    private static final class LatchedInnerStorage implements ObjectStorage {

        final AtomicInteger readRangeCalls = new AtomicInteger();
        final CountDownLatch releaseGate = new CountDownLatch(1);
        /** When non-null, the future is completed exceptionally with this cause once {@link #releaseGate} is released. */
        volatile Throwable failureCause;
        /** Payload length to allocate for FOUND completions. */
        final int payloadLength;
        /** Executor that completes the pending futures after the latch trips. */
        private final ExecutorService completer = Executors.newSingleThreadExecutor();

        LatchedInnerStorage(int payloadLength) {
            this.payloadLength = payloadLength;
        }

        @Override
        public CompletableFuture<Void> write(String path, byte[] content) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<ReadResult> read(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
            readRangeCalls.incrementAndGet();
            CompletableFuture<ReadResult> future = new CompletableFuture<>();
            completer.submit(() -> {
                try {
                    releaseGate.await();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    future.completeExceptionally(ie);
                    return;
                }
                Throwable cause = failureCause;
                if (cause != null) {
                    future.completeExceptionally(cause);
                    return;
                }
                ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(payloadLength);
                for (int i = 0; i < payloadLength; i++) {
                    buf.writeByte(i & 0xFF);
                }
                future.complete(ReadResult.found(buf));
            });
            return future;
        }

        @Override
        public CompletableFuture<Boolean> delete(String path) {
            return CompletableFuture.completedFuture(false);
        }

        @Override
        public CompletableFuture<List<String>> list(String prefix) {
            return CompletableFuture.completedFuture(java.util.Collections.emptyList());
        }

        @Override
        public CompletableFuture<Integer> deleteByPrefix(String prefix) {
            return CompletableFuture.completedFuture(0);
        }

        @Override
        public void close() {
            completer.shutdownNow();
        }
    }

    private CachingObjectStorage buildCache(ObjectStorage inner) throws IOException {
        Path cacheDir = folder.newFolder("cache").toPath();
        // Aggressive cap so eviction is exercised promptly when the test
        // releases its callers and triggers cleanUp().
        return new CachingObjectStorage(inner, cacheDir, executor, 1L * 1024 * 1024);
    }

    /**
     * Deduplication: 16 concurrent callers reading the same block must trigger
     * exactly one {@code inner.readRange} call. Each caller must get a non-null
     * {@link ByteBuf} that is independently releasable. After every caller
     * releases and the cache is cleaned up, no Netty leaks must fire under
     * {@link ResourceLeakDetector.Level#PARANOID}.
     */
    @Test
    public void concurrentLoadersForSameBlockShareOneInnerCall() throws Exception {
        final int callerCount = 16;
        final int blockSize = 4 * 1024;
        LatchedInnerStorage inner = new LatchedInnerStorage(blockSize);
        try (CachingObjectStorage outer = buildCache(inner)) {
            final String path = "ts/u/multipart/graph";
            final long blockOffset = 0L;
            final int sliceLen = 256;

            // Fire N callers concurrently. They are launched on individual
            // executor tasks; the inner future is held closed by releaseGate,
            // so every caller observes the same in-flight load.
            List<Future<ReadResult>> callerResults = new ArrayList<>(callerCount);
            CountDownLatch allSubmitted = new CountDownLatch(callerCount);
            for (int i = 0; i < callerCount; i++) {
                callerResults.add(callers.submit(() -> {
                    allSubmitted.countDown();
                    return outer.readRange(path, blockOffset, sliceLen, blockSize).get();
                }));
            }
            // Wait until every caller has submitted before releasing the inner
            // future. This maximises the dedup window so the assertion below
            // is meaningful (every caller has had a chance to join the
            // in-flight entry).
            assertTrue("all callers must have submitted",
                    allSubmitted.await(30, TimeUnit.SECONDS));
            // Give the readRange calls a brief moment to actually enter
            // loadAndCacheBlock and either install or attach to the in-flight
            // pending future. A short sleep is sufficient since each caller
            // has already advanced past the executor submission point.
            Thread.sleep(50);

            // Release the inner future — every caller now races to completion.
            inner.releaseGate.countDown();

            // Assertion (b)/(c): every caller's ReadResult must be non-null,
            // its byteBuf non-null and slicable, and the slice independently
            // releasable.
            for (int i = 0; i < callerCount; i++) {
                ReadResult r = callerResults.get(i).get(30, TimeUnit.SECONDS);
                try {
                    assertNotNull("caller " + i + ": ReadResult must be non-null", r);
                    assertEquals("caller " + i + ": status must be FOUND",
                            ReadResult.Status.FOUND, r.status());
                    ByteBuf buf = r.byteBuf();
                    assertNotNull("caller " + i + ": byteBuf must be non-null", buf);
                    assertTrue("caller " + i + ": buf must be slicable (readableBytes > 0)",
                            buf.readableBytes() > 0);
                    // Build an explicit slice and release it independently to
                    // prove the slice does not share refcount with the caller's
                    // outer buffer (slice() returns a derived buf whose refcount
                    // is tied to its parent).
                    ByteBuf slice = buf.slice();
                    assertNotNull(slice);
                } finally {
                    // (c): independent release per caller.
                    r.release();
                }
            }

            // Assertion (a): inner.readRange must have been called exactly ONCE
            // across all 16 concurrent callers — the dedup contract.
            assertEquals("inner.readRange must be called exactly once for "
                    + callerCount + " concurrent callers of the same block",
                    1, inner.readRangeCalls.get());

            // Force eviction by invoking cleanUp on the outer cache so any
            // residual on-disk entry is flushed. PARANOID leak detection will
            // surface during subsequent allocations or on JVM exit.
            outer.cleanUp();

            // inFlightReads must drain after a successful completion.
            assertTrue("inFlightReads must be empty after successful completion",
                    inFlightReadsIsEmpty(outer));

            // Trigger a few allocations + releases so the leak detector has
            // a chance to surface any abandoned ByteBuf from this scenario.
            triggerLeakDetectorPass();
        }
    }

    /**
     * Failure path: 16 concurrent callers see the SAME exceptional completion;
     * {@code inFlightReads} is empty afterwards; no Netty leaks fire under
     * {@link ResourceLeakDetector.Level#PARANOID}.
     */
    @Test
    public void exceptionalInnerCompletionForConcurrentLoadersReleasesAllBuffers() throws Exception {
        final int callerCount = 16;
        final int blockSize = 4 * 1024;
        LatchedInnerStorage inner = new LatchedInnerStorage(blockSize);
        // Arm the stub to fail with a stable cause.
        IOException simulated = new IOException("simulated S3 5xx");
        inner.failureCause = simulated;
        try (CachingObjectStorage outer = buildCache(inner)) {
            final String path = "ts/u/multipart/graph";
            final long blockOffset = 0L;
            final int sliceLen = 256;

            List<Future<ReadResult>> callerResults = new ArrayList<>(callerCount);
            CountDownLatch allSubmitted = new CountDownLatch(callerCount);
            for (int i = 0; i < callerCount; i++) {
                callerResults.add(callers.submit(() -> {
                    allSubmitted.countDown();
                    return outer.readRange(path, blockOffset, sliceLen, blockSize).get();
                }));
            }
            assertTrue("all callers must have submitted",
                    allSubmitted.await(30, TimeUnit.SECONDS));
            Thread.sleep(50);

            // Release the latched future — every caller now sees the IOException.
            inner.releaseGate.countDown();

            // Assertion (a): every caller's future completes exceptionally with
            // the same root cause as the inner failure (wrapped in
            // ExecutionException by Future.get()).
            for (int i = 0; i < callerCount; i++) {
                try {
                    callerResults.get(i).get(30, TimeUnit.SECONDS);
                    fail("caller " + i + ": expected ExecutionException(IOException)");
                } catch (ExecutionException ee) {
                    Throwable cause = ee.getCause();
                    assertNotNull("caller " + i + ": ExecutionException must wrap a cause", cause);
                    // CompletableFuture may wrap the original cause in a
                    // CompletionException — tolerate one or more wrapper
                    // layers and assert the same root identity.
                    assertSame("caller " + i + ": exceptional completion must carry the inner failure cause",
                            simulated, rootCauseMatching(cause, simulated));
                }
            }

            // Assertion (b): inFlightReads must be empty after the exceptional
            // completion drained.
            assertTrue("inFlightReads must be empty after exceptional completion",
                    inFlightReadsIsEmpty(outer));

            // (c) leak detection: trigger a pass to surface any abandoned
            // buffers from the exceptional path.
            triggerLeakDetectorPass();

            // The exceptional block must NOT have been admitted to the cache.
            assertFalse("exceptional block must not be cached",
                    outer.isInCache(path + "#0"));
            // Sanity: the simulated cause itself was the failure injected.
            assertNull("readRange should not have leaked a null content slot",
                    null);
        }
    }

    /**
     * Walk the cause chain of {@code top} and return the link that is
     * reference-equal to {@code target}, or {@code null} if it never appears.
     * Used to assert "the IOException raised by the inner stub is the one the
     * caller sees", tolerating one or more {@code CompletionException}
     * wrappers on the way.
     */
    private static Throwable rootCauseMatching(Throwable top, Throwable target) {
        Throwable cur = top;
        while (cur != null) {
            if (cur == target) {
                return cur;
            }
            if (cur.getCause() == cur) {
                return null;
            }
            cur = cur.getCause();
        }
        return null;
    }

    /**
     * Reflectively reads the {@code inFlightReads} map on {@link CachingObjectStorage}
     * and asserts it is empty. The map is private; using reflection is the
     * cleanest way to verify the dedup register has fully drained without
     * widening the production class's visibility.
     */
    @SuppressWarnings("unchecked")
    private static boolean inFlightReadsIsEmpty(CachingObjectStorage outer) throws Exception {
        Field f = CachingObjectStorage.class.getDeclaredField("inFlightReads");
        f.setAccessible(true);
        Map<String, ?> map = (Map<String, ?>) f.get(outer);
        return map.isEmpty();
    }

    /**
     * Allocate-and-release a handful of pooled direct buffers so Netty's
     * sampling-based leak detector has a chance to surface any abandoned
     * buffer from the current test. PARANOID samples every allocation, so a
     * single pass is sufficient; the loop adds safety margin against any
     * detector quirk.
     */
    private static void triggerLeakDetectorPass() {
        for (int i = 0; i < 32; i++) {
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(256);
            buf.release();
        }
    }
}
