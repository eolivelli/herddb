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

package herddb.index.blink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.ClockProPolicy;
import herddb.utils.Sized;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/**
 * Targeted regression test for the {@code Node.lock} / {@code Anchor.lock}
 * swap from {@link java.util.concurrent.locks.ReentrantReadWriteLock} to
 * {@link java.util.concurrent.locks.StampedLock} (issue #386).
 *
 * <p>Reproduces the exact lock-contention shape from the issue: many concurrent
 * search threads exercising the read-lock path against a smaller set of insert
 * threads exercising the write-lock path on overlapping keys, plus a snapshot
 * of pre-populated keys that must remain visible throughout the run. Asserts:
 * <ul>
 *   <li>no exception thrown by any worker thread,</li>
 *   <li>every pre-populated key is found by the search threads on every
 *       iteration (the snapshot stays visible while writers churn around it),</li>
 *   <li>after the writers complete, every newly-inserted key is findable and
 *       every newly-deleted key is gone,</li>
 *   <li>the tree's reported {@code size()} matches the expected count.</li>
 * </ul>
 *
 * <p>The non-reentrancy of {@link java.util.concurrent.locks.StampedLock} would
 * surface here as deadlock or {@link IllegalMonitorStateException}; mismatched
 * stamps would surface as cross-key visibility errors.
 */
public class BLinkConcurrentSearchInsertTest {

    @Test
    public void searchHeavyConcurrentLoad() throws Exception {

        final int snapshotKeys = 4000;
        final int writerKeys = 4000;
        final int searchThreads = 12;
        final int writerThreads = 4;
        final int searchIterations = 200;
        final int writerIterations = 1500;
        final long timeoutSeconds = 120;

        BLinkTest.DummyBLinkIndexDataStorage<Sized<Long>, Long> storage =
                new BLinkTest.DummyBLinkIndexDataStorage<>();

        try (BLink<Sized<Long>, Long> blink = new BLink<>(
                4096L,
                new BLinkTest.LongSizeEvaluator(),
                new ClockProPolicy(50),
                storage)) {

            // Snapshot: pre-populated keys that must remain visible for the
            // entire run. They never overlap with the writer key range so a
            // search for any of them must always succeed.
            final ConcurrentHashMap<Long, Long> snapshot = new ConcurrentHashMap<>();
            for (long k = 0; k < snapshotKeys; k++) {
                long value = k * 31L + 7L; // arbitrary non-zero value
                blink.insert(Sized.valueOf(k), value);
                snapshot.put(k, value);
            }
            assertEquals(snapshotKeys, blink.size());

            // Writer key range starts above the snapshot range.
            final long writerKeyBase = snapshotKeys;
            final ConcurrentHashMap<Long, Long> writerExpected = new ConcurrentHashMap<>();

            final List<Long> snapshotKeysList = new ArrayList<>(snapshot.keySet());
            Collections.shuffle(snapshotKeysList, new Random(42));
            final List<Long> snapshotKeysReadOnly = Collections.unmodifiableList(snapshotKeysList);

            final ExecutorService pool = Executors.newFixedThreadPool(searchThreads + writerThreads);
            final CountDownLatch ready = new CountDownLatch(searchThreads + writerThreads);
            final CountDownLatch start = new CountDownLatch(1);
            final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
            final AtomicLong searches = new AtomicLong();
            final AtomicLong writes = new AtomicLong();
            final AtomicLong deletes = new AtomicLong();

            final List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < searchThreads; t++) {
                final long seed = 1_000L + t;
                futures.add(pool.submit(() -> {
                    Random rnd = new Random(seed);
                    ready.countDown();
                    try {
                        start.await();
                        for (int i = 0; i < searchIterations; i++) {
                            // Spot-check a random subset of snapshot keys per iteration:
                            // keep the work per iteration bounded so the writer threads
                            // get plenty of overlap rather than spending all their time
                            // queued behind a single long search round.
                            int probes = 32;
                            for (int p = 0; p < probes; p++) {
                                long k = snapshotKeysReadOnly.get(
                                        rnd.nextInt(snapshotKeysReadOnly.size()));
                                Long expected = snapshot.get(k);
                                Long actual = blink.search(Sized.valueOf(k));
                                assertNotNull("snapshot key " + k + " disappeared during run",
                                        actual);
                                assertEquals("snapshot key " + k + " value drift",
                                        expected, actual);
                                searches.incrementAndGet();
                            }
                            // Also probe writer-range keys to exercise the read path
                            // against keys that may or may not exist concurrently with
                            // writers; result is allowed to be null but must not throw.
                            long wk = writerKeyBase + rnd.nextInt(writerKeys);
                            blink.search(Sized.valueOf(wk));
                            searches.incrementAndGet();
                        }
                    } catch (Throwable err) {
                        firstFailure.compareAndSet(null, err);
                    }
                }));
            }

            for (int t = 0; t < writerThreads; t++) {
                final long seed = 2_000L + t;
                futures.add(pool.submit(() -> {
                    Random rnd = new Random(seed);
                    ready.countDown();
                    try {
                        start.await();
                        for (int i = 0; i < writerIterations; i++) {
                            long k = writerKeyBase + rnd.nextInt(writerKeys);
                            int op = rnd.nextInt(10);
                            if (op < 8) {
                                // 80 % insert/update
                                long v = rnd.nextLong();
                                if (v == 0L) {
                                    v = 1L; // reserve 0 as "deleted" sentinel
                                }
                                blink.insert(Sized.valueOf(k), v);
                                writerExpected.put(k, v);
                                writes.incrementAndGet();
                            } else {
                                // 20 % delete
                                blink.delete(Sized.valueOf(k));
                                writerExpected.put(k, 0L);
                                deletes.incrementAndGet();
                            }
                        }
                    } catch (Throwable err) {
                        firstFailure.compareAndSet(null, err);
                    }
                }));
            }

            // Synchronize start so all threads contend together.
            assertTrue("workers never reached the start barrier",
                    ready.await(30, TimeUnit.SECONDS));
            start.countDown();

            for (Future<?> f : futures) {
                f.get(timeoutSeconds, TimeUnit.SECONDS);
            }
            pool.shutdown();
            assertTrue("thread pool did not shut down cleanly",
                    pool.awaitTermination(30, TimeUnit.SECONDS));

            if (firstFailure.get() != null) {
                fail("worker thread failed: " + firstFailure.get());
            }

            // Re-verify the snapshot keys: every one of them must still be present
            // with the original value.
            for (long k = 0; k < snapshotKeys; k++) {
                Long actual = blink.search(Sized.valueOf(k));
                assertNotNull("post-run: snapshot key " + k + " missing", actual);
                assertEquals("post-run: snapshot key " + k + " drifted",
                        snapshot.get(k), actual);
            }

            // Verify writer-range keys match the latest writer expectation.
            int finalLiveWriterKeys = 0;
            for (long k = writerKeyBase; k < writerKeyBase + writerKeys; k++) {
                Long expected = writerExpected.get(k);
                Long actual = blink.search(Sized.valueOf(k));
                if (expected == null || expected == 0L) {
                    assertNull("post-run: writer key " + k + " should be absent but found "
                            + actual, actual);
                } else {
                    assertEquals("post-run: writer key " + k + " value mismatch",
                            expected, actual);
                    finalLiveWriterKeys++;
                }
            }

            assertEquals("tree size mismatch after concurrent run",
                    snapshotKeys + finalLiveWriterKeys, blink.size());

            System.out.println("BLinkConcurrentSearchInsertTest done — searches="
                    + searches.get() + " writes=" + writes.get()
                    + " deletes=" + deletes.get()
                    + " liveWriterKeys=" + finalLiveWriterKeys);
        }
    }
}
