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

package herddb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Tests on LocalLocalManager
 *
 * @author eolivelli
 */
public class LocalLockManagerTest {

    private static final Bytes KEY = Bytes.from_int(1);

    @Rule
    public Timeout timeout = new Timeout(10000);

    @Test
    public void testSimple() {
        ILocalLockManager manager = makeLockManager();
        LockHandle h = manager.acquireReadLockForKey(KEY);
        assertEquals(1, manager.getNumKeys());

        manager.releaseLock(h);
        assertEquals(0, manager.getNumKeys());

        LockHandle h2 = manager.acquireWriteLockForKey(KEY);
        assertEquals(1, manager.getNumKeys());

        manager.releaseLock(h2);
        assertEquals(0, manager.getNumKeys());
    }

    @Test
    public void testReentrantReads() {
        ILocalLockManager manager = makeLockManager();
        LockHandle h = manager.acquireReadLockForKey(KEY);
        assertEquals(1, manager.getNumKeys());

        LockHandle hb = manager.acquireReadLockForKey(KEY);
        assertEquals(1, manager.getNumKeys());

        manager.releaseLock(hb);
        manager.releaseLock(h);
        assertEquals(0, manager.getNumKeys());

    }

    @Test
    public void testReentrantReads2() {
        ILocalLockManager manager = makeLockManager();
        LockHandle h = manager.acquireReadLockForKey(KEY);
        assertEquals(1, manager.getNumKeys());

        LockHandle hb = manager.acquireReadLockForKey(KEY);
        assertEquals(1, manager.getNumKeys());

        manager.releaseLock(h);
        manager.releaseLock(hb);
        assertEquals(0, manager.getNumKeys());

    }

    @Test(expected = RuntimeException.class)
    public void testWriterBlockedByReaderNonRentrant() {
        ILocalLockManager manager = makeLockManager();
        manager.acquireReadLockForKey(KEY);
        assertEquals(1, manager.getNumKeys());
        manager.acquireWriteLockForKey(KEY);
    }

    @Test(expected = RuntimeException.class)
    public void testReaderBlockedByWriterNonRentrant() {
        ILocalLockManager manager = makeLockManager();
        manager.acquireWriteLockForKey(KEY);
        assertEquals(1, manager.getNumKeys());
        manager.acquireReadLockForKey(KEY);
    }

    @Test
    public void testHammer() {
        ILocalLockManager manager = makeLockManager();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future> res = new ArrayList<>();
            for (int i = 0; i < 10000; i++) {
                int _i = i;
                res.add(executor.submit(() -> {
                    LockHandle l;
                    if (_i % 5 == 0) {
                        l = manager.acquireWriteLockForKey(KEY);
                    } else {
                        l = manager.acquireReadLockForKey(KEY);
                    }
                    manager.releaseLock(l);
                }));
            }
            res.forEach(new Consumer<Future>() {
                int count;

                @Override
                public void accept(Future f) {
                    try {
                        f.get(10, TimeUnit.SECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
            assertEquals(0, manager.getNumKeys());

        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testTwoThreads() throws InterruptedException {
        ILocalLockManager manager = makeLockManager();
        CountDownLatch ww = new CountDownLatch(1);
        CountDownLatch ww2 = new CountDownLatch(1);
        AtomicReference<Throwable> error1 = new AtomicReference<>();
        AtomicReference<Throwable> error2 = new AtomicReference<>();
        Thread thread1 = new Thread() {
            @Override
            public void run() {
                try {
                    ww2.countDown();
                    LockHandle l = manager.acquireReadLockForKey(KEY);
                    ww.await();
                    manager.releaseLock(l);
                } catch (Throwable ex) {
                    ex.printStackTrace();
                    error1.set(ex);
                }
            }
        };
        Thread thread2 = new Thread() {
            @Override
            public void run() {
                try {
                    ww2.await();
                    LockHandle l = manager.acquireReadLockForKey(KEY);
                    ww.countDown();
                    manager.releaseLock(l);
                } catch (Throwable ex) {
                    ex.printStackTrace();
                    error2.set(ex);
                }
            }
        };
        thread2.start();
        thread1.start();

        thread1.join();
        thread2.join();

        assertNull(error1.get());
        assertNull(error2.get());

        assertEquals(0, manager.getNumKeys());

    }

    @Test
    public void testTwoThreadsInterleaved() throws InterruptedException {
        ILocalLockManager manager = makeLockManager();
        CountDownLatch ww = new CountDownLatch(1);
        CountDownLatch ww2 = new CountDownLatch(1);
        AtomicReference<Throwable> error1 = new AtomicReference<>();
        AtomicReference<Throwable> error2 = new AtomicReference<>();
        Thread thread1 = new Thread() {
            @Override
            public void run() {
                try {
                    ww2.countDown();
                    LockHandle l = manager.acquireReadLockForKey(KEY);
                    ww.await();
                    manager.releaseLock(l);
                } catch (Throwable ex) {
                    ex.printStackTrace();
                    error1.set(ex);
                }
            }
        };
        Thread thread2 = new Thread() {
            @Override
            public void run() {
                try {
                    ww2.await();
                    LockHandle l = manager.acquireReadLockForKey(KEY);

                    manager.releaseLock(l);
                    ww.countDown();
                } catch (Throwable ex) {
                    ex.printStackTrace();
                    error2.set(ex);
                }
            }
        };
        thread2.start();
        thread1.start();

        thread1.join();
        thread2.join();

        assertNull(error1.get());
        assertNull(error2.get());

        assertEquals(0, manager.getNumKeys());

    }

    /**
     * Hammer many distinct keys concurrently to exercise the slow-path
     * put/remove and the resurrection race (refCount goes to zero on one
     * thread while another thread is concurrently acquiring the same key).
     * After all work is done the map must be empty.
     */
    @Test
    public void testManyDistinctKeysConcurrent() {
        final ILocalLockManager manager = makeLockManager();
        final int numKeys = 64;
        final int opsPerKey = 2_000;
        final int threads = 8;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            final List<Future<?>> res = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int seed = t;
                res.add(executor.submit(() -> {
                    final Random rnd = new Random(seed);
                    for (int i = 0; i < numKeys * opsPerKey / threads; i++) {
                        final Bytes key = Bytes.from_int(rnd.nextInt(numKeys));
                        final LockHandle h;
                        if ((i & 3) == 0) {
                            h = manager.acquireWriteLockForKey(key);
                        } else {
                            h = manager.acquireReadLockForKey(key);
                        }
                        manager.releaseLock(h);
                    }
                }));
            }
            for (Future<?> f : res) {
                try {
                    f.get(30, TimeUnit.SECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                    throw new RuntimeException(ex);
                }
            }
            assertEquals(0, manager.getNumKeys());
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Tight rapid acquire/release cycle on a single key from many threads.
     * Stresses the refcount CAS-loop and the {@code remove(key, instance)}
     * race against concurrent {@code compute(key, ACQUIRE_FN)} resurrections.
     */
    @Test
    public void testRapidAcquireReleaseSameKey() {
        final ILocalLockManager manager = makeLockManager();
        final int threads = 8;
        final int opsPerThread = 5_000;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            final List<Future<?>> res = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int idx = t;
                res.add(executor.submit(() -> {
                    for (int i = 0; i < opsPerThread; i++) {
                        final LockHandle h;
                        if (((idx + i) & 1) == 0) {
                            h = manager.acquireReadLockForKey(KEY);
                        } else {
                            h = manager.acquireWriteLockForKey(KEY);
                        }
                        manager.releaseLock(h);
                    }
                }));
            }
            for (Future<?> f : res) {
                try {
                    f.get(30, TimeUnit.SECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                    throw new RuntimeException(ex);
                }
            }
            assertEquals(0, manager.getNumKeys());
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Verify that after a key has been fully released (refcount → 0,
     * entry removed) it can be re-acquired and that the new acquire
     * returns a working {@link LockHandle}. This exercises the slow-path
     * recreation of a {@code LockInstance} after retirement.
     */
    @Test
    public void testFastPathThenResurrection() {
        final ILocalLockManager manager = makeLockManager();
        for (int round = 0; round < 100; round++) {
            final LockHandle h1 = manager.acquireWriteLockForKey(KEY);
            assertNotNull(h1);
            assertEquals(1, manager.getNumKeys());
            manager.releaseLock(h1);
            assertEquals(0, manager.getNumKeys());

            final LockHandle h2 = manager.acquireReadLockForKey(KEY);
            assertNotNull(h2);
            assertEquals(1, manager.getNumKeys());
            manager.releaseLock(h2);
            assertEquals(0, manager.getNumKeys());
        }
    }

    /**
     * Releasing the same handle twice must be detected. {@link StampedLock}
     * itself rejects the double {@code unlockWrite} with
     * {@link IllegalMonitorStateException} (a {@link RuntimeException}
     * subclass), and the {@code returnLockForKey} refcount underflow check
     * provides a backup signal. Either is acceptable as evidence that the
     * double release was rejected.
     */
    @Test
    public void testDoubleReleaseThrows() {
        final ILocalLockManager manager = makeLockManager();
        final LockHandle h = manager.acquireWriteLockForKey(KEY);
        manager.releaseLock(h);
        try {
            manager.releaseLock(h);
            fail("expected an exception on double release");
        } catch (IllegalMonitorStateException | IllegalStateException expected) {
            // ok — both are acceptable signals that the double release
            // was rejected.
        }
    }

    private ILocalLockManager makeLockManager() {
        LocalLockManager res = new LocalLockManager();
        res.setWriteLockTimeout(1);
        res.setReadLockTimeout(1);
        return res;
    }

}
