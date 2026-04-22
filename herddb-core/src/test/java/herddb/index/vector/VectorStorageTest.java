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

package herddb.index.vector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Test;

public class VectorStorageTest {

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();

    private static VectorFloat<?> vec(float... values) {
        return VTS.createFloatVector(values);
    }

    private static float[] toFloats(VectorFloat<?> v) {
        float[] result = new float[v.length()];
        for (int i = 0; i < v.length(); i++) {
            result[i] = v.get(i);
        }
        return result;
    }

    @Test
    public void testGetSet() {
        VectorStorage storage = new VectorStorage(16);

        VectorFloat<?> v0 = vec(1f, 2f, 3f);
        VectorFloat<?> v5 = vec(4f, 5f, 6f);

        storage.set(0, v0);
        storage.set(5, v5);

        assertSame(v0, storage.get(0));
        assertSame(v5, storage.get(5));
    }

    @Test
    public void testGetBeforeSet() {
        VectorStorage storage = new VectorStorage(16);
        // No exception, just null for unset nodeIds
        assertNull(storage.get(0));
        assertNull(storage.get(100));
        assertNull(storage.get(1_000_000)); // beyond initial capacity
    }

    @Test
    public void testGrowth() {
        VectorStorage storage = new VectorStorage(4);

        VectorFloat<?> vLow = vec(1f, 2f);
        VectorFloat<?> vHigh = vec(3f, 4f);

        storage.set(0, vLow);
        storage.set(1_000_000, vHigh); // triggers multiple doublings

        assertSame(vLow, storage.get(0));
        assertSame(vHigh, storage.get(1_000_000));
        assertTrue(storage.length() > 1_000_000);
    }

    @Test
    public void testRemove() {
        VectorStorage storage = new VectorStorage(16);
        VectorFloat<?> v = vec(1f, 2f);
        storage.set(3, v);
        assertSame(v, storage.get(3));

        storage.remove(3);
        assertNull(storage.get(3));
    }

    @Test
    public void testRemoveUnset() {
        VectorStorage storage = new VectorStorage(16);
        // Should not throw
        storage.remove(999);
        storage.remove(0);
    }

    @Test
    public void testConcurrentWrites() throws Exception {
        int threads = 8;
        int perThread = 1000;
        VectorStorage storage = new VectorStorage(threads * perThread);
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int base = t * perThread;
            futures.add(exec.submit(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int i = 0; i < perThread; i++) {
                    int nodeId = base + i;
                    storage.set(nodeId, vec((float) nodeId));
                }
            }));
        }
        startLatch.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        exec.shutdown();

        // Verify all vectors are readable
        for (int nodeId = 0; nodeId < threads * perThread; nodeId++) {
            VectorFloat<?> v = storage.get(nodeId);
            assertArrayEquals(new float[]{(float) nodeId}, toFloats(v), 0.001f);
        }
    }

    @Test
    public void testVolatileVisibility() throws Exception {
        VectorStorage storage = new VectorStorage(16);
        VectorFloat<?> v = vec(42f);
        CountDownLatch written = new CountDownLatch(1);
        CountDownLatch read = new CountDownLatch(1);

        Thread writer = new Thread(() -> {
            storage.set(7, v);
            written.countDown();
        });
        Thread reader = new Thread(() -> {
            try {
                written.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            VectorFloat<?> got = storage.get(7);
            if (got == null) {
                throw new AssertionError("Expected non-null vector after write");
            }
            read.countDown();
        });

        writer.start();
        reader.start();
        writer.join(5000);
        reader.join(5000);
        read.await();
    }

    /**
     * Stresses the lock-free fast path of {@link VectorStorage#set(int, VectorFloat)}
     * against frequent concurrent growth: each writer thread touches nodeIds spread
     * across a range that forces many doublings of the backing array.  Verifies that
     * no write is lost on the final published array despite writes racing with the
     * synchronized grow copy-loop.
     */
    @Test
    public void testConcurrentWritesAcrossGrowths() throws Exception {
        int threads = 16;
        int perThread = 5000;
        // Start small so that almost every write in the first stripe triggers growth.
        VectorStorage storage = new VectorStorage(8);
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int threadIdx = t;
            futures.add(exec.submit(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                // Interleave nodeIds across threads to maximize grow contention:
                // thread t writes nodeIds t, t+threads, t+2*threads, ...
                for (int i = 0; i < perThread; i++) {
                    int nodeId = threadIdx + i * threads;
                    storage.set(nodeId, vec((float) nodeId));
                }
            }));
        }
        startLatch.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        exec.shutdown();

        int total = threads * perThread;
        for (int nodeId = 0; nodeId < total; nodeId++) {
            VectorFloat<?> v = storage.get(nodeId);
            if (v == null) {
                fail("Lost write at nodeId=" + nodeId + " after concurrent growth");
            }
            assertArrayEquals(new float[]{(float) nodeId}, toFloats(v), 0.001f);
        }
    }

    /**
     * Stresses set/remove interleaving under concurrent growth.  remove() is
     * synchronized so it excludes growAndSet; the lock-free set fast path must
     * still produce a consistent final state: every nodeId either has its
     * last-set vector or was explicitly removed.
     */
    @Test
    public void testConcurrentSetAndRemoveAcrossGrowths() throws Exception {
        int writerThreads = 8;
        int removerThreads = 2;
        int perThread = 2000;
        VectorStorage storage = new VectorStorage(8);
        ExecutorService exec = Executors.newFixedThreadPool(writerThreads + removerThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        // Writers: each owns a disjoint range of nodeIds so set/remove on the same
        // nodeId are produced by the same thread (matching PersistentVectorStore's
        // nodeId-per-thread invariant).
        for (int t = 0; t < writerThreads; t++) {
            final int base = t * perThread;
            futures.add(exec.submit(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perThread; i++) {
                    storage.set(base + i, vec((float) (base + i)));
                }
            }));
        }
        // Removers: call remove() on a range that is NOT owned by any writer,
        // just to keep the grow/remove monitor contention alive without racing
        // set/remove on the same slot.
        int removerBase = writerThreads * perThread;
        for (int t = 0; t < removerThreads; t++) {
            final int base = removerBase + t * perThread;
            futures.add(exec.submit(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perThread; i++) {
                    // Set then remove the same slot. After the remove the slot must be null
                    // even if the set raced with a concurrent grow.
                    storage.set(base + i, vec((float) -1));
                    storage.remove(base + i);
                }
            }));
        }
        startLatch.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        exec.shutdown();

        int writerTotal = writerThreads * perThread;
        for (int nodeId = 0; nodeId < writerTotal; nodeId++) {
            VectorFloat<?> v = storage.get(nodeId);
            if (v == null) {
                fail("Lost write at nodeId=" + nodeId + " after concurrent growth + remove");
            }
            assertArrayEquals(new float[]{(float) nodeId}, toFloats(v), 0.001f);
        }
        for (int nodeId = removerBase; nodeId < removerBase + removerThreads * perThread; nodeId++) {
            VectorFloat<?> v = storage.get(nodeId);
            if (v != null) {
                fail("Expected null at removed nodeId=" + nodeId + ", got " + toFloats(v)[0]);
            }
        }
    }

    @Test
    public void testConcurrentGrowthAndRead() throws Exception {
        VectorStorage storage = new VectorStorage(4);
        int count = 500;
        ExecutorService exec = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        // Writer: triggers growth repeatedly
        futures.add(exec.submit(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            for (int i = 0; i < count; i++) {
                storage.set(i, vec((float) i));
            }
        }));

        // Reader: reads concurrently, should never throw
        futures.add(exec.submit(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            for (int iter = 0; iter < 10000; iter++) {
                storage.get(iter % count); // may return null, but must not throw
            }
        }));

        latch.countDown();
        for (Future<?> f : futures) {
            f.get(); // propagates any exception
        }
        exec.shutdown();
    }

    // ---- VectorStorageRandomAccessVectorValues tests ----

    @Test
    public void testRAVVGetVector() {
        VectorStorage storage = new VectorStorage(16);
        VectorFloat<?> v = vec(1f, 2f, 3f);
        storage.set(4, v);

        VectorStorageRandomAccessVectorValues ravv =
                new VectorStorageRandomAccessVectorValues(storage, 3);
        assertSame(v, ravv.getVector(4));
        assertNull(ravv.getVector(99));
    }

    @Test
    public void testRAVVIsValueShared() {
        VectorStorage storage = new VectorStorage(16);
        VectorStorageRandomAccessVectorValues ravv =
                new VectorStorageRandomAccessVectorValues(storage, 3);
        assertTrue("isValueShared must be false", !ravv.isValueShared());
    }

    @Test
    public void testRAVVCopyReturnsSelf() {
        VectorStorage storage = new VectorStorage(16);
        VectorStorageRandomAccessVectorValues ravv =
                new VectorStorageRandomAccessVectorValues(storage, 3);
        RandomAccessVectorValues copy = ravv.copy();
        assertSame(ravv, copy);
    }

    @Test
    public void testRAVVSizeThrows() {
        VectorStorage storage = new VectorStorage(16);
        VectorStorageRandomAccessVectorValues ravv =
                new VectorStorageRandomAccessVectorValues(storage, 3);
        try {
            ravv.size();
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
            // pass
        }
    }

    @Test
    public void testRAVVDimension() {
        VectorStorage storage = new VectorStorage(16);
        VectorStorageRandomAccessVectorValues ravv =
                new VectorStorageRandomAccessVectorValues(storage, 128);
        assertTrue(ravv.dimension() == 128);
    }
}
