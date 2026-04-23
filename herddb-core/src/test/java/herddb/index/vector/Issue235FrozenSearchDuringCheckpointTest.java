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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #235: a search issued while the store is mid-
 * checkpoint (Phase B completed the parallel shard writes but Phase C has
 * not yet acquired the swap write lock) must still be able to traverse the
 * frozen shards and return results.
 *
 * <p>Before the fix, {@code writeShardAsFusedPQSegment} closed each frozen
 * shard's builder in its {@code finally} block during Phase B. That raced
 * with concurrent searches reading {@code frozenShards} and could corrupt
 * the jvector graph view mid-traversal. Phase C already closes the same
 * builders under the write lock, so deferring the close to Phase C is the
 * safe point.
 *
 * <p>The test pins the checkpoint thread at the existing Phase B hook (which
 * fires after the shard writes but before Phase C's write lock) and issues
 * a concurrent search from a different thread. It asserts the search
 * completes without throwing and returns a non-empty result set drawn from
 * the frozen snapshot. The intent is not to catch the race deterministically
 * — jvector's closed-builder state is benign for small in-memory graphs —
 * but to guard the end-to-end invariant that the frozen-search code path
 * remains exercised across the full Phase-B-to-Phase-C window.
 */
public class Issue235FrozenSearchDuringCheckpointTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test(timeout = 60_000)
    public void searchDuringPhaseBReturnsFrozenShardResults() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            Random rng = new Random(7);
            int dim = 32;
            int n = 200;
            for (int i = 0; i < n; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            CountDownLatch hookReached = new CountDownLatch(1);
            CountDownLatch releaseHook = new CountDownLatch(1);

            store.setCheckpointPhaseBHook(() -> {
                hookReached.countDown();
                try {
                    if (!releaseHook.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("hook release timed out");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });

            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                Future<?> checkpoint = executor.submit(() -> {
                    try {
                        store.checkpoint();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                assertTrue("checkpoint never reached Phase B hook",
                        hookReached.await(30, TimeUnit.SECONDS));

                AtomicReference<Throwable> searchError = new AtomicReference<>();
                AtomicReference<List<Map.Entry<Bytes, Float>>> searchResult = new AtomicReference<>();
                Future<?> search = executor.submit(() -> {
                    try {
                        searchResult.set(store.search(randomVector(new Random(11), dim), 10));
                    } catch (Throwable t) {
                        searchError.set(t);
                    }
                });
                search.get(30, TimeUnit.SECONDS);

                assertNull("search during Phase B must not throw: " + searchError.get(),
                        searchError.get());
                assertNotNull("search must return a result list", searchResult.get());
                assertFalse("search returned no candidates — frozen traversal is skipped",
                        searchResult.get().isEmpty());

                releaseHook.countDown();
                checkpoint.get(30, TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
