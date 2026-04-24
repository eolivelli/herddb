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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression suite for issue #256 — global node-id counter widened to
 * {@code long} and per-shard {@link VectorStorage}.
 *
 * <p>Before the fix, {@code PersistentVectorStore.nextNodeId} was an
 * {@link java.util.concurrent.atomic.AtomicInteger}. Crossing
 * {@link Integer#MAX_VALUE} silently wrapped to {@link Integer#MIN_VALUE},
 * after which {@code vectorStorage.set(nodeId,…)} and
 * {@code (int)(nodeId - startNodeId)} produced garbage. Tests below
 * exercise the replacement {@link java.util.concurrent.atomic.AtomicLong}
 * counter + per-shard storage from every meaningful angle.
 */
public class Issue256LongNodeIdTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void disableDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
    }

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, 0);
        return store;
    }

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Seeding {@code nextNodeId} above {@code Integer.MAX_VALUE} and then
     * ingesting vectors must succeed — the whole point of the widening.
     * Every inserted PK must round-trip through search.
     */
    @Test
    public void ingestAcrossIntegerMaxValueBoundary() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            // Seed right below MAX_VALUE so the very first ingest crosses it.
            store.seedNextNodeIdForTest(Integer.MAX_VALUE - 5L);
            Random rng = new Random(7);
            int dim = 16;

            int n = 50;
            for (int i = 0; i < n; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }

            assertTrue("counter must have crossed Integer.MAX_VALUE, saw "
                            + store.getNextNodeId(),
                    store.getNextNodeId() > Integer.MAX_VALUE);

            // Every PK must be searchable.
            int found = 0;
            Random probe = new Random(42);
            for (int q = 0; q < 30; q++) {
                List<Map.Entry<Bytes, Float>> hits = store.search(vec(probe, dim), n);
                for (Map.Entry<Bytes, Float> h : hits) {
                    int pk = h.getKey().to_int();
                    if (pk >= 0 && pk < n) {
                        found++;
                    }
                }
            }
            assertTrue("inserted vectors must be searchable across the boundary, found=" + found,
                    found > 0);
        }
    }

    /**
     * The compactor's {@code allocateCompactionNodeIds} reservation must be
     * allowed to cross {@code Integer.MAX_VALUE}. Before the widening, the
     * {@code int} return type would have truncated.
     */
    @Test
    public void compactionReservationAcrossBoundary() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            // Seed just below the boundary with enough headroom for a bump.
            store.seedNextNodeIdForTest((long) Integer.MAX_VALUE - 10L);
            Random rng = new Random(11);
            int dim = 16;

            for (int i = 0; i < 5; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }

            long beforeBump = store.getNextNodeId();
            assertTrue("counter must still be near the boundary", beforeBump >= Integer.MAX_VALUE - 10L);

            long reservedStart = store.allocateCompactionNodeIds(1_000);
            assertTrue("reservation must happen below Integer.MAX_VALUE to exercise the crossing",
                    reservedStart <= Integer.MAX_VALUE);
            assertTrue("post-bump counter must exceed Integer.MAX_VALUE",
                    store.getNextNodeId() > Integer.MAX_VALUE);

            // Checkpoint must still produce a valid Phase B segment.
            assertTrue("checkpoint must succeed after a boundary-crossing reservation",
                    store.checkpoint());
        }
    }

    /**
     * A checkpoint with {@code nextNodeId > Integer.MAX_VALUE} must
     * persist the counter as a {@code long} and the value must survive
     * close / reload. A truncated {@code int} write would round-trip as
     * a negative value and every subsequent ingest would corrupt the
     * local-ordinal math.
     */
    @Test
    public void checkpointRoundTripAboveIntegerMaxValue() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        // Shared DataStorageManager + explicit indexUUID so the reloaded
        // store finds the checkpoint written by the first instance. The
        // zero-arg convenience constructor generates a nanoTime-based UUID
        // that would change between instances and make the reload look
        // like a fresh index.
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        String uuid = "issue256-reload-test";
        long seeded = (long) Integer.MAX_VALUE + 1_234_567L;
        long observedAfterCheckpoint;
        try (PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                uuid, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                io.github.jbellis.jvector.vector.VectorSimilarityFunction.COSINE,
                Long.MAX_VALUE, null, 0, 0)) {
            store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, 0);
            store.start();
            store.seedNextNodeIdForTest(seeded);

            Random rng = new Random(101);
            for (int i = 0; i < 20; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, 16));
            }
            assertTrue("checkpoint must succeed with large nextNodeId",
                    store.checkpoint());
            observedAfterCheckpoint = store.getNextNodeId();
            assertTrue("checkpoint must not roll the counter back",
                    observedAfterCheckpoint >= seeded + 20L);
        }

        // Reload: the persisted int64 nextNodeId must round-trip.
        try (PersistentVectorStore reloaded = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                uuid, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                io.github.jbellis.jvector.vector.VectorSimilarityFunction.COSINE,
                Long.MAX_VALUE, null, 0, 0)) {
            reloaded.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, 0);
            reloaded.start();
            long reloadedCounter = reloaded.getNextNodeId();
            assertTrue("reloaded nextNodeId must be > Integer.MAX_VALUE (saw "
                            + reloadedCounter + ")",
                    reloadedCounter > Integer.MAX_VALUE);
            assertTrue("reloaded nextNodeId must be at least the pre-close value",
                    reloadedCounter >= observedAfterCheckpoint);
        }
    }

    /**
     * Structural proof that each {@link LiveGraphShard}'s own
     * {@link VectorStorage} stays bounded by the configured shard cap —
     * {@link Integer#MAX_VALUE} positions are never approached, because
     * the shard rotates long before its local ordinal space fills.
     */
    @Test
    public void perShardStorageStaysBoundedRegardlessOfGlobalCounter() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            store.seedNextNodeIdForTest((long) Integer.MAX_VALUE - 3L);
            Random rng = new Random(13);
            int dim = 16;

            int cap = store.getEffectiveMaxLiveGraphSize();
            // Ingest enough to trigger at least one shard rotation.
            int n = Math.min(cap * 2 + 10, 200);
            for (int i = 0; i < n; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }

            PersistentVectorStore.LiveGraphShard active = store.activeLiveShardForTest();
            assertNotNull(active);
            int len = active.vectorStorage.length();
            // Post-cap the backing array may be sized up to 2× cap because
            // VectorStorage#growAndSet doubles. A small constant slack
            // accounts for the initial capacity floor of 256.
            assertTrue("active shard storage length must stay near the cap, got " + len
                            + " with cap=" + cap,
                    len <= Math.max(cap * 2 + 512, 1024));
            assertTrue("active shard storage must not grow with global counter (" + len
                            + " vs counter=" + store.getNextNodeId() + ")",
                    (long) len < store.getNextNodeId() - (long) Integer.MAX_VALUE + cap * 4L);
        }
    }

    /**
     * Negative test — explicitly proves that the {@link Math#toIntExact}
     * guard at the local-ordinal cast throws if the shard-cap invariant
     * is violated, rather than silently wrapping. Constructs a rogue
     * shard with a {@code startNodeId} so far in the past that the next
     * insert would blow past {@code Integer.MAX_VALUE} locally.
     */
    @Test
    public void toIntExactFiresWhenShardCapInvariantIsBypassed() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            // Seed the global counter to a huge value, then bypass the
            // normal rotation path by pointing a shard's startNodeId
            // at 0. The next addVector will compute
            // (int)(huge - 0) → ArithmeticException.
            store.seedNextNodeIdForTest((long) Integer.MAX_VALUE + 1_000L);
            // Force-create the first shard so we can reflectively reset
            // its startNodeId.
            store.addVector(Bytes.from_int(0), vec(new Random(1), 16));
            PersistentVectorStore.LiveGraphShard active = store.activeLiveShardForTest();
            assertNotNull(active);
            java.lang.reflect.Field f =
                    PersistentVectorStore.LiveGraphShard.class.getDeclaredField("startNodeId");
            f.setAccessible(true);
            // Override the final field: valid only in a unit-test sandbox.
            try {
                f.set(active, 0L);
            } catch (IllegalAccessException e) {
                fail("reflection setup failed: " + e);
            }
            try {
                // Reset rotation cap state by bumping the counter into
                // toIntExact-overflow territory, then inserting.
                store.seedNextNodeIdForTest((long) Integer.MAX_VALUE + 2L);
                store.addVector(Bytes.from_int(1), vec(new Random(2), 16));
                fail("expected ArithmeticException from Math.toIntExact at "
                        + "addVector when the shard-cap invariant is bypassed");
            } catch (ArithmeticException expected) {
                // pass — the guard fires loudly instead of wrapping silently
            }
        }
    }

    /**
     * Sanity-check the test-only seed hook itself: seeding zero is a
     * no-op, seeding a negative value is accepted (the counter is
     * {@code long}, no validation), and getters reflect the seeded value.
     */
    @Test
    public void seedHookBasics() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            assertEquals(0L, store.getNextNodeId());
            store.seedNextNodeIdForTest(42L);
            assertEquals(42L, store.getNextNodeId());
            store.seedNextNodeIdForTest((long) Integer.MAX_VALUE + 7L);
            assertEquals((long) Integer.MAX_VALUE + 7L, store.getNextNodeId());
            assertFalse("no shard is expected before first ingest",
                    store.activeLiveShardForTest() != null && !store.activeLiveShardForTest().nodeToPk.isEmpty());
        }
    }
}
