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
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link PersistentVectorStore#runCompactionCycle()} drops every
 * temporary BLink storage it creates for the {@link CompactionAuthorityMap}
 * and the {@code pendingCompactionDeletes} {@link PagedPkSet} — both on the
 * happy path and when the cycle finishes without doing useful work.
 *
 * <p>This is the regression test for the cleanup contract introduced for
 * issue #290: every {@code _tmp_cmpaut_*} and {@code _tmp_pkset_cmpdel_*}
 * index name initialised by the cycle must have a matching {@code dropIndex}
 * by the time the cycle returns.
 */
public class CompactionAuthorityMapCleanupTest {

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

    /** A {@link MemoryDataStorageManager} that records every {@code initIndex} /
     * {@code dropIndex} call so the test can assert proper cleanup. */
    static final class TrackingDsm extends MemoryDataStorageManager {
        final Set<String> openIndexes = ConcurrentHashMap.newKeySet();
        final AtomicInteger initCallsCmpaut = new AtomicInteger();
        final AtomicInteger dropCallsCmpaut = new AtomicInteger();
        final AtomicInteger initCallsPkset = new AtomicInteger();
        final AtomicInteger dropCallsPkset = new AtomicInteger();

        @Override
        public void initIndex(String tableSpace, String name) throws DataStorageManagerException {
            super.initIndex(tableSpace, name);
            openIndexes.add(name);
            if (name.contains("_tmp_cmpaut_")) {
                initCallsCmpaut.incrementAndGet();
            } else if (name.contains("_tmp_pkset_")) {
                initCallsPkset.incrementAndGet();
            }
        }

        @Override
        public void dropIndex(String tableSpace, String name) throws DataStorageManagerException {
            super.dropIndex(tableSpace, name);
            openIndexes.remove(name);
            if (name.contains("_tmp_cmpaut_")) {
                dropCallsCmpaut.incrementAndGet();
            } else if (name.contains("_tmp_pkset_")) {
                dropCallsPkset.incrementAndGet();
            }
        }

        Set<String> orphanTempIndexes() {
            Set<String> out = new HashSet<>();
            for (String name : openIndexes) {
                if (name.contains("_tmp_cmpaut_") || name.contains("_tmp_pkset_")) {
                    out.add(name);
                }
            }
            return out;
        }
    }

    private PersistentVectorStore createStore(Path tmpDir, TrackingDsm dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
        // Aggressive compaction policy: any segment count >= 4 with any size triggers a cycle.
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

    @Test
    public void successfulCompactionCycleDropsTempStorages() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        TrackingDsm dsm = new TrackingDsm();
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            Random rng = new Random(11);
            int dim = 16;
            // Build 5 small segments via 5 checkpoints.
            for (int c = 0; c < 5; c++) {
                for (int i = 0; i < 300; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                store.checkpoint();
            }
            assertTrue("expected >= 4 segments before compaction",
                    store.getSegmentCount() >= 4);

            int initBefore = dsm.initCallsCmpaut.get();
            int dropBefore = dsm.dropCallsCmpaut.get();
            int psInitBefore = dsm.initCallsPkset.get();
            int psDropBefore = dsm.dropCallsPkset.get();

            store.runCompactionCycle();

            assertEquals("authority map init/drop pair on success",
                    1, dsm.initCallsCmpaut.get() - initBefore);
            assertEquals("authority map dropped on success",
                    1, dsm.dropCallsCmpaut.get() - dropBefore);
            // The cycle creates ONE pkset (pendingCompactionDeletes); checkpoint
            // ones initialised earlier should have been dropped already.
            assertEquals("compaction-deletes pkset init",
                    1, dsm.initCallsPkset.get() - psInitBefore);
            assertEquals("compaction-deletes pkset dropped",
                    1, dsm.dropCallsPkset.get() - psDropBefore);
            assertEquals("no orphan _tmp_* index after cycle",
                    java.util.Collections.emptySet(), dsm.orphanTempIndexes());
        }
    }

    @Test
    public void backToBackCyclesNeverLeak() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        TrackingDsm dsm = new TrackingDsm();
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            Random rng = new Random(11);
            int dim = 16;
            for (int c = 0; c < 12; c++) {
                for (int i = 0; i < 300; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                store.checkpoint();
            }
            int totalCycles = 0;
            for (int i = 0; i < 4; i++) {
                int beforeRuns = (int) store.getCompactionRunsTotal();
                store.runCompactionCycle();
                if (store.getCompactionRunsTotal() > beforeRuns) {
                    totalCycles++;
                }
            }
            assertTrue("at least one compaction cycle ran across the loop",
                    totalCycles > 0);
            assertEquals("no orphan _tmp_* index after multiple cycles",
                    java.util.Collections.emptySet(), dsm.orphanTempIndexes());
            assertEquals("authority map init count == drop count",
                    dsm.initCallsCmpaut.get(), dsm.dropCallsCmpaut.get());
            // pkset init count must equal drop count too. We track both
            // checkpoint and compaction sets, so the counter aggregates them.
            assertEquals("pkset init count == drop count",
                    dsm.initCallsPkset.get(), dsm.dropCallsPkset.get());
        }
    }

    @Test
    public void emptyResultCycleStillDrops() throws Exception {
        // If chooseSegmentsToMerge returns nothing the cycle exits early —
        // it must NOT have allocated any temp storage in that path.
        Path tmpDir = tmpFolder.newFolder().toPath();
        TrackingDsm dsm = new TrackingDsm();
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // Raise the min-segment-count threshold above any segment count
            // we'll reach so the cycle returns early.
            store.start();
            store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 1000, 0);

            // No segments at all => candidates.isEmpty() => early return.
            int initBefore = dsm.initCallsCmpaut.get();
            int dropBefore = dsm.dropCallsCmpaut.get();
            int psInitBefore = dsm.initCallsPkset.get();
            int psDropBefore = dsm.dropCallsPkset.get();

            store.runCompactionCycle();

            assertEquals("no authority map allocated on early return",
                    0, dsm.initCallsCmpaut.get() - initBefore);
            assertEquals("no authority map dropped on early return",
                    0, dsm.dropCallsCmpaut.get() - dropBefore);
            assertEquals("no pkset allocated on early return",
                    0, dsm.initCallsPkset.get() - psInitBefore);
            assertEquals("no pkset dropped on early return",
                    0, dsm.dropCallsPkset.get() - psDropBefore);
            assertEquals("no orphan _tmp_* index after early-return cycle",
                    java.util.Collections.emptySet(), dsm.orphanTempIndexes());
        }
    }
}
