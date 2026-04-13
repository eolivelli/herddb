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

package herddb.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Exercises the compaction progress instrumentation added for issue #80:
 * the {@link PersistentVectorStore} tracks how many nodes have been added
 * to the FusedPQ graph during Phase B and exposes the counters via
 * {@link PersistentVectorStore#getCompactionNodesDone()} /
 * {@link PersistentVectorStore#getCompactionNodesTotal()} /
 * {@link PersistentVectorStore#getCompactionPhase()}.
 *
 * <p>This test uses {@link MemoryDataStorageManager}, which does not
 * support multipart writes, so the upload counters stay at zero here;
 * the node counters are the signal we verify.
 */
public class PersistentVectorStoreCompactionProgressTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    public void testIdleDefaults() throws Exception {
        Path tmpDir = tmpFolder.newFolder("idle").toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            assertEquals("idle", store.getCompactionPhase());
            assertEquals(-1, store.getCompactionProgressPercent());
            assertEquals(0L, store.getCompactionNodesDone());
            assertEquals(0L, store.getCompactionNodesTotal());
            assertEquals(0L, store.getUploadBytesDone());
            assertEquals(0L, store.getUploadBytesTotal());
            assertEquals(0, store.getWritingGraphActiveCount());
            assertEquals(0, store.getUploadingActiveCount());
        }
    }

    @Test
    public void testCountersTrackPhaseBWork() throws Exception {
        Path tmpDir = tmpFolder.newFolder("phase-b").toPath();
        int numVectors = 5_000;
        int dim = 64;
        Random rng = new Random(42);

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertEquals(numVectors, store.size());

            // Background watcher samples compaction counters while Phase B
            // is running. Phase B on MemoryDataStorageManager is fast but
            // the watcher has a good chance to see a non-zero snapshot at
            // some point; more importantly, we assert that the final
            // post-checkpoint counters reflect all the work that happened.
            AtomicLong maxNodesDone = new AtomicLong();
            AtomicLong maxNodesTotal = new AtomicLong();
            AtomicBoolean watcherStop = new AtomicBoolean();
            Thread watcher = new Thread(() -> {
                while (!watcherStop.get()) {
                    long done = store.getCompactionNodesDone();
                    long total = store.getCompactionNodesTotal();
                    if (done > maxNodesDone.get()) {
                        maxNodesDone.set(done);
                    }
                    if (total > maxNodesTotal.get()) {
                        maxNodesTotal.set(total);
                    }
                }
            }, "cp-watcher");
            watcher.setDaemon(true);
            watcher.start();

            store.checkpoint();

            watcherStop.set(true);
            watcher.join(5000);

            // After a clean checkpoint the phase should settle back to idle
            // and the active-phase counters drain to zero.
            assertEquals("idle", store.getCompactionPhase());
            assertEquals(-1, store.getCompactionProgressPercent());
            assertEquals(0, store.getWritingGraphActiveCount());
            assertEquals(0, store.getUploadingActiveCount());

            // The final counters are left populated (not reset on exit) so a
            // post-hoc describe-index call shows the totals of the last
            // compaction. We expect all `numVectors` to have flowed through
            // the instrumented graph-write path.
            assertEquals("compactionNodesTotal should equal vectors processed",
                    (long) numVectors, store.getCompactionNodesTotal());
            assertEquals("compactionNodesDone should equal vectors processed",
                    (long) numVectors, store.getCompactionNodesDone());

            // The watcher should have seen the intermediate totals too
            // (same final value is fine — we only require the instance
            // counters got above zero while Phase B was active).
            assertTrue("watcher should have observed non-zero nodesTotal mid-checkpoint",
                    maxNodesTotal.get() > 0);
            assertTrue("watcher should have observed non-zero nodesDone mid-checkpoint",
                    maxNodesDone.get() > 0);
        }
    }
}
