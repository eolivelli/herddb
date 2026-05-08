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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.IndexStatus;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Locks down the failure semantics of the three-stage
 * {@code atomicSwapCompactionResult} protocol introduced for issue #462.
 *
 * <p>If the slow Stage-2 persist throws (e.g., S3 down), the in-memory
 * state must remain UNCHANGED: {@code this.segments} still references the
 * inputs, {@code currentIndexStatusGeneration} did not bump, and the
 * memory counter still matches the iterated sum. A follow-up successful
 * compaction must then succeed normally.
 *
 * <p>This is a regression guard for the order of Stage 1 vs. Stage 2:
 * the queued-pending-deletes side effect happens BEFORE the persist (same
 * as the pre-#462 code). A failure leaves {@code pendingDeletes} populated
 * — that's the existing wart, not a regression.
 */
public class PersistentVectorStoreCompactionSwapFailureTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    public void persistFailureLeavesInMemoryStateIntact() throws Exception {
        Path tmpDir = tmpFolder.newFolder("compaction-failure").toPath();
        int dim = 16;
        Random rng = new Random(2026);

        MemoryDataStorageManager underlying = new MemoryDataStorageManager();
        AtomicBoolean throwOnNext = new AtomicBoolean(false);
        ForwardingDataStorageManager dsm = new ForwardingDataStorageManager(underlying) {
            @Override
            public List<PostCheckpointAction> indexCheckpoint(String tableSpace, String uuid,
                    IndexStatus indexStatus, boolean pin) throws DataStorageManagerException {
                if (throwOnNext.compareAndSet(true, false)) {
                    throw new DataStorageManagerException("simulated S3 failure (test)");
                }
                return delegate().indexCheckpoint(tableSpace, uuid, indexStatus, pin);
            }
        };
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        try (PersistentVectorStore store = new PersistentVectorStore("testidx",
                "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN)) {
            // Aggressive compaction so 2+ segments merge.
            store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 2,
                    Integer.MAX_VALUE, 0);
            store.start();

            for (int c = 0; c < 4; c++) {
                for (int i = 0; i < 100; i++) {
                    store.addVector(Bytes.from_int(c * 1000 + i), randomVector(rng, dim));
                }
                store.checkpoint();
            }
            int segmentsBefore = store.getSegmentCount();
            assertTrue("test setup must have several segments, got " + segmentsBefore,
                    segmentsBefore >= 2);
            long genBefore = store.getCurrentIndexStatusGeneration();
            long memCounterBefore = store.getOnDiskSegmentsEstimatedMemoryBytes();
            long compactionFailuresBefore = store.getCompactionFailuresMetadataIoTotal();
            long swapWriteLockNanosBefore = store.getLastCompactionSwapWriteLockNanos();

            // Trigger the failure: next indexCheckpoint throws.
            throwOnNext.set(true);
            store.runCompactionCycle();

            // ---------- Invariant assertions ----------

            // The compaction recorded a failure (the catch block in
            // runCompactionCycle handles DataStorageManagerException as
            // METADATA_IO).
            assertEquals("compaction failure was not recorded as METADATA_IO",
                    compactionFailuresBefore + 1L,
                    store.getCompactionFailuresMetadataIoTotal());

            // Segments list unchanged: the Stage-3 publish never ran.
            assertEquals("this.segments must be unchanged after a failed compaction swap",
                    segmentsBefore, store.getSegmentCount());

            // Generation unchanged: the persist threw before bumping it.
            assertEquals("currentIndexStatusGeneration must NOT have bumped on failure",
                    genBefore, store.getCurrentIndexStatusGeneration());

            // Memory counter unchanged.
            assertEquals("on-disk-segment memory counter must be unchanged after failed swap",
                    memCounterBefore, store.getOnDiskSegmentsEstimatedMemoryBytes());

            // Diagnostic counter unchanged: the writeLock was never acquired
            // because Stage 2 (the persist) threw before Stage 3. The
            // counter must reflect either the previous successful swap's
            // value or 0 — NOT a fresh value updated on the failed cycle.
            // Catches a future regression that incorrectly stamps the
            // counter on the abort path.
            assertEquals("lastCompactionSwapWriteLockNanos must be unchanged when Stage 2 throws"
                            + " (Stage 3 is never reached, no new writeLock window)",
                    swapWriteLockNanosBefore, store.getLastCompactionSwapWriteLockNanos());

            // Searches still work.
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 5);
            assertNotNull("search returned null after failed compaction", results);
            assertFalse("search after failed compaction returned empty results",
                    results.isEmpty());

            // A follow-up compaction with a healthy DSM succeeds.
            store.runCompactionCycle();
            assertTrue("recovery compaction must have reduced segment count: before="
                    + segmentsBefore + " after=" + store.getSegmentCount(),
                    store.getSegmentCount() < segmentsBefore);

            // The recovery compaction bumped the generation.
            assertTrue("recovery compaction must have bumped the generation past "
                    + genBefore,
                    store.getCurrentIndexStatusGeneration() > genBefore);

            // Memory counter matches reality after the recovery compaction.
            long counterAfter = store.getOnDiskSegmentsEstimatedMemoryBytes();
            long iteratedSumAfter = store.sumOnDiskSegmentMemoryBytesByIterationForTesting();
            assertEquals("memory counter must equal iterated sum after recovery",
                    iteratedSumAfter, counterAfter);
        }
    }
}
