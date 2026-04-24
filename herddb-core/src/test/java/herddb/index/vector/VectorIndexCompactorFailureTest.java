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
import herddb.storage.IndexStatus;
import herddb.utils.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Failure-mode coverage for graph-merge compaction. Each test
 * injects a specific fault into a wrapped {@link MemoryDataStorageManager}
 * and asserts the observable outcome:
 * <ul>
 *   <li>compaction aborts cleanly (segment list unchanged);</li>
 *   <li>the correct {@code compaction_failures_*} counter advances;</li>
 *   <li>{@code compactionConsecutiveFailures} rises so exponential
 *       backoff can kick in;</li>
 *   <li>the reaper keeps retried entries on disk-delete failure.</li>
 * </ul>
 */
public class VectorIndexCompactorFailureTest {

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

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private PersistentVectorStore createStore(Path tmpDir, FailingDsm dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, 0);
        return store;
    }

    /** Seeds the store with 5 small segments (≥ the minCount=4 trigger). */
    private void seedFiveSegments(PersistentVectorStore store) throws Exception {
        Random rng = new Random(1);
        int dim = 16;
        for (int c = 0; c < 5; c++) {
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
            }
            store.checkpoint();
        }
        assertTrue(store.getSegmentCount() >= 4);
    }

    @Test
    public void ioErrorDuringWriteOfMergedOutput() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        FailingDsm dsm = new FailingDsm();
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            seedFiveSegments(store);
            int segmentsBefore = store.getSegmentCount();

            dsm.failNextMultipartWrites.set(1);
            store.runCompactionCycle();

            assertEquals("segment list must not change on failure",
                    segmentsBefore, store.getSegmentCount());
            assertEquals("success counter must stay at zero",
                    0, store.getCompactionSuccessesTotal());
            assertTrue("write_io or metadata_io failure must be recorded",
                    store.getCompactionFailuresWriteIoTotal()
                            + store.getCompactionFailuresMetadataIoTotal() > 0);
            assertTrue(store.getCompactionConsecutiveFailures() > 0);
        }
    }

    @Test
    public void ioErrorDuringIndexStatusPublish() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        FailingDsm dsm = new FailingDsm();
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            seedFiveSegments(store);
            int segmentsBefore = store.getSegmentCount();

            // Let segments write through; fail only the IndexStatus publish.
            dsm.failIndexCheckpoints.set(1);
            store.runCompactionCycle();

            assertEquals(segmentsBefore, store.getSegmentCount());
            assertEquals(0, store.getCompactionSuccessesTotal());
            assertTrue(store.getCompactionFailuresMetadataIoTotal()
                    + store.getCompactionFailuresWriteIoTotal() > 0);
        }
    }

    @Test
    public void partialDeleteFailureDuringReapKeepsFailingEntry() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        FailingDsm dsm = new FailingDsm();
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            seedFiveSegments(store);
            store.runCompactionCycle();
            assertEquals(1, store.getCompactionSuccessesTotal());
            int pendingBefore = store.getPendingDeletesSnapshot().size();
            assertTrue(pendingBefore > 0);

            // Fail exactly one of the upcoming delete calls.
            dsm.failNextMultipartDeletes.set(1);
            int reaped = store.reapExpiredPendingDeletes(Long.MAX_VALUE);

            assertEquals("all-but-one must succeed", pendingBefore - 1, reaped);
            assertEquals(1, store.getPendingDeletesSnapshot().size());
            assertEquals(pendingBefore - 1, store.getPendingDeletesReapedTotal());
            assertEquals(1, store.getPendingDeletesReapFailuresTotal());

            // Next reap pass (after the injected failure count hits zero)
            // must clear the retained entry.
            int reapedAgain = store.reapExpiredPendingDeletes(Long.MAX_VALUE);
            assertEquals(1, reapedAgain);
            assertTrue(store.getPendingDeletesSnapshot().isEmpty());
        }
    }

    @Test
    public void consecutiveFailuresAccumulateUntilSuccess() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        FailingDsm dsm = new FailingDsm();
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            seedFiveSegments(store);

            // First attempt: fail.
            dsm.failNextMultipartWrites.set(1);
            store.runCompactionCycle();
            long afterFirst = store.getCompactionConsecutiveFailures();
            assertTrue(afterFirst >= 1);

            // Second attempt: fail.
            dsm.failNextMultipartWrites.set(1);
            store.runCompactionCycle();
            long afterSecond = store.getCompactionConsecutiveFailures();
            assertTrue("counter must monotonically increase on repeated failures",
                    afterSecond > afterFirst);

            // Third attempt: succeed — counter must reset.
            store.runCompactionCycle();
            assertEquals(0, store.getCompactionConsecutiveFailures());
            assertEquals(1, store.getCompactionSuccessesTotal());
        }
    }

    /**
     * Failure-injection DSM. Each counter decrements on the matching
     * operation and throws while it is above zero.
     */
    private static final class FailingDsm extends MemoryDataStorageManager {
        final AtomicInteger failNextMultipartWrites = new AtomicInteger(0);
        final AtomicInteger failNextMultipartDeletes = new AtomicInteger(0);
        final AtomicInteger failIndexCheckpoints = new AtomicInteger(0);

        @Override
        public String writeMultipartIndexFile(String tableSpace, String uuid,
                                              String fileType, Path tempFile,
                                              LongConsumer progress)
                throws IOException, DataStorageManagerException {
            if (failNextMultipartWrites.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new IOException("injected write failure");
            }
            return super.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile, progress);
        }

        @Override
        public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType)
                throws DataStorageManagerException {
            if (failNextMultipartDeletes.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new DataStorageManagerException("injected delete failure");
            }
            super.deleteMultipartIndexFile(tableSpace, uuid, fileType);
        }

        @Override
        public List<herddb.core.PostCheckpointAction> indexCheckpoint(
                String tableSpace, String indexName, IndexStatus indexStatus, boolean pin)
                throws DataStorageManagerException {
            if (failIndexCheckpoints.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new DataStorageManagerException("injected IndexStatus publish failure");
            }
            return super.indexCheckpoint(tableSpace, indexName, indexStatus, pin);
        }
    }
}
