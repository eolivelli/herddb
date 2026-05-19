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
package herddb.indexing.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Focused tests for the eager source-graph download introduced in issue #602.
 *
 * <p>Verifies:
 * <ol>
 *   <li><b>Temp-file cleanup</b> — no {@code herddb-compact-src-*.idx} files
 *       remain in the store's {@code tmpDirectory} after a successful (or
 *       failed) compaction cycle. A resource leak here wastes disk space and
 *       can fill the volume on busy clusters.</li>
 *   <li><b>{@code maxInputBytes} trimming</b> — when
 *       {@link PersistentVectorStore#setCompactionMaxInputBytes} is set to a
 *       very small value, the number of candidates actually merged must be
 *       capped at the minimum of 2 (the smallest pair that still constitutes a
 *       meaningful merge), and the compaction must still succeed.</li>
 *   <li><b>Download metrics advance</b> — {@link
 *       VectorIndexCompactor#COMPACTION_EAGER_DOWNLOAD_COUNT} increases by at
 *       least 2 after the cycle, proving that every source segment went through
 *       the eager-download code path (not the old block-cache path).</li>
 * </ol>
 *
 * <p>All tests use {@link MemoryDataStorageManager}, so the eager-download path
 * takes the copy-via-multipart-reader fallback (in-memory sequential read).
 * The fallback exercises the same temp-file creation and cleanup logic as the
 * direct S3/GCS path, which is sufficient to lock in the invariants above
 * without a real object store.
 */
public class EagerDownloadCompactionTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private boolean savedStreamingFlag;

    @Before
    public void setup() {
        // Disable the checkpoint deferral gate so small test segments are
        // checkpointed immediately.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
        savedStreamingFlag = VectorIndexCompactor.streamingCompactionEnabled;
        VectorIndexCompactor.streamingCompactionEnabled = true;
    }

    @After
    public void teardown() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
        VectorIndexCompactor.streamingCompactionEnabled = savedStreamingFlag;
    }

    private PersistentVectorStore createStore(Path tmpDir, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ 1L,
                /*maxBytes*/ Long.MAX_VALUE,
                /*minCount*/ 2,
                /*maxCount*/ Integer.MAX_VALUE,
                /*retentionMs*/ 0);
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
     * Counts files matching the glob pattern {@code herddb-compact-src-*.idx}
     * in the given directory. A non-zero count after compaction signals a
     * temp-file leak.
     */
    private static int countDownloadTempFiles(Path dir) throws IOException {
        int count = 0;
        try (DirectoryStream<Path> ds =
                Files.newDirectoryStream(dir, "herddb-compact-src-*.idx")) {
            for (Path ignored : ds) {
                count++;
            }
        }
        return count;
    }

    /**
     * After a successful streaming compaction cycle all eager-download temp
     * files must be deleted. This is the primary resource-leak regression gate
     * for issue #602.
     */
    @Test
    public void downloadTempFilesAreDeletedAfterSuccessfulCompaction() throws Exception {
        Path tmpDir = tmpFolder.newFolder("eager-download-cleanup").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();

            // Build 5 segments (each of 300 vectors) so the streaming path fires.
            Random rng = new Random(602);
            for (int c = 0; c < 5; c++) {
                for (int i = 0; i < 300; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, 16));
                }
                store.checkpoint();
            }
            assertTrue("need >= 2 segments before compaction",
                    store.getSegmentCount() >= 2);

            store.runCompactionCycle();

            assertEquals("compaction must succeed",
                    1, store.getCompactionSuccessesTotal());
            assertEquals("no consecutive failures expected",
                    0, store.getCompactionConsecutiveFailures());

            // All herddb-compact-src-*.idx temp files must be cleaned up.
            Path downloadDir = store.tmpDirectory();
            int remaining = countDownloadTempFiles(downloadDir);
            assertEquals(
                    "all eager-download temp files must be deleted after successful compaction;"
                            + " found " + remaining + " leftover file(s) in " + downloadDir,
                    0, remaining);
        }
    }

    /**
     * Setting {@code maxInputBytes} to 1 byte forces the trimming logic in
     * {@link PersistentVectorStore#runCompactionCycle()} to reduce candidates
     * to the minimum of 2 (two segments always proceed regardless of the
     * budget — the comment in the implementation explains why). The compaction
     * must still succeed, just with a smaller merge.
     *
     * <p>After the trimming, all remaining temp files must still be cleaned up,
     * proving that the cleanup path is not dependent on the number of inputs.
     */
    @Test
    public void maxInputBytesTrimsCandidatesToMinimumAndCleans() throws Exception {
        Path tmpDir = tmpFolder.newFolder("eager-download-trim").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // 1 byte cap: any two segments whose combined size exceeds 1 byte
            // (i.e. every real segment pair) will be trimmed to exactly 2 candidates.
            store.setCompactionMaxInputBytes(1L);
            store.start();

            // Build 5 segments so there would be more than 2 candidates without
            // the cap, making the trimming observable via getCompactionLastInputSegments.
            Random rng = new Random(602_2);
            for (int c = 0; c < 5; c++) {
                for (int i = 0; i < 300; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, 16));
                }
                store.checkpoint();
            }
            int segmentsBefore = store.getSegmentCount();
            assertTrue("need > 2 segments so trimming is observable, got " + segmentsBefore,
                    segmentsBefore > 2);

            store.runCompactionCycle();

            assertEquals("compaction must succeed despite tiny maxInputBytes cap",
                    1, store.getCompactionSuccessesTotal());

            // The trimming kept exactly 2 candidates.
            long lastInputs = store.getCompactionLastInputSegments();
            assertEquals("maxInputBytes=1 must trim candidates to the minimum of 2;"
                            + " got lastInputSegments=" + lastInputs,
                    2L, lastInputs);

            // All temp files must still be deleted even with a trimmed candidate set.
            int remaining = countDownloadTempFiles(store.tmpDirectory());
            assertEquals("eager-download temp files must be deleted even after trimmed compaction;"
                            + " found " + remaining + " leftover(s)",
                    0, remaining);
        }
    }

    /**
     * Verifies that {@link VectorIndexCompactor#COMPACTION_EAGER_DOWNLOAD_COUNT}
     * advances by at least the number of merged source segments after one
     * compaction cycle, and that the in-flight counter returns to zero.
     *
     * <p>This is an orthogonal check to
     * {@link VectorIndexStreamingCompactionTest#streamingCompactionUsesEagerDownload}
     * — the older test verifies recall-level correctness; this one focuses on
     * the exact counter semantics required by issue #602 observability.
     */
    @Test
    public void eagerDownloadMetricsAdvanceAndInflightReturnsToZero() throws Exception {
        Path tmpDir = tmpFolder.newFolder("eager-download-metrics").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        long countBefore = VectorIndexCompactor.COMPACTION_EAGER_DOWNLOAD_COUNT.get();
        long bytesBefore = VectorIndexCompactor.COMPACTION_EAGER_DOWNLOAD_BYTES.get();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();

            Random rng = new Random(602_3);
            int dim = 16;
            int numSegments = 4;
            for (int c = 0; c < numSegments; c++) {
                for (int i = 0; i < 300; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                store.checkpoint();
            }
            assertTrue("need >= 2 segments", store.getSegmentCount() >= 2);

            store.runCompactionCycle();

            assertEquals("compaction must succeed", 1, store.getCompactionSuccessesTotal());

            long countAfter = VectorIndexCompactor.COMPACTION_EAGER_DOWNLOAD_COUNT.get();
            long delta = countAfter - countBefore;
            assertTrue("COMPACTION_EAGER_DOWNLOAD_COUNT must increase by at least 2 "
                            + "(one per source segment); delta=" + delta,
                    delta >= 2);

            // The download-bytes counter must have advanced (every source graph
            // has at least 1 byte of serialised data).
            long bytesAfter = VectorIndexCompactor.COMPACTION_EAGER_DOWNLOAD_BYTES.get();
            assertTrue("COMPACTION_EAGER_DOWNLOAD_BYTES must advance; delta="
                            + (bytesAfter - bytesBefore),
                    bytesAfter > bytesBefore);

            // After compaction completes the in-flight counter must be back at zero.
            long inflight = VectorIndexCompactor.COMPACTION_EAGER_DOWNLOAD_INFLIGHT.get();
            assertEquals("COMPACTION_EAGER_DOWNLOAD_INFLIGHT must be 0 after cycle; got "
                            + inflight,
                    0L, inflight);

            // No stray temp files.
            assertFalse("no herddb-compact-src-*.idx files should remain in "
                            + store.tmpDirectory(),
                    countDownloadTempFiles(store.tmpDirectory()) > 0);
        }
    }
}
