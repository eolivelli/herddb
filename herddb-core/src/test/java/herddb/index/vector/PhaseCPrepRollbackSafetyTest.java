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
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #551 (Root Cause A): zombie segments caused
 * by Phase C-prep failure rolling back artefacts that are already in
 * IndexStatus.
 *
 * <p>The pre-fix code in
 * {@link PersistentVectorStore#doCheckpointFusedPQThreePhase} called
 * {@code rollbackProvisionalArtefacts()} in the Phase C-prep failure
 * handler — which physically deleted the multipart files of the new
 * segments even though Phase B had already persisted IndexStatus with
 * those segment UUIDs. The result was a zombie: ZK shows the segment
 * as ACTIVE (because {@code reconcileWithIndexStatus} promotes any
 * PROVISIONAL znode whose UUID is in IndexStatus), but the underlying
 * files are gone.
 *
 * <p>This test forces the Phase C-prep failure path by injecting a DSM
 * that throws on the FIRST {@code multipartIndexReaderSupplier} call
 * AFTER a checkpoint Phase B has uploaded its files. The assertion is
 * the heart of the fix: NO {@code deleteMultipartIndexFile} calls
 * targeting those just-uploaded files must happen during the failure
 * recovery. Pre-fix this test would observe one delete per (graph, map)
 * pair of every Phase B segment.
 */
public class PhaseCPrepRollbackSafetyTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void disableDeferral() {
        // Allow tiny shards to seal so a single checkpoint() call produces
        // an on-disk segment with real multipart files.
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

    private PersistentVectorStore createStore(Path tmpDir, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, Integer.MAX_VALUE, 0);
        return store;
    }

    /**
     * Phase C-prep failure must NOT delete multipart files that Phase B
     * already persisted into IndexStatus.
     *
     * <p>Pre-fix behaviour (the bug): the failure handler called
     * {@code rollbackProvisionalArtefacts()} which deleted every entry in
     * {@code provisionalMultipartFiles}. Those files are durably referenced
     * by IndexStatus — deleting them creates zombie segments.
     *
     * <p>Post-fix behaviour: the failure handler clears the trackers
     * without deleting, throws the exception, and leaves the files in
     * place. The next successful checkpoint reconciles state.
     */
    @Test(timeout = 30_000)
    public void phaseCPrepFailureDoesNotDeleteFilesPersistedInIndexStatus() throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue551-phaseCprep").toPath();
        TrackingFailingDsm dsm = new TrackingFailingDsm();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();

            // First, run a successful checkpoint so we have a working store
            // with a few on-disk segments and a clean failure baseline.
            Random rng = new Random(551);
            int dim = 16;
            for (int i = 0; i < 50; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }
            store.checkpoint();

            // Snapshot the set of multipart files that exist before the
            // failing checkpoint — these were created by the successful
            // first checkpoint above. We use this to identify the NEW
            // multipart files Phase B will upload in the failing checkpoint.
            Set<String> filesBefore = dsm.snapshotMultipartFiles();

            // Now add more vectors so the next checkpoint emits at least
            // one new segment (forcing Phase B to upload new graph + map
            // files), and arm the DSM to fail Phase C-prep.
            for (int i = 50; i < 100; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }

            // Reset the delete counter to capture only the failure-path
            // deletes, and arm the failure: the next call to
            // multipartIndexReaderSupplier will throw.
            dsm.resetDeleteTracking();
            dsm.armReaderSupplierFailure(true);

            // Trigger the checkpoint. Phase B will successfully upload the
            // new multipart files AND persist IndexStatus referencing them,
            // then Phase C-prep will fail when readMultipartMapDataToTempFile
            // tries to read them back.
            //
            // pr-reviewer pass on #552: narrow the catch to the precise
            // exception types the Phase C-prep handler can rethrow
            // (IOException from readMultipartMapDataToTempFile,
            // DataStorageManagerException from the lookup, or RuntimeException
            // from any defensive wrapping). A swallowed failure (no throw,
            // no counter bump) must fail the test — that's what assertion
            // (A) below catches.
            long failuresBefore = store.getConsecutiveCheckpointFailures();
            boolean threw = false;
            try {
                store.checkpoint();
            } catch (RuntimeException expected) {
                // checkpoint() declares DataStorageManagerException, which
                // is itself a RuntimeException (via HerdDBInternalException);
                // any defensive wrap from the rollback/recover handlers is
                // also a RuntimeException. We deliberately do NOT catch
                // Throwable here — an Error must still escape so a real
                // VM-level fault doesn't get masked.
                threw = true;
            }

            // ----- Post-fix assertions -----

            // (A) The checkpoint must have failed — surfaced by either a
            //     thrown exception OR the consecutiveCheckpointFailures
            //     counter advancing. If NEITHER fired, the failure
            //     injection did not actually trip the C-prep handler and
            //     the rest of the test is meaningless. pr-reviewer pass:
            //     a swallowed failure now fails this assertion immediately.
            assertTrue("Phase C-prep must have failed — either by throw"
                            + " (threw=" + threw + ") or by counter bump"
                            + " (failuresBefore=" + failuresBefore + ", after="
                            + store.getConsecutiveCheckpointFailures() + ")",
                    threw || store.getConsecutiveCheckpointFailures() > failuresBefore);

            // (B) THE FIX: no delete calls were made against the multipart
            // files that Phase B uploaded in the failing checkpoint. The
            // failure handler must have cleared the tracker WITHOUT
            // calling deleteMultipartIndexFile.
            //
            // Pre-fix, the handler would call rollbackProvisionalArtefacts(),
            // which deletes every entry in provisionalMultipartFiles. With
            // at least one new segment emitted in Phase B, that is at
            // minimum 2 delete calls (graph + map). This assertion catches
            // any regression that re-introduces those calls.
            assertEquals("Phase C-prep failure handler must NOT delete any"
                            + " multipart files (issue #551 root cause A). Files"
                            + " deleted during recovery: " + dsm.deletedFiles,
                    0, dsm.deleteCalls.get());

            // (C) The new multipart files that Phase B uploaded BEFORE the
            // Phase C-prep failure must still exist in the DSM. Pre-fix
            // they would have been physically deleted; the segments would
            // then be zombies on the next restart.
            Set<String> filesAfter = dsm.snapshotMultipartFiles();
            Set<String> newFiles = new HashSet<>(filesAfter);
            newFiles.removeAll(filesBefore);
            assertTrue("Phase B must have uploaded at least one new multipart"
                            + " file (graph + map for a new segment) before"
                            + " Phase C-prep failed; nothing new uploaded means"
                            + " the test is not exercising the right code path."
                            + " filesBefore=" + filesBefore.size()
                            + ", filesAfter=" + filesAfter.size(),
                    !newFiles.isEmpty());
        }
    }

    /**
     * Tracking + failure-injecting wrapper around {@link MemoryDataStorageManager}.
     * Counts {@code deleteMultipartIndexFile} calls so the test can assert
     * the failure handler did NOT physically delete the artefacts in
     * IndexStatus. Optionally fails the next
     * {@code multipartIndexReaderSupplier} call to drive the Phase C-prep
     * failure path.
     */
    private static final class TrackingFailingDsm extends MemoryDataStorageManager {

        final AtomicInteger deleteCalls = new AtomicInteger();
        final Set<String> deletedFiles =
                java.util.Collections.synchronizedSet(new java.util.LinkedHashSet<>());
        final AtomicBoolean failNextReader = new AtomicBoolean(false);

        void resetDeleteTracking() {
            deleteCalls.set(0);
            deletedFiles.clear();
        }

        void armReaderSupplierFailure(boolean fail) {
            failNextReader.set(fail);
        }

        Set<String> snapshotMultipartFiles() {
            // multipartFiles is the package-private map in MemoryDataStorageManager.
            // We can't access it directly from this package, so reflect.
            try {
                java.lang.reflect.Field f = MemoryDataStorageManager.class
                        .getDeclaredField("multipartFiles");
                f.setAccessible(true);
                @SuppressWarnings("unchecked")
                java.util.Map<String, byte[]> map = (java.util.Map<String, byte[]>) f.get(this);
                return new java.util.LinkedHashSet<>(map.keySet());
            } catch (ReflectiveOperationException e) {
                throw new AssertionError(
                        "MemoryDataStorageManager.multipartFiles layout changed", e);
            }
        }

        @Override
        public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType)
                throws DataStorageManagerException {
            deleteCalls.incrementAndGet();
            deletedFiles.add(tableSpace + "/" + uuid + "/" + fileType);
            super.deleteMultipartIndexFile(tableSpace, uuid, fileType);
        }

        @Override
        public ReaderSupplier multipartIndexReaderSupplier(
                String tableSpace, String uuid, String fileType, long fileSize)
                throws DataStorageManagerException {
            // Fail exactly once per arm to ensure we trip Phase C-prep but
            // don't poison subsequent retries / cleanup paths.
            if (failNextReader.compareAndSet(true, false)) {
                throw new DataStorageManagerException(
                        "test-injected reader-supplier failure (Phase C-prep)");
            }
            return super.multipartIndexReaderSupplier(tableSpace, uuid, fileType, fileSize);
        }

        // Make the test deterministic by forcing the slower
        // multipartIndexReaderSupplier path (default
        // supportsDirectMultipartDownload() returns false anyway, but we
        // pin it here so the test cannot become silently broken if a future
        // refactor flips the default).
        @Override
        public boolean supportsDirectMultipartDownload() {
            return false;
        }

        @Override
        public void downloadMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                               long fileSize, java.nio.file.Path target)
                throws IOException, DataStorageManagerException {
            throw new UnsupportedOperationException("test pins reader-supplier path");
        }
    }
}
