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
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #616 — Phase B checkpoint guard.
 *
 * <p>The pre-fix code in {@link PersistentVectorStore#writeShardAsFusedPQSegment}
 * called {@code trackProvisionalMultipartFile(segUuid, "graph")} AFTER the
 * graph multipart upload returned successfully (same for {@code "map"}).
 * When an upload aborted mid-flight — e.g. an {@code OutOfDirectMemoryError}
 * from Netty's direct-buffer pool while writing one of the file's blocks —
 * control jumped out of the function via the thrown exception WITHOUT
 * registering the partial file in {@code provisionalMultipartFiles}. The
 * subsequent {@code rollbackProvisionalArtefacts()} call therefore had no
 * record of the partial multipart file and never deleted it. The partial
 * blocks were left orphaned in remote storage, which is exactly the failure
 * mode the issue's reproduction describes (every later compaction cycle
 * re-selected the orphan, failed with {@code NoSuchKeyException}, and looped
 * indefinitely).
 *
 * <p>The fix moves both {@code trackProvisionalMultipartFile} calls to
 * BEFORE the corresponding {@code writeMultipartIndexFile} call so any
 * partial upload is eligible for rollback cleanup.
 *
 * <p>This test reproduces the partial-write scenario with an in-memory
 * {@link MemoryDataStorageManager} subclass that:
 * <ol>
 *   <li>actually performs the write (so the partial blocks "leak" into
 *       the simulated remote storage), then</li>
 *   <li>throws an {@link IOException} to simulate the mid-flight abort.</li>
 * </ol>
 * Under the pre-fix behaviour the simulated remote storage retains the
 * partial file forever; under the post-fix behaviour the rollback path
 * deletes it. The assertions below catch the regression.
 */
public class Issue616PhaseBPartialUploadRollbackTest {

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
     * Phase B partial-upload failure must leave the simulated remote storage
     * unchanged across the failing checkpoint: the pre-track + rollback
     * pipeline deletes the partial file that the failing
     * {@code writeMultipartIndexFile} call put into the storage map before
     * throwing.
     *
     * <p>The test triggers a multi-segment Phase B and arms the DSM to fail
     * on the second {@code writeMultipartIndexFile} call (the graph upload
     * of the second segment). After the failing checkpoint:
     * <ul>
     *   <li>Phase B must have failed (thrown or counter bumped).</li>
     *   <li>The set of multipart files in the DSM must equal the snapshot
     *       taken before the failing checkpoint — no partial leftover.
     *       <b>This is the assertion that fails pre-fix.</b></li>
     *   <li>{@code this.segments} must not have grown: no half-written
     *       segment leaks into the registry.</li>
     *   <li>A SEVERE log record naming the failing {@code segUuid} must
     *       have been emitted so operators can identify the victim segment
     *       from the IS log stream.</li>
     * </ul>
     */
    @Test(timeout = 60_000)
    public void phaseBPartialUploadRolledBack() throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue616-phaseB-partial").toPath();
        MidWriteFailingDsm dsm = new MidWriteFailingDsm();

        // Capture SEVERE log lines emitted by PersistentVectorStore so we
        // can assert the failing segment's UUID is named (issue #616
        // diagnostic gap — pre-fix the outer "Phase B exception" log line
        // hides shard identity).
        SevereCaptor captor = new SevereCaptor();
        Logger storeLogger = Logger.getLogger(PersistentVectorStore.class.getName());
        storeLogger.addHandler(captor);
        Level previousLevel = storeLogger.getLevel();
        storeLogger.setLevel(Level.ALL);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();

            // First, drive a successful baseline checkpoint with some vectors
            // so the store has a clean known-good state and Phase B has
            // exercised the success path at least once. We snapshot the
            // DSM's multipart-files set after this baseline so the test
            // can compare against it once the failing checkpoint completes.
            Random rng = new Random(616);
            int dim = 16;
            for (int i = 0; i < 50; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }
            store.checkpoint();

            Set<String> filesAfterBaseline = dsm.snapshotMultipartFiles();
            int segmentsAfterBaseline = store.getSegmentCount();

            // Add more vectors so the next checkpoint will emit at least one
            // new shard segment in Phase B.
            for (int i = 50; i < 100; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }

            // Arm the DSM to fail mid-write on the next multipart write call.
            // (Whichever call lands second under parallel Phase B execution
            // is the victim — the test does not assume strict ordering.)
            dsm.resetWriteTracking();
            dsm.failOnNextWrite();

            // Trigger the checkpoint. The first writeMultipartIndexFile call
            // succeeds; the second performs the put-then-throw simulation
            // (so the partial multipart file appears in the DSM's storage
            // map, just as a partial multipart upload would leave block 0..N
            // in S3 / MinIO).
            long failuresBefore = store.getConsecutiveCheckpointFailures();
            boolean threw = false;
            try {
                store.checkpoint();
            } catch (RuntimeException expected) {
                // checkpoint() declares DataStorageManagerException (a
                // RuntimeException via HerdDBInternalException); the
                // Phase B IOException catch in checkpoint() wraps the
                // IOException from our injected DSM into a
                // DataStorageManagerException, which surfaces here.
                threw = true;
            }

            // (A) Phase B must have failed — under the fix, BOTH a thrown
            // exception (the original IOException wrapped as
            // DataStorageManagerException by checkpoint()) AND the
            // consecutiveCheckpointFailures counter bump must fire (the
            // outer Phase B catch in doCheckpointFusedPQThreePhase
            // increments the counter BEFORE rethrowing). Asserting both
            // tightens the contract — a future refactor that swallows
            // the throw or skips the counter bump is caught immediately.
            assertTrue("Phase B must have surfaced as a thrown exception"
                            + " (threw=" + threw + ")", threw);
            assertTrue("Phase B must have bumped the consecutive failure"
                            + " counter (before=" + failuresBefore + ", after="
                            + store.getConsecutiveCheckpointFailures() + ")",
                    store.getConsecutiveCheckpointFailures() > failuresBefore);

            // (B) THE FIX: the partial multipart file that the failing
            // writeMultipartIndexFile call left in the DSM must have been
            // deleted by the rollback path. Net effect: the set of files
            // in the DSM after the failing checkpoint is equal to the
            // baseline snapshot (no orphan, no leak).
            //
            // Pre-fix the failing segment's "graph" key remains in the
            // DSM map: trackProvisionalMultipartFile was never called
            // (the upload threw), so the rollback's tracker did not list
            // it, and deleteMultipartIndexFile was never invoked for it.
            // The remote storage would carry that orphan indefinitely,
            // poisoning every subsequent compaction cycle (issue #616
            // reproduction).
            //
            // Post-fix the pre-track entry ensures the rollback knows
            // about the partial file and deletes it.
            Set<String> filesAfterFailure = dsm.snapshotMultipartFiles();
            Set<String> orphanedFiles = new HashSet<>(filesAfterFailure);
            orphanedFiles.removeAll(filesAfterBaseline);
            assertTrue("Phase B rollback must delete every partial multipart"
                            + " file from the failing checkpoint (issue #616)."
                            + " Orphaned: " + orphanedFiles,
                    orphanedFiles.isEmpty());

            // The DSM must also have observed at least one delete call
            // against the failing checkpoint's segUuid — confirming the
            // rollback path traversed the (now correctly-populated)
            // provisional tracker and reached the partial file.
            assertTrue("Phase B rollback must call deleteMultipartIndexFile"
                            + " for the failing segment (issue #616). Deletes"
                            + " observed: " + dsm.deletedFiles,
                    dsm.deleteCalls.get() > 0);

            // (C) The on-disk segment registry must not have grown: no
            // half-written segment from the failing Phase B can leak
            // into this.segments. recoverFromPhaseBFailure leaves the
            // registry untouched, and the failing Phase B never reached
            // persistIndexStatusMultiSegment.
            assertEquals("Failing Phase B must not grow the segment registry",
                    segmentsAfterBaseline, store.getSegmentCount());

            // (D) The SEVERE log must name the failing segUuid so
            // operators can identify which segment's upload aborted
            // (issue #616 diagnostic gap).
            assertTrue("SEVERE log must name the failing segUuid. Captured"
                            + " messages: " + captor.severeMessages,
                    captor.sawMessageContaining("Phase B: graph multipart write FAILED")
                            || captor.sawMessageContaining("Phase B: map multipart write FAILED"));
            // The IS-internal segment naming convention is "<indexUUID>_seg<segmentId>"
            // — verify a UUID-shaped token is present in the SEVERE line.
            assertTrue("SEVERE log must name a segUuid of the form '<indexUUID>_seg<N>'."
                            + " Captured messages: " + captor.severeMessages,
                    captor.sawMessageContaining("_seg"));

        } finally {
            storeLogger.removeHandler(captor);
            storeLogger.setLevel(previousLevel);
        }
    }

    /**
     * Issue #616 review item: the OOM scenario cited in the issue's
     * reproduction surfaces as Netty's {@code OutOfDirectMemoryError},
     * which extends {@link java.lang.OutOfMemoryError} (an {@link Error},
     * NOT a {@link RuntimeException}). The inner SEVERE diagnostic catch
     * in {@code writeShardAsFusedPQSegment} must catch {@link Throwable}
     * — not just {@code IOException | RuntimeException} — so the failing
     * segment's identity is logged in exactly the failure mode the issue
     * is trying to make diagnosable.
     *
     * <p>In the default parallel Phase B path
     * ({@code PHASE_B_SEGMENT_PARALLELISM=2}), {@code awaitAllOrFirstFailure}
     * wraps a thrown {@link Throwable} that is neither {@link IOException}
     * nor {@link RuntimeException} into an {@link IOException} with the
     * original {@link Error} as the cause — so the outer Phase B catch
     * arm still runs {@code rollbackProvisionalArtefacts()} +
     * {@code recoverFromPhaseBFailure()} on Error. We assert that the
     * partial multipart file is cleaned up AND the new SEVERE diagnostic
     * line names the failing {@code segUuid}.
     */
    @Test(timeout = 60_000)
    public void phaseBPartialUploadUnderErrorRolledBack() throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue616-phaseB-partial-error").toPath();
        ErrorWriteFailingDsm dsm = new ErrorWriteFailingDsm();

        SevereCaptor captor = new SevereCaptor();
        Logger storeLogger = Logger.getLogger(PersistentVectorStore.class.getName());
        storeLogger.addHandler(captor);
        Level previousLevel = storeLogger.getLevel();
        storeLogger.setLevel(Level.ALL);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();

            Random rng = new Random(616);
            int dim = 16;
            for (int i = 0; i < 50; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }
            store.checkpoint();

            Set<String> filesAfterBaseline = dsm.snapshotMultipartFiles();
            int segmentsAfterBaseline = store.getSegmentCount();

            for (int i = 50; i < 100; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }

            dsm.resetWriteTracking();
            dsm.failOnNextWriteWithError();

            // We do NOT assert that checkpoint() throws an Error — the
            // outer parallel-Phase-B path wraps it into an IOException
            // (then DataStorageManagerException via checkpoint()). What
            // matters is (i) the diagnostic SEVERE log fires (Throwable
            // catch in the patched method), (ii) the rollback path runs
            // (no orphan), and (iii) the segment registry is untouched.
            long failuresBefore = store.getConsecutiveCheckpointFailures();
            boolean failed = false;
            try {
                store.checkpoint();
            } catch (Throwable expected) {
                // Either a wrapped DataStorageManagerException (parallel
                // path), or the raw Error (serial path) — both are
                // acceptable evidence that Phase B failed. We catch
                // Throwable here precisely because the Error path is
                // exactly what we are testing.
                failed = true;
            }

            assertTrue("Phase B must have failed under Error injection",
                    failed || store.getConsecutiveCheckpointFailures() > failuresBefore);

            Set<String> filesAfterFailure = dsm.snapshotMultipartFiles();
            Set<String> orphanedFiles = new HashSet<>(filesAfterFailure);
            orphanedFiles.removeAll(filesAfterBaseline);
            assertTrue("Phase B rollback must delete every partial multipart"
                            + " file from the failing checkpoint, even when the"
                            + " injected failure is an Error (issue #616 review)."
                            + " Orphaned: " + orphanedFiles,
                    orphanedFiles.isEmpty());

            assertEquals("Failing Phase B (Error path) must not grow the"
                            + " segment registry",
                    segmentsAfterBaseline, store.getSegmentCount());

            // The diagnostic log MUST fire for the Error path — that is
            // the gap the reviewer identified. Without the Throwable
            // catch in writeShardAsFusedPQSegment, this assertion fails.
            assertTrue("SEVERE log must name the failing segUuid even when"
                            + " the upload throws an Error (issue #616 review)."
                            + " Captured messages: " + captor.severeMessages,
                    captor.sawMessageContaining("Phase B: graph multipart write FAILED")
                            || captor.sawMessageContaining("Phase B: map multipart write FAILED"));
        } finally {
            storeLogger.removeHandler(captor);
            storeLogger.setLevel(previousLevel);
        }
    }

    /**
     * {@link MemoryDataStorageManager} subclass that:
     * <ul>
     *   <li>counts {@code deleteMultipartIndexFile} calls so the test can
     *       assert the rollback path reached the partial file;</li>
     *   <li>simulates a mid-multipart-write abort: when armed, the next
     *       {@code writeMultipartIndexFile} call performs the underlying put
     *       AND THEN throws an {@link IOException}, mimicking the case where
     *       block N has already been transmitted to remote storage but
     *       block N+1's allocation aborts (the OOM scenario from the issue's
     *       reproduction). Without the "put then throw" simulation the test
     *       would not exercise the bug — the bug is precisely that a put
     *       that left bytes in the store was not tracked for cleanup.</li>
     * </ul>
     */
    private static final class MidWriteFailingDsm extends MemoryDataStorageManager {

        final AtomicInteger writeCalls = new AtomicInteger();
        final AtomicInteger deleteCalls = new AtomicInteger();
        final Set<String> deletedFiles =
                java.util.Collections.synchronizedSet(new java.util.LinkedHashSet<>());
        final AtomicBoolean armed = new AtomicBoolean(false);

        void resetWriteTracking() {
            writeCalls.set(0);
            deleteCalls.set(0);
            deletedFiles.clear();
        }

        void failOnNextWrite() {
            armed.set(true);
        }

        Set<String> snapshotMultipartFiles() {
            // multipartFiles is the package-private map in MemoryDataStorageManager.
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
        public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                              java.nio.file.Path tempFile,
                                              java.util.function.LongConsumer progress)
                throws IOException, DataStorageManagerException {
            writeCalls.incrementAndGet();
            // Compare-and-set so concurrent Phase B parallel writers race
            // for the "victim" slot deterministically — exactly ONE write
            // fails per arm, regardless of PHASE_B_SEGMENT_PARALLELISM.
            boolean takeVictimSlot = armed.compareAndSet(true, false);
            if (takeVictimSlot) {
                // Simulate "block N has been put to remote storage but
                // block N+1 aborts". We perform the actual put first so
                // the in-memory map carries the file (this is the leak
                // the rollback must clean up), then throw.
                super.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile, progress);
                throw new IOException("issue #616 test: simulated mid-multipart-write"
                        + " failure for " + tableSpace + "/" + uuid + "/" + fileType);
            }
            return super.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile, progress);
        }

        @Override
        public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType)
                throws DataStorageManagerException {
            deleteCalls.incrementAndGet();
            deletedFiles.add(tableSpace + "/" + uuid + "/" + fileType);
            super.deleteMultipartIndexFile(tableSpace, uuid, fileType);
        }

        // Pin to the reader-supplier path so test behaviour does not change
        // if a future refactor flips the direct-download default (same
        // pattern as PhaseCPrepRollbackSafetyTest).
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

    /**
     * Variant of {@link MidWriteFailingDsm} that simulates the Netty
     * {@code OutOfDirectMemoryError} scenario from the issue's reproduction:
     * performs the underlying put (so the file appears in the simulated
     * remote storage), then throws an {@link OutOfMemoryError}. The
     * {@code OutOfMemoryError} hierarchy mirrors Netty's
     * {@code io.netty.util.internal.OutOfDirectMemoryError extends
     * java.lang.OutOfMemoryError extends VirtualMachineError extends Error}.
     */
    private static final class ErrorWriteFailingDsm extends MemoryDataStorageManager {

        final AtomicInteger deleteCalls = new AtomicInteger();
        final AtomicBoolean armed = new AtomicBoolean(false);

        void resetWriteTracking() {
            deleteCalls.set(0);
        }

        void failOnNextWriteWithError() {
            armed.set(true);
        }

        Set<String> snapshotMultipartFiles() {
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
        public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                              java.nio.file.Path tempFile,
                                              java.util.function.LongConsumer progress)
                throws IOException, DataStorageManagerException {
            boolean takeVictimSlot = armed.compareAndSet(true, false);
            if (takeVictimSlot) {
                // Same put-then-throw simulation as MidWriteFailingDsm,
                // but throws an Error instead of an IOException.
                super.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile, progress);
                throw new OutOfMemoryError("issue #616 test: simulated direct-buffer"
                        + " allocation failure for " + tableSpace + "/" + uuid
                        + "/" + fileType);
            }
            return super.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile, progress);
        }

        @Override
        public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType)
                throws DataStorageManagerException {
            deleteCalls.incrementAndGet();
            super.deleteMultipartIndexFile(tableSpace, uuid, fileType);
        }

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

    /**
     * JUL handler that captures SEVERE log records emitted by
     * {@link PersistentVectorStore}, resolving {0}-style parameter
     * substitutions so assertions can grep the rendered text.
     */
    private static final class SevereCaptor extends Handler {
        final java.util.List<String> severeMessages =
                java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        @Override
        public void publish(LogRecord record) {
            if (record == null) {
                return;
            }
            if (record.getLevel().intValue() >= Level.SEVERE.intValue()) {
                String msg = record.getMessage();
                Object[] params = record.getParameters();
                if (msg != null && params != null) {
                    for (int i = 0; i < params.length; i++) {
                        msg = msg.replace("{" + i + "}", String.valueOf(params[i]));
                    }
                }
                severeMessages.add(msg);
            }
        }

        boolean sawMessageContaining(String needle) {
            synchronized (severeMessages) {
                for (String m : severeMessages) {
                    if (m != null && m.contains(needle)) {
                        return true;
                    }
                }
                return false;
            }
        }

        @Override
        public void flush() {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
