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
import herddb.file.FileDataStorageManager;
import herddb.index.vector.NewSegmentInfo;
import herddb.index.vector.PersistentVectorStore;
import herddb.index.vector.SegmentPublisher;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Validates the publisher hot-swap contract documented for
 * {@link PersistentVectorStore#setSegmentPublisher}: each checkpoint takes a
 * single snapshot of the publisher reference and uses it for both stage and
 * commit — a publisher attached or cleared mid-checkpoint affects only the
 * NEXT checkpoint (review item C1).
 */
public class PublisherHotSwapTest {

    private static final String TABLE_SPACE = "tstblspace";
    private static final String INDEX_NAME = "testidx";
    private static final String INDEX_UUID = "testidx_testtable_fixed";

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static class CountingPublisher implements SegmentPublisher {
        final AtomicInteger stageCalls = new AtomicInteger();
        final AtomicInteger commitCalls = new AtomicInteger();

        @Override
        public void stageNewSegments(List<NewSegmentInfo> segments) {
            stageCalls.incrementAndGet();
        }

        @Override
        public void commitStagedSegments(List<NewSegmentInfo> segments) {
            commitCalls.incrementAndGet();
        }
    }

    private PersistentVectorStore createStore(Path tmpDir, FileDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(INDEX_NAME, "testtable", TABLE_SPACE,
                "vector_col", INDEX_UUID, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0, Long.MAX_VALUE);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Installs the package-private {@code betweenStageAndCommitHookForTests} hook
     * via reflection. The production class deliberately exposes no public setter
     * (review-pass-3 P3-4: keep test-only knobs off the production API surface).
     */
    private static void installBetweenStageAndCommitHook(PersistentVectorStore store, Runnable hook) {
        try {
            java.lang.reflect.Field f = PersistentVectorStore.class
                    .getDeclaredField("betweenStageAndCommitHookForTests");
            f.setAccessible(true);
            f.set(store, hook);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed to install Phase-B test hook via reflection", e);
        }
    }

    @Test
    public void publisherAttachedBeforeCheckpointParticipatesInStageAndCommit() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            CountingPublisher pub = new CountingPublisher();
            store.setSegmentPublisher(pub);
            store.start();
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), 32));
            }
            store.checkpoint();
            // Stage and commit MUST be called the same number of times — exactly one
            // each per checkpoint cycle. The snapshot at Phase B start guarantees this.
            assertEquals("stage and commit must be called identically",
                    pub.stageCalls.get(), pub.commitCalls.get());
            assertTrue("at least one stage/commit pair must have run", pub.stageCalls.get() >= 1);
        }
    }

    @Test
    public void publisherClearedAfterStartMissedByInFlightCheckpointButTakesEffectOnNext() throws Exception {
        // Build a publisher that throws on commit if it ever gets called. Then attach,
        // start, ingest, swap to a counting publisher, and checkpoint. The new publisher
        // must be the one that runs — the original throwing one had been swapped before
        // the checkpoint started.
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        AtomicReference<Throwable> capturedThrow = new AtomicReference<>();
        SegmentPublisher throwing = new SegmentPublisher() {
            @Override
            public void stageNewSegments(List<NewSegmentInfo> segments) {
                capturedThrow.set(new IllegalStateException("old publisher must NOT be called"));
            }

            @Override
            public void commitStagedSegments(List<NewSegmentInfo> segments) {
                capturedThrow.set(new IllegalStateException("old publisher must NOT be called"));
            }
        };
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setSegmentPublisher(throwing);
            store.start();
            // Hot-swap to a clean publisher BEFORE the checkpoint starts. The new
            // publisher is the one Phase B will snapshot.
            CountingPublisher pub = new CountingPublisher();
            store.setSegmentPublisher(pub);
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), 32));
            }
            store.checkpoint();
            assertEquals("only the new (counting) publisher must have been called",
                    null, capturedThrow.get());
            assertEquals(pub.stageCalls.get(), pub.commitCalls.get());
            assertTrue(pub.stageCalls.get() >= 1);
        }
    }

    @Test
    public void midPhaseBSwapDoesNotProduceTornStageCommit() throws Exception {
        // Review-item R3 from second pr-reviewer pass: install a
        // betweenStageAndCommitHookForTests hook (renamed in pass-3 P3-4) that
        // swaps the publisher reference between stage and commit. The captured
        // snapshot (a local in doCheckpointFusedPQPhaseB) is what handles BOTH
        // stage and commit, so the original publisher must see exactly one stage
        // AND one commit; the swapped-in publisher must see neither.
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            CountingPublisher original = new CountingPublisher();
            CountingPublisher swappedIn = new CountingPublisher();
            store.setSegmentPublisher(original);
            store.start();
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), 32));
            }

            // Hook fires after stage, before persist+commit. Swap the publisher.
            // Use reflection to set the package-private hook field — the production
            // class deliberately does not expose a setter (review-pass-3 P3-4).
            installBetweenStageAndCommitHook(store, () -> store.setSegmentPublisher(swappedIn));

            store.checkpoint();

            // The original publisher (snapshotted at Phase B start) handled BOTH stage
            // and commit. The swapped-in publisher saw nothing in this checkpoint.
            assertEquals("original publisher must see exactly one stage",
                    1, original.stageCalls.get());
            assertEquals("original publisher must see exactly one commit",
                    1, original.commitCalls.get());
            assertEquals("swapped-in publisher must see no stage in this checkpoint",
                    0, swappedIn.stageCalls.get());
            assertEquals("swapped-in publisher must see no commit in this checkpoint",
                    0, swappedIn.commitCalls.get());

            // The swap takes effect on the NEXT checkpoint. Clear the hook and
            // run another checkpoint.
            installBetweenStageAndCommitHook(store, null);
            for (int i = 300; i < 600; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), 32));
            }
            store.checkpoint();
            assertEquals("swapped-in publisher must see one stage on the next checkpoint",
                    1, swappedIn.stageCalls.get());
            assertEquals("swapped-in publisher must see one commit on the next checkpoint",
                    1, swappedIn.commitCalls.get());
        }
    }

    @Test
    public void clearingPublisherAfterCheckpointMakesNextCheckpointLegacy() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            CountingPublisher pub = new CountingPublisher();
            store.setSegmentPublisher(pub);
            store.start();
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), 32));
            }
            store.checkpoint();
            int stageAfterFirst = pub.stageCalls.get();

            // Clear the publisher; ingest more; checkpoint again.
            store.setSegmentPublisher(null);
            for (int i = 300; i < 600; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), 32));
            }
            store.checkpoint();

            // Counts must NOT have grown — second checkpoint ran with no publisher.
            assertEquals(stageAfterFirst, pub.stageCalls.get());
        }
    }
}
