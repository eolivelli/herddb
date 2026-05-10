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
import herddb.index.vector.PersistentVectorStore;
import herddb.indexing.optimizer.IndexOptimizerEngine;
import herddb.indexing.optimizer.MergePolicy;
import herddb.indexing.optimizer.RemoteSegmentMerger;
import herddb.indexing.segment.SegmentAssignmentListener;
import herddb.indexing.segment.SegmentAssignmentWatcher;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentRegistryPublisher;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end integration test for issue #514: the IS must automatically adopt
 * optimizer-merged segments and drop deprecated inputs without restarting.
 *
 * <p>Test flow:
 * <ol>
 *   <li>Start a real {@code TestingServer} ZooKeeper and create a
 *       {@link SegmentRegistryClient}.</li>
 *   <li>Create a {@link PersistentVectorStore} backed by a
 *       {@link FileDataStorageManager} with a {@link SegmentRegistryPublisher}
 *       attached — so every checkpoint publishes segments to ZK.</li>
 *   <li>Load the siftsmall base dataset (10 000 vectors, 128 dims, EUCLIDEAN)
 *       in 3 equal batches, each followed by a {@code checkpoint()} call, leaving
 *       3 ACTIVE segments in ZK.</li>
 *   <li>Wire a {@link SegmentAssignmentWatcher} that calls
 *       {@link PersistentVectorStore#adoptExternalSegment} / {@link
 *       PersistentVectorStore#dropSegmentByUuid} on events.</li>
 *   <li>Run {@link IndexOptimizerEngine#runOnce()} with a
 *       {@link RemoteSegmentMerger} (backed by the <em>same</em>
 *       {@link FileDataStorageManager}) and a merge policy that force-fires on
 *       any 2+ segments — so all 3 collapse into 1 in a single tick.</li>
 *   <li>Wait (up to 10 s) for the watcher to deliver events and the store to
 *       hold exactly 1 on-disk segment.</li>
 *   <li>Measure recall@10 and recall@100 against the siftsmall ground truth and
 *       assert both ≥ 0.70 — the same floor used by the multi-segment recall
 *       test in {@link PersistentVectorStoreRecallTest}.</li>
 * </ol>
 *
 * <p>Uses {@code TestingServer} + {@code FileDataStorageManager} entirely
 * in-process (no BookKeeper, no ZKTestEnv), so no
 * {@code @Category(ClusterTest.class)} annotation is needed.
 */
public class OptimizerMergeAdoptionRecallTest {

    private static final String BASE_PATH = "/herd-test-514-recall";
    private static final String TABLESPACE_UUID = "ts-514";
    private static final String TABLE_NAME = "docs";
    private static final String INDEX_NAME = "docs_v1";
    private static final String VECTOR_COL = "embedding";
    /** IS instance ID — must match the ownerSelector passed to IndexOptimizerEngine. */
    private static final int INSTANCE_ID = 0;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;

    /** Saved per-class gate so concurrent JVM forks don't observe drift. */
    private int savedMinLive;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        CountDownLatch connected = new CountDownLatch(1);
        zk = new ZooKeeper(zkServer.getConnectString(), 30_000, ev -> {
            if (ev.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue("ZK connect timed out", connected.await(30, TimeUnit.SECONDS));
        zk.create(BASE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        registry = new SegmentRegistryClient(() -> zk, BASE_PATH);
        registry.ensureRoot();

        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        // Ensure every checkpoint flushes regardless of live-vector count,
        // so small-batch tests always produce real sealed segments.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void tearDown() throws Exception {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
        if (zk != null) {
            zk.close();
        }
        if (zkServer != null) {
            zkServer.close();
        }
    }

    // -------------------------------------------------------------------------
    // Dataset helpers (same pattern as PersistentVectorStoreRecallTest)
    // -------------------------------------------------------------------------

    private List<float[]> baseVectors;
    private List<float[]> queryVectors;
    private List<List<Integer>> groundTruth;

    private void loadDataset() {
        baseVectors = SiftLoader.readFvecs(
                getClass().getResourceAsStream("/siftsmall/siftsmall_base.fvecs"));
        queryVectors = SiftLoader.readFvecs(
                getClass().getResourceAsStream("/siftsmall/siftsmall_query.fvecs"));
        groundTruth = SiftLoader.readIvecs(
                getClass().getResourceAsStream("/siftsmall/siftsmall_groundtruth.ivecs"));
    }

    private double computeRecall(List<Map.Entry<Bytes, Float>> results,
            List<Integer> truthTopK, int k) {
        Set<Integer> truthSet = new HashSet<>(truthTopK.subList(0, Math.min(k, truthTopK.size())));
        int hits = 0;
        int limit = Math.min(k, results.size());
        for (int i = 0; i < limit; i++) {
            int ordinal = results.get(i).getKey().to_int();
            if (truthSet.contains(ordinal)) {
                hits++;
            }
        }
        return (double) hits / truthSet.size();
    }

    private double measureRecall(PersistentVectorStore store, int topK) {
        double total = 0;
        for (int q = 0; q < queryVectors.size(); q++) {
            List<Map.Entry<Bytes, Float>> results = store.search(queryVectors.get(q), topK);
            total += computeRecall(results, groundTruth.get(q), topK);
        }
        return total / queryVectors.size();
    }

    // -------------------------------------------------------------------------
    // Test
    // -------------------------------------------------------------------------

    @Test
    public void optimizerMergeAdoptedWithoutRestartAndRecallPreserved() throws Exception {
        loadDataset();

        Path dataDir = tmpFolder.newFolder("data").toPath();
        Path storeTmpDir = tmpFolder.newFolder("store-tmp").toPath();
        Path optimizerTmpDir = tmpFolder.newFolder("opt-tmp").toPath();

        FileDataStorageManager dsm = new FileDataStorageManager(dataDir);
        MemoryManager mm = new MemoryManager(512 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        // ---------------------------------------------------------------
        // Create the PersistentVectorStore (siftsmall: dim=128, EUCLIDEAN)
        // ---------------------------------------------------------------
        PersistentVectorStore store = new PersistentVectorStore(
                INDEX_NAME, TABLE_NAME, TABLESPACE_UUID,
                VECTOR_COL, storeTmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN);

        // Wire the registry publisher BEFORE start() (per its Javadoc).
        String indexUUID = store.getIndexUuid();
        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLESPACE_UUID, TABLE_NAME, indexUUID, INDEX_NAME, INSTANCE_ID);
        store.setSegmentPublisher(publisher);
        store.setExternalCompactionEnabled(true);

        try {
            store.start();

            // ---------------------------------------------------------------
            // Ingest siftsmall base vectors in 3 equal batches; checkpoint
            // after each so each batch produces one sealed on-disk segment.
            // ---------------------------------------------------------------
            int batchSize = baseVectors.size() / 3;
            for (int batch = 0; batch < 3; batch++) {
                int start = batch * batchSize;
                int end = (batch == 2) ? baseVectors.size() : start + batchSize;
                for (int i = start; i < end; i++) {
                    store.addVector(Bytes.from_int(i), baseVectors.get(i));
                }
                store.checkpoint();
            }

            assertEquals("3 batches × 1 checkpoint each must produce 3 on-disk segments",
                    3, store.getOnDiskSegmentCount());

            List<VersionedSegmentMetadata> zkSegs = registry.listSegments(TABLESPACE_UUID, indexUUID);
            long activeCount = zkSegs.stream()
                    .filter(v -> v.metadata().getState()
                            == herddb.indexing.segment.SegmentState.ACTIVE)
                    .count();
            assertEquals("ZK must hold 3 ACTIVE segments after 3 checkpoints",
                    3L, activeCount);

            // ---------------------------------------------------------------
            // Wire a SegmentAssignmentWatcher to drive adoptExternalSegment /
            // dropSegmentByUuid on ZK events.
            // ---------------------------------------------------------------
            AtomicLong adoptions = new AtomicLong();
            AtomicLong drops = new AtomicLong();

            SegmentAssignmentWatcher watcher = new SegmentAssignmentWatcher(
                    registry, INSTANCE_ID,
                    new SegmentAssignmentListener() {
                        @Override
                        public void onSegmentAssigned(VersionedSegmentMetadata vsm) {
                            SegmentMetadata m = vsm.metadata();
                            try {
                                boolean added = store.adoptExternalSegment(
                                        m.getSegmentUuid(),
                                        m.getSegmentId(),
                                        m.getGraphPath(),
                                        m.getSizeBytes() - m.getMapFileSize(),
                                        m.getMapPath(),
                                        m.getMapFileSize(),
                                        m.getGeneration());
                                if (added) {
                                    // New segment actually loaded (not an idempotent re-fire).
                                    adoptions.incrementAndGet();
                                }
                            } catch (IOException | DataStorageManagerException e) {
                                // Propagate as RuntimeException so fireAssigned logs it clearly.
                                throw new RuntimeException("adoptExternalSegment failed", e);
                            }
                            // Any unchecked exception from adoptExternalSegment propagates
                            // naturally; fireAssigned will catch and log it at WARNING.
                        }

                        @Override
                        public void onSegmentReleased(SegmentMetadata previous) {
                            store.dropSegmentByUuid(previous.getSegmentUuid());
                            drops.incrementAndGet();
                        }
                    });
            try {
                // watchIndex performs the initial scan synchronously (sees the 3
                // IS-produced ACTIVE segments and fires onSegmentAssigned for each;
                // the idempotency guard silently skips them since they are already
                // in the store's segment list).
                watcher.watchIndex(TABLESPACE_UUID, indexUUID);

                // ---------------------------------------------------------------
                // Run the optimizer: SmallestFirstPolicy with maxCount=2 fires
                // on any 2+ segments, picking ALL candidates (maxBytes=MAX).
                // With 3 segments this collapses everything into 1 in a single tick.
                // ---------------------------------------------------------------
                RemoteSegmentMerger merger = new RemoteSegmentMerger(
                        dsm, optimizerTmpDir,
                        128,    // vector dimension (siftsmall)
                        16,     // graphM
                        100,    // beamWidth
                        1.2f,   // neighborOverflow
                        1.4f,   // alpha
                        VectorSimilarityFunction.EUCLIDEAN);

                IndexOptimizerEngine engine = new IndexOptimizerEngine(
                        registry, merger, TABLESPACE_UUID,
                        new MergePolicy.SmallestFirstPolicy(2, 2, 1L, Long.MAX_VALUE),
                        60_000L,    // retentionMs — long enough that reaping doesn't fire
                        () -> INSTANCE_ID);

                engine.runOnce();

                // ---------------------------------------------------------------
                // Wait for watcher events to propagate and the store to settle to
                // exactly 1 on-disk segment (the merged output) with at least 1
                // successful adoption. Both conditions must be met: just reaching
                // count==1 via drops alone (e.g. 2 drops out of 3) is insufficient.
                // ---------------------------------------------------------------
                long deadline = System.currentTimeMillis() + 30_000L;
                while (System.currentTimeMillis() < deadline
                        && (store.getOnDiskSegmentCount() != 1 || adoptions.get() < 1)) {
                    Thread.sleep(20);
                }

                assertEquals("after optimizer merge + watcher adoption the store must hold "
                                + "exactly 1 segment (got " + store.getOnDiskSegmentCount() + ")",
                        1, store.getOnDiskSegmentCount());

                assertTrue("watcher must have adopted at least 1 new (external) segment",
                        adoptions.get() >= 1);
                assertTrue("watcher must have dropped at least 3 deprecated segments",
                        drops.get() >= 3);

                // ---------------------------------------------------------------
                // Recall check: merged single-segment index must still answer
                // queries with recall ≥ 0.70 (same floor as the multi-segment
                // case in PersistentVectorStoreRecallTest).
                // ---------------------------------------------------------------
                double recall10 = measureRecall(store, 10);
                double recall100 = measureRecall(store, 100);
                System.out.printf(
                        "OptimizerMergeAdoptionRecallTest: recall@10=%.4f, recall@100=%.4f%n",
                        recall10, recall100);
                assertTrue("recall@10 should be >= 0.70 after optimizer merge, was " + recall10,
                        recall10 >= 0.70);
                assertTrue("recall@100 should be >= 0.70 after optimizer merge, was " + recall100,
                        recall100 >= 0.70);
            } finally {
                watcher.close();
            }
        } finally {
            store.close();
        }
    }

    /**
     * Verifies that an optimizer-adopted (external) segment survives a store
     * restart: after the optimizerMerge collapses 3 IS segments into 1 merged
     * segment and the store adopts it, we close and re-open the store. The
     * reloaded store must still serve exactly 1 on-disk segment and achieve
     * recall@10 ≥ 0.70 (same floor as the base recall test).
     *
     * <p>This test exercises the V5 IndexStatus format round-trip: the merged
     * segment's {@code externalStorageKey} must be written by the checkpoint
     * that follows adoption and read back correctly by the new store instance.
     */
    @Test
    public void adoptedSegmentSurvivesRestart() throws Exception {
        loadDataset();

        Path dataDir = tmpFolder.newFolder("data-restart").toPath();
        Path storeTmpDir = tmpFolder.newFolder("store-tmp-restart").toPath();
        Path storeTmpDir2 = tmpFolder.newFolder("store-tmp-restart2").toPath();
        Path optimizerTmpDir = tmpFolder.newFolder("opt-tmp-restart").toPath();

        FileDataStorageManager dsm = new FileDataStorageManager(dataDir);
        MemoryManager mm = new MemoryManager(512 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        // ---------------------------------------------------------------
        // Phase 1: create store, ingest 3 batches, run optimizer, adopt.
        // ---------------------------------------------------------------
        String capturedIndexUuid;
        PersistentVectorStore store = new PersistentVectorStore(
                INDEX_NAME, TABLE_NAME, TABLESPACE_UUID,
                VECTOR_COL, storeTmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN);

        capturedIndexUuid = store.getIndexUuid();
        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLESPACE_UUID, TABLE_NAME, capturedIndexUuid, INDEX_NAME, INSTANCE_ID);
        store.setSegmentPublisher(publisher);
        store.setExternalCompactionEnabled(true);

        try {
            store.start();

            int batchSize = baseVectors.size() / 3;
            for (int batch = 0; batch < 3; batch++) {
                int start = batch * batchSize;
                int end = (batch == 2) ? baseVectors.size() : start + batchSize;
                for (int i = start; i < end; i++) {
                    store.addVector(Bytes.from_int(i), baseVectors.get(i));
                }
                store.checkpoint();
            }
            assertEquals("pre-merge: 3 on-disk segments", 3, store.getOnDiskSegmentCount());

            AtomicLong adoptions = new AtomicLong();

            SegmentAssignmentWatcher watcher = new SegmentAssignmentWatcher(
                    registry, INSTANCE_ID,
                    new SegmentAssignmentListener() {
                        @Override
                        public void onSegmentAssigned(VersionedSegmentMetadata vsm) {
                            SegmentMetadata m = vsm.metadata();
                            try {
                                boolean added = store.adoptExternalSegment(
                                        m.getSegmentUuid(),
                                        m.getSegmentId(),
                                        m.getGraphPath(),
                                        m.getSizeBytes() - m.getMapFileSize(),
                                        m.getMapPath(),
                                        m.getMapFileSize(),
                                        m.getGeneration());
                                if (added) {
                                    adoptions.incrementAndGet();
                                }
                            } catch (IOException | DataStorageManagerException e) {
                                throw new RuntimeException("adoptExternalSegment failed", e);
                            }
                        }

                        @Override
                        public void onSegmentReleased(SegmentMetadata previous) {
                            if (previous != null) {
                                store.dropSegmentByUuid(previous.getSegmentUuid());
                            }
                        }
                    });
            try {
                watcher.watchIndex(TABLESPACE_UUID, capturedIndexUuid);

                RemoteSegmentMerger merger = new RemoteSegmentMerger(
                        dsm, optimizerTmpDir,
                        128, 16, 100, 1.2f, 1.4f,
                        VectorSimilarityFunction.EUCLIDEAN);
                IndexOptimizerEngine engine = new IndexOptimizerEngine(
                        registry, merger, TABLESPACE_UUID,
                        new MergePolicy.SmallestFirstPolicy(2, 2, 1L, Long.MAX_VALUE),
                        60_000L,
                        () -> INSTANCE_ID);
                engine.runOnce();

                long deadline = System.currentTimeMillis() + 30_000L;
                while (System.currentTimeMillis() < deadline
                        && (store.getOnDiskSegmentCount() != 1 || adoptions.get() < 1)) {
                    Thread.sleep(20);
                }
                assertEquals("post-merge: 1 on-disk segment", 1, store.getOnDiskSegmentCount());
                assertTrue("at least 1 adoption", adoptions.get() >= 1);

                // Checkpoint so the V5 IndexStatus (with externalStorageKey) is written to disk.
                store.checkpoint();
            } finally {
                watcher.close();
            }
        } finally {
            store.close();
        }

        // ---------------------------------------------------------------
        // Phase 2: re-open the SAME store (same DSM, same indexUUID).
        // The merged segment must be reloaded from the V5 IndexStatus.
        // ---------------------------------------------------------------
        // storeTmpDir2 is a fresh tmp directory for the new instance; the
        // on-disk data lives in dataDir (shared DSM).
        Files.createDirectories(storeTmpDir2);
        MemoryManager mm2 = new MemoryManager(512 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore reloaded = new PersistentVectorStore(
                INDEX_NAME, TABLE_NAME, TABLESPACE_UUID,
                VECTOR_COL, capturedIndexUuid, storeTmpDir2, dsm, mm2,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN);
        try {
            reloaded.start();

            assertEquals("reloaded store must hold exactly 1 segment (the merged one)",
                    1, reloaded.getOnDiskSegmentCount());

            double recall10 = measureRecallOn(reloaded, 10);
            double recall100 = measureRecallOn(reloaded, 100);
            System.out.printf(
                    "adoptedSegmentSurvivesRestart: recall@10=%.4f, recall@100=%.4f%n",
                    recall10, recall100);
            assertTrue("recall@10 after restart should be >= 0.70, was " + recall10,
                    recall10 >= 0.70);
            assertTrue("recall@100 after restart should be >= 0.70, was " + recall100,
                    recall100 >= 0.70);
        } finally {
            reloaded.close();
        }
    }

    /** Helper overload using an arbitrary store instance (not the field {@code store}). */
    private double measureRecallOn(PersistentVectorStore s, int topK) {
        double total = 0;
        for (int q = 0; q < queryVectors.size(); q++) {
            List<Map.Entry<Bytes, Float>> results = s.search(queryVectors.get(q), topK);
            total += computeRecall(results, groundTruth.get(q), topK);
        }
        return total / queryVectors.size();
    }
}
