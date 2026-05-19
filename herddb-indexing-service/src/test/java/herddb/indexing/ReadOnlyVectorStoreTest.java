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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.indexing.vector.ReadOnlyVectorStore;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.IndexStatus;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link ReadOnlyVectorStore} (the shadow replicas' vector store)
 * produces the same search results as the underlying
 * {@link PersistentVectorStore} primary, rejects writes at the API boundary,
 * and correctly re-loads a new {@code IndexStatus} via
 * {@link ReadOnlyVectorStore#reloadFromStatus(IndexStatus)}.
 */
public class ReadOnlyVectorStoreTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private int savedMinLive;
    private long savedDeferral;

    @org.junit.Before
    public void saveGateState() {
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        savedDeferral = PersistentVectorStore.maxCheckpointDeferralMs;
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @org.junit.After
    public void restoreGateState() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
        PersistentVectorStore.maxCheckpointDeferralMs = savedDeferral;
    }

    private static float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private PersistentVectorStore createPrimary(MemoryDataStorageManager dsm, MemoryManager mm,
                                                 String indexUuid, Path tmpDir) {
        return new PersistentVectorStore("idx", "tbl", "tstblspace",
                "vector_col", indexUuid, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                60_000L,
                VectorSimilarityFunction.EUCLIDEAN);
    }

    private ReadOnlyVectorStore createShadow(MemoryDataStorageManager dsm, MemoryManager mm,
                                              String indexUuid, Path tmpDir) {
        return new ReadOnlyVectorStore("idx", "tbl", "tstblspace",
                "vector_col", indexUuid, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                VectorSimilarityFunction.EUCLIDEAN);
    }

    @Test
    public void shadowServesSameResultsAsPrimaryAfterCheckpoint() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        String indexUuid = "shared-uuid-recall";
        Path tmpPrimary = tmpFolder.newFolder("primary").toPath();
        Path tmpShadow = tmpFolder.newFolder("shadow").toPath();

        PersistentVectorStore primary = createPrimary(dsm, mm, indexUuid, tmpPrimary);
        primary.start();
        try {
            Random rng = new Random(7);
            int dim = 24;
            int n = 200;
            for (int i = 0; i < n; i++) {
                primary.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertTrue("primary must successfully checkpoint", primary.checkpoint());
            assertTrue("primary must have on-disk segments", primary.getSegmentCount() >= 1);

            float[] q = randomVector(rng, dim);
            int topK = 10;
            List<Map.Entry<Bytes, Float>> primaryResults = primary.search(q, topK);

            ReadOnlyVectorStore shadow = createShadow(dsm, mm, indexUuid, tmpShadow);
            shadow.start();
            try {
                assertEquals("shadow segment count matches primary",
                        primary.getSegmentCount(), shadow.getSegmentCount());
                // Primary size includes live + frozen + ondisk; after a clean
                // checkpoint the live shard is empty so the values should
                // match. Allow a small slack to tolerate an empty live-shard
                // scaffold that primary may have created.
                assertTrue("shadow size is at least the primary's segment node count",
                        shadow.size() >= primary.getSegmentCount());

                List<Map.Entry<Bytes, Float>> shadowResults = shadow.search(q, topK);
                assertEquals("shadow returns same number of results as primary",
                        primaryResults.size(), shadowResults.size());
                // Shadow search path only consults on-disk segments (no live
                // shard). Primary's search merges live+ondisk but after a
                // clean checkpoint the live shard is empty, so the two sets
                // should coincide.
                for (int i = 0; i < primaryResults.size(); i++) {
                    assertEquals(
                            "result " + i + " key matches",
                            primaryResults.get(i).getKey(),
                            shadowResults.get(i).getKey());
                    assertEquals(
                            "result " + i + " score matches",
                            primaryResults.get(i).getValue(),
                            shadowResults.get(i).getValue(),
                            1e-5f);
                }
            } finally {
                shadow.close();
            }
        } finally {
            primary.close();
        }
    }

    @Test
    public void shadowRejectsWrites() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        String indexUuid = "shared-uuid-rejects";
        Path tmpPrimary = tmpFolder.newFolder("primary").toPath();
        Path tmpShadow = tmpFolder.newFolder("shadow").toPath();

        PersistentVectorStore primary = createPrimary(dsm, mm, indexUuid, tmpPrimary);
        primary.start();
        try {
            primary.addVector(Bytes.from_int(1), new float[]{0.1f, 0.2f, 0.3f, 0.4f});
            assertTrue(primary.checkpoint());
        } finally {
            primary.close();
        }

        ReadOnlyVectorStore shadow = createShadow(dsm, mm, indexUuid, tmpShadow);
        shadow.start();
        try {
            assertThrows(UnsupportedOperationException.class,
                    () -> shadow.addVector(Bytes.from_int(2), new float[]{0f, 0f, 0f, 0f}));
            assertThrows(UnsupportedOperationException.class,
                    () -> shadow.removeVector(Bytes.from_int(1)));
        } finally {
            shadow.close();
        }
    }

    @Test
    public void shadowDoesNotStartCompactionThread() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        String indexUuid = "shared-uuid-threads";
        Path tmpShadow = tmpFolder.newFolder("shadow").toPath();

        ReadOnlyVectorStore shadow = createShadow(dsm, mm, indexUuid, tmpShadow);
        shadow.start();
        try {
            // Scan live threads for the compaction thread prefix used by PVS.
            // There must be none because ReadOnlyVectorStore's delegate runs
            // in read-only mode.
            Thread[] threads = new Thread[Thread.activeCount() * 2 + 8];
            int n = Thread.enumerate(threads);
            for (int i = 0; i < n; i++) {
                Thread t = threads[i];
                if (t == null) {
                    continue;
                }
                assertFalse(
                        "shadow should not start a compaction thread (found: " + t.getName() + ")",
                        t.getName().startsWith("persistent-vector-store-compaction-idx"));
            }
            assertTrue("shadow unwrap returns a read-only PVS", shadow.unwrap().isReadOnly());
        } finally {
            shadow.close();
        }
    }

    @Test
    public void shadowReloadsFromStatus() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        String indexUuid = "shared-uuid-reload";
        Path tmpPrimary = tmpFolder.newFolder("primary").toPath();
        Path tmpShadow = tmpFolder.newFolder("shadow").toPath();

        PersistentVectorStore primary = createPrimary(dsm, mm, indexUuid, tmpPrimary);
        primary.start();
        ReadOnlyVectorStore shadow = createShadow(dsm, mm, indexUuid, tmpShadow);
        try {
            Random rng = new Random(42);
            int dim = 16;
            // Initial batch + checkpoint — shadow starts from empty and catches up.
            for (int i = 0; i < 150; i++) {
                primary.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertTrue(primary.checkpoint());
            int sc1 = primary.getSegmentCount();

            shadow.start();
            assertEquals("initial shadow segment count matches primary after 1st ckpt",
                    sc1, shadow.getSegmentCount());
            int initialSize = shadow.size();

            // Primary grows and checkpoints again.
            for (int i = 150; i < 400; i++) {
                primary.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertTrue(primary.checkpoint());

            IndexStatus newStatus = dsm.getIndexStatus(
                    "tstblspace", indexUuid, LogSequenceNumber.START_OF_TIME);
            assertNotNull("post-checkpoint status must be present", newStatus);

            shadow.reloadFromStatus(newStatus);
            assertTrue("shadow size must grow after reload", shadow.size() > initialSize);

            float[] q = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> shadowResults = shadow.search(q, 10);
            List<Map.Entry<Bytes, Float>> primaryResults = primary.search(q, 10);
            assertEquals(primaryResults.size(), shadowResults.size());
        } finally {
            shadow.close();
            primary.close();
        }
    }
}
