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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.IndexStatus;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * After compaction, a read-only {@code PersistentVectorStore}
 * (shadow replica) that reloads the latest {@code IndexStatus} must
 * see the merged segment and continue to serve search queries.
 */
public class VectorIndexShadowReloadTest {

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

    @Test
    public void shadowReloadAfterCompactionSeesMergedSegment() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        // Shared UUID so the shadow and the leader read/write the same
        // IndexStatus + multipart files in the shared storage manager.
        String indexUuid = "shared_idx_" + System.nanoTime();

        PersistentVectorStore leader = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                indexUuid, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                io.github.jbellis.jvector.vector.VectorSimilarityFunction.COSINE,
                Long.MAX_VALUE);
        leader.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, 0);
        leader.start();

        try {
            Random rng = new Random(11);
            int dim = 16;
            for (int c = 0; c < 5; c++) {
                for (int i = 0; i < 300; i++) {
                    leader.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                leader.checkpoint();
            }
            leader.runCompactionCycle();
            long leaderGen = leader.getCurrentIndexStatusGeneration();
            assertTrue("leader generation advanced", leaderGen > 0);
            assertEquals("leader collapsed to one segment", 1, leader.getSegmentCount());

            // Now bring up a read-only shadow against the same storage.
            PersistentVectorStore shadow = new PersistentVectorStore(
                    "testidx", "testtable", "tstblspace", "vector_col",
                    indexUuid, tmpDir, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE,
                    io.github.jbellis.jvector.vector.VectorSimilarityFunction.COSINE,
                    Long.MAX_VALUE);
            shadow.setReadOnly(true);
            shadow.start();
            try {
                assertEquals("shadow loaded exactly the merged segment", 1,
                        shadow.getSegmentCount());
                assertEquals("shadow generation matches leader", leaderGen,
                        shadow.getCurrentIndexStatusGeneration());
                List<Map.Entry<Bytes, Float>> hits = shadow.search(
                        vec(new Random(99), dim), 5);
                assertNotNull(hits);
                assertTrue("shadow must serve search queries", !hits.isEmpty());
            } finally {
                shadow.close();
            }
        } finally {
            leader.close();
        }
    }

    @Test
    public void reloadFromStatusAppliesCompactionResult() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        String indexUuid = "shared_idx_" + System.nanoTime();

        PersistentVectorStore leader = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                indexUuid, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                io.github.jbellis.jvector.vector.VectorSimilarityFunction.COSINE,
                Long.MAX_VALUE);
        leader.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, 0);
        leader.start();

        PersistentVectorStore shadow = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                indexUuid, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                io.github.jbellis.jvector.vector.VectorSimilarityFunction.COSINE,
                Long.MAX_VALUE);
        shadow.setReadOnly(true);
        shadow.start();

        try {
            Random rng = new Random(22);
            int dim = 16;
            // Leader builds 5 segments.
            for (int c = 0; c < 5; c++) {
                for (int i = 0; i < 300; i++) {
                    leader.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                leader.checkpoint();
            }
            // Shadow reloads to the pre-compaction state.
            IndexStatus pre = dsm.getIndexStatus("tstblspace", indexUuid, herddb.log.LogSequenceNumber.START_OF_TIME);
            shadow.reloadFromStatus(pre);
            assertEquals(5, shadow.getSegmentCount());

            // Leader compacts to 1 segment.
            leader.runCompactionCycle();
            assertEquals(1, leader.getSegmentCount());

            // Shadow reloads to the post-compaction state.
            IndexStatus post = dsm.getIndexStatus("tstblspace", indexUuid, herddb.log.LogSequenceNumber.START_OF_TIME);
            shadow.reloadFromStatus(post);
            assertEquals(1, shadow.getSegmentCount());
            assertEquals(leader.getCurrentIndexStatusGeneration(),
                    shadow.getCurrentIndexStatusGeneration());
        } finally {
            shadow.close();
            leader.close();
        }
    }
}
