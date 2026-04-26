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
 * Regression test for issue #255.
 *
 * <p>{@link PersistentVectorStore#allocateCompactionNodeIds(int)} bumps the
 * shared {@code nextNodeId} counter so the compactor can stage a contiguous
 * range of vectors in {@code vectorStorage}. Before the fix the bump was
 * lock-free and not coordinated with the live-shard rotation, so a
 * concurrent {@code addVector} would allocate a global nodeId far beyond the
 * active shard's {@code startNodeId} and call
 * {@code shard.builder.addGraphNode(local, vec)} with a {@code local} value
 * thousands or millions higher than any prior local id. The next Phase B
 * checkpoint trained PQ on {@code shard.nodeToPk.size()} vectors but the
 * graph contained ordinals far beyond that count, so
 * {@code FusedPQ.writeInline} → {@code PQVectors.get(ordinal)} threw
 * {@link IndexOutOfBoundsException}.
 */
public class Issue255CompactionOrdinalGapTest {

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

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, Integer.MAX_VALUE, 0);
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
     * Direct invariant test: after {@code allocateCompactionNodeIds} the
     * subsequent {@code addVector} must produce a 0-based local ordinal in a
     * fresh shard, not a {@code count}-wide gap in the previously-active shard.
     * Phase B checkpoint must then succeed without {@link IndexOutOfBoundsException}.
     */
    @Test
    public void allocationDuringIngestDoesNotPoisonPhaseBCheckpoint() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            Random rng = new Random(7);
            int dim = 16;

            // Seed the active live shard so it has a few vectors but is well
            // below the rotation cap.
            for (int i = 0; i < 10; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }

            // Simulate a graph-merge compaction reserving a large nodeId range.
            // The reservation size mirrors what real compactions consume on the
            // failing benchmark workload (hundreds of thousands of nodeIds).
            int reservation = 200_000;
            long reservedStart = store.allocateCompactionNodeIds(reservation);
            assertTrue("reservation returns a long nodeId (issue #256)",
                    reservedStart >= 0L);
            // After the fix, the next ingest must land on a fresh shard
            // whose startNodeId == reservedStart + reservation. Without the
            // fix it would land on the OLD shard with a 200K-wide gap.
            for (int i = 0; i < 10; i++) {
                store.addVector(Bytes.from_int(1_000_000 + i), vec(rng, dim));
            }

            // Phase B builds a FusedPQ segment per live shard. Without the
            // fix the active shard's local ordinal space would be
            // {0..9, 200000..200009} but PQ would only train on 20 vectors,
            // so writer.write → FusedPQ.writeInline → pqv.get(200000) →
            // IndexOutOfBoundsException. With the fix, both shards are
            // contiguous (sealed: 0..9, fresh: 0..9) so the checkpoint
            // succeeds.
            assertTrue("checkpoint must succeed after compaction nodeId reservation",
                    store.checkpoint());
            // After issue #256 the synthetic shard owns its own VectorStorage,
            // so there is no store-side release step — the storage is reclaimed
            // by the GC when VectorIndexCompactor's local reference goes away.

            // All inserted vectors must be searchable.
            int found = 0;
            Random probe = new Random(42);
            for (int q = 0; q < 50; q++) {
                List<Map.Entry<Bytes, Float>> hits = store.search(vec(probe, dim), 30);
                for (Map.Entry<Bytes, Float> h : hits) {
                    int pk = h.getKey().to_int();
                    if (pk < 10 || (pk >= 1_000_000 && pk < 1_000_010)) {
                        found++;
                    }
                }
            }
            assertTrue("inserted vectors must be searchable, found=" + found, found > 0);
        }
    }

    /**
     * Same scenario but with a real background compaction cycle: ingest
     * sealed segments, fire compaction, then ingest a few more vectors and
     * checkpoint. Without the fix the post-compaction checkpoint throws
     * inside {@code writeShardAsFusedPQSegment}.
     */
    @Test
    public void postCompactionIngestThenCheckpointSucceeds() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            Random rng = new Random(13);
            int dim = 16;

            // Build several sealed on-disk segments so compaction has work to do.
            for (int c = 0; c < 5; c++) {
                for (int i = 0; i < 300; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                store.checkpoint();
            }
            assertTrue(store.getSegmentCount() >= 4);

            // Add a few more vectors to the active live shard.
            for (int i = 0; i < 10; i++) {
                store.addVector(Bytes.from_int(900_000 + i), vec(rng, dim));
            }

            // Run a synchronous compaction cycle.
            store.runCompactionCycle();
            assertEquals(1, store.getCompactionSuccessesTotal());

            // Continue ingesting after compaction and then checkpoint.
            for (int i = 0; i < 10; i++) {
                store.addVector(Bytes.from_int(950_000 + i), vec(rng, dim));
            }
            assertTrue("post-compaction checkpoint must succeed", store.checkpoint());
        }
    }
}
