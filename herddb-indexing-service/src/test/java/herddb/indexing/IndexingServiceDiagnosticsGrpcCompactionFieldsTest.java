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
import herddb.indexing.admin.IndexingAdminClient;
import herddb.indexing.proto.DescribeIndexResponse;
import herddb.indexing.proto.GetEngineStatsResponse;
import herddb.indexing.proto.MetricEntry;
import herddb.indexing.proto.MetricValue;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #640 — gRPC-level verification that the new real-time compaction
 * fields ({@code compaction_batches_done/total}, {@code compaction_cycle_id},
 * {@code compaction_input_segment_count/vector_count},
 * {@code compaction_elapsed_ms}, {@code compaction_running}) flow end-to-end
 * from {@link PersistentVectorStore} through
 * {@link herddb.indexing.IndexingServiceImpl#describeIndex} and
 * {@link herddb.indexing.IndexingServiceImpl#getEngineStats} into the
 * proto response, and that the aggregate {@code compaction_phase} on
 * engine-stats reflects the per-store state correctly.
 */
public class IndexingServiceDiagnosticsGrpcCompactionFieldsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private EmbeddedIndexingService service;
    private IndexingAdminClient client;
    private int savedMinLiveVectors;

    @Before
    public void setUp() throws Exception {
        savedMinLiveVectors = PersistentVectorStore.minLiveVectorsForCheckpoint;
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
        service = new EmbeddedIndexingService(
                folder.newFolder("log").toPath(),
                folder.newFolder("data").toPath());
        service.start();
        client = new IndexingAdminClient(service.getAddress(), 10);
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (service != null) {
            service.close();
        }
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLiveVectors;
    }

    private Index vectorIndex(String indexName, String table, String tablespace) {
        return Index.builder()
                .name(indexName)
                .table(table)
                .tablespace(tablespace)
                .type(Index.TYPE_VECTOR)
                .column("embedding", ColumnTypes.FLOATARRAY)
                .build();
    }

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private static Map<String, MetricValue> indexMetricsByKey(GetEngineStatsResponse r) {
        Map<String, MetricValue> m = new HashMap<>();
        for (MetricEntry e : r.getMetricsList()) {
            m.put(e.getKey(), e.getValue());
        }
        return m;
    }

    @Test
    public void compactionFieldsPropagateAfterStreamingCycle() throws Exception {
        // Build a real PersistentVectorStore in a temp dir and register it
        // through registerIndexForTest so describe-index / engine-stats see
        // it. We drive the compaction directly on the store (the engine's
        // own compaction loop isn't started here).
        Path tmpDir = folder.newFolder("pvs-data").toPath();
        int dim = 16;
        int numSegments = 3;
        int perSegment = 200;
        Random rng = new Random(640L);
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        PersistentVectorStore pvs = new PersistentVectorStore(
                "default", "tbl_640", "tsuuid-640", "embedding",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN);
        pvs.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 2,
                Integer.MAX_VALUE, 0);
        pvs.start();
        try {
            // Register through the engine's test seam so the gRPC layer
            // resolves this PVS for describe-index lookups.
            service.getEngine().registerIndexForTest(
                    vectorIndex("idx_640", "tbl_640", "default"), pvs);

            // Build numSegments on-disk segments.
            for (int c = 0; c < numSegments; c++) {
                for (int i = 0; i < perSegment; i++) {
                    pvs.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                pvs.checkpoint();
            }
            assertEquals(numSegments, pvs.getSegmentCount());

            // Pre-cycle describe-index: compaction_running=false, cycleId
            // captures whatever the checkpoints already drove (each one
            // bumps cycleId since checkpoint Phase B now uses
            // beginCompactionCycle).
            DescribeIndexResponse pre = client.describeIndex(
                    "default", "tbl_640", "idx_640");
            assertEquals("PersistentVectorStore", pre.getStoreClass());
            assertEquals(false, pre.getCompactionRunning());
            long preCycleId = pre.getCompactionCycleId();
            assertTrue("checkpoint Phase B must have bumped cycleId at least once",
                    preCycleId >= 1L);

            // Drive a streaming compaction cycle.
            pvs.runCompactionCycle();

            // Post-cycle describe-index: cycleId bumped, running=false,
            // counters left populated (final totals of the last cycle).
            DescribeIndexResponse post = client.describeIndex(
                    "default", "tbl_640", "idx_640");
            assertEquals(false, post.getCompactionRunning());
            assertTrue("cycleId must strictly increase after runCompactionCycle:"
                            + " pre=" + preCycleId + " post=" + post.getCompactionCycleId(),
                    post.getCompactionCycleId() > preCycleId);
            assertTrue("compaction_batches_total left populated after the cycle: "
                            + post.getCompactionBatchesTotal(),
                    post.getCompactionBatchesTotal() > 0L);
            assertTrue("compaction_batches_done left populated after the cycle: "
                            + post.getCompactionBatchesDone(),
                    post.getCompactionBatchesDone() > 0L);
            assertEquals("input_segment_count reflects the merged candidates",
                    numSegments, post.getCompactionInputSegmentCount());
            assertTrue("input_vector_count is positive after a successful cycle: "
                            + post.getCompactionInputVectorCount(),
                    post.getCompactionInputVectorCount() > 0L);
            // elapsed_ms reads 0 once the cycle ends (started_nanos cleared).
            assertEquals("compaction_elapsed_ms reads 0 when idle",
                    0L, post.getCompactionElapsedMs());

            // engine-stats reflects the same idle state.
            GetEngineStatsResponse stats = client.getEngineStats();
            Map<String, MetricValue> metrics = indexMetricsByKey(stats);
            assertTrue("compaction_running key present in engine-stats",
                    metrics.containsKey("compaction_running"));
            assertEquals("compaction_running is false once the cycle ends",
                    false, metrics.get("compaction_running").getBoolValue());
            assertEquals("compaction_phase aggregates to 'idle' once the cycle ends",
                    "idle", metrics.get("compaction_phase").getStringValue());
        } finally {
            pvs.close();
        }
    }
}
