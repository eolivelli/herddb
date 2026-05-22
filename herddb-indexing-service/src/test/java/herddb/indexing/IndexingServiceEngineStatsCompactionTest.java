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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #640 — verifies that {@code GetEngineStats} surfaces a top-level
 * {@code compaction_running} flag and an aggregated {@code compaction_phase}
 * derived from every loaded {@link PersistentVectorStore}, so an operator
 * can answer "is any index on this IS currently compacting?" with a
 * single RPC.
 */
public class IndexingServiceEngineStatsCompactionTest {

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
    public void engineStatsAggregatesCompactionState() throws Exception {
        // Baseline: an idle service reports compaction_running=false /
        // compaction_phase="idle". This is also covered by
        // IndexingServiceDiagnosticsGrpcTest#testGetEngineStats; we
        // re-assert here so this test is self-contained.
        GetEngineStatsResponse idleStats = client.getEngineStats();
        Map<String, MetricValue> idleMetrics = indexMetricsByKey(idleStats);
        assertEquals(false, idleMetrics.get("compaction_running").getBoolValue());
        assertEquals("idle", idleMetrics.get("compaction_phase").getStringValue());

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
            service.getEngine().registerIndexForTest(
                    vectorIndex("idx_640", "tbl_640", "default"), pvs);

            for (int c = 0; c < numSegments; c++) {
                for (int i = 0; i < perSegment; i++) {
                    pvs.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                pvs.checkpoint();
            }
            assertEquals(numSegments, pvs.getSegmentCount());

            // Poll engine-stats while a compaction is in flight on a
            // background thread. The watcher tracks both whether the
            // aggregate `compaction_running` flag ever flipped to true
            // and whether the aggregate `compaction_phase` ever reported
            // a non-idle value.
            //
            // Note: there is a narrow window at the very start of a cycle
            // where `compaction_running=true` (beginCompactionCycle has
            // fired) but `compaction_phase` still reads as "idle" — the
            // streaming-active counter only flips inside
            // VectorIndexCompactor.rebuildSegmentStreaming around the
            // jvector compact() call, AFTER candidate selection + eager
            // download. So the two booleans are tracked independently.
            AtomicBoolean watcherStop = new AtomicBoolean();
            AtomicBoolean sawRunningTrue = new AtomicBoolean();
            AtomicBoolean sawNonIdlePhase = new AtomicBoolean();
            AtomicReference<String> lastNonIdlePhase = new AtomicReference<>(null);
            Thread watcher = new Thread(() -> {
                while (!watcherStop.get()) {
                    GetEngineStatsResponse r = client.getEngineStats();
                    Map<String, MetricValue> m = indexMetricsByKey(r);
                    if (m.get("compaction_running").getBoolValue()) {
                        sawRunningTrue.set(true);
                    }
                    String phase = m.get("compaction_phase").getStringValue();
                    if (!"idle".equals(phase)) {
                        sawNonIdlePhase.set(true);
                        lastNonIdlePhase.set(phase);
                    }
                    // Throttle the polling loop: every iteration issues an
                    // RPC, which at full spin produces tens of thousands of
                    // calls per cycle. On a single-vCPU CI runner that
                    // starves the cycle thread and amplifies flake risk
                    // (pr-reviewer pass #640).
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }, "engine-stats-watcher");
            watcher.setDaemon(true);
            watcher.start();

            pvs.runCompactionCycle();

            watcherStop.set(true);
            watcher.join(10_000);

            assertTrue("watcher must have observed compaction_running=true during"
                            + " the cycle (issue #640: aggregate flag must reflect"
                            + " in-flight compaction across all stores)",
                    sawRunningTrue.get());
            // The non-idle-phase observation is best-effort: on a very fast
            // machine the watcher may miss every sub-phase entirely. When
            // it DID see one, it must be one of the recognised values.
            if (sawNonIdlePhase.get()) {
                String observed = lastNonIdlePhase.get();
                assertTrue("non-idle phase must be one of"
                                + " {compacting-graph, writing-graph, uploading-segment};"
                                + " observed=" + observed,
                        "compacting-graph".equals(observed)
                                || "writing-graph".equals(observed)
                                || "uploading-segment".equals(observed));
            }

            // Post-cycle: the aggregate flag must flip back to false and
            // the phase aggregator must read as "idle".
            GetEngineStatsResponse post = client.getEngineStats();
            Map<String, MetricValue> postMetrics = indexMetricsByKey(post);
            assertEquals("compaction_running false after the cycle ends",
                    false, postMetrics.get("compaction_running").getBoolValue());
            assertEquals("compaction_phase aggregates back to 'idle' after the cycle",
                    "idle", postMetrics.get("compaction_phase").getStringValue());
        } finally {
            pvs.close();
        }
    }
}
