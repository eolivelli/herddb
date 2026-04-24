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
import herddb.index.vector.PersistentVectorStore;
import herddb.indexing.admin.IndexingAdminClient;
import herddb.indexing.proto.DescribeIndexResponse;
import herddb.mem.MemoryDataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end test for the {@code next_node_id} gauge + gRPC field
 * added in issue #256. Exercises:
 * <ul>
 *   <li>{@link PersistentVectorStore#getNextNodeId()} reflects ingest,</li>
 *   <li>the engine-level {@code IndexDetails.nextNodeId} is populated,</li>
 *   <li>{@code DescribeIndexResponse.next_node_id} round-trips over gRPC,</li>
 *   <li>both Grafana dashboards parse and reference the new metric.</li>
 * </ul>
 */
public class Issue256GaugeAndProtoTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private EmbeddedIndexingService service;
    private IndexingAdminClient client;

    @Before
    public void setUp() throws Exception {
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

    /**
     * Registers a real {@link PersistentVectorStore} behind the
     * indexing service so we can seed {@code nextNodeId} and observe
     * it flowing through every layer.
     */
    private PersistentVectorStore registerPersistent(String table, String indexName,
                                                      Path tmpDir) throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                indexName, table, "default", "embedding",
                indexName + "-" + System.nanoTime(), tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.COSINE, Long.MAX_VALUE, null, 0, 0);
        store.start();
        service.getEngine().registerIndexForTest(
                vectorIndex(indexName, table, "default"), store);
        return store;
    }

    @Test
    public void nextNodeIdFlowsFromStoreToGrpc() throws Exception {
        Path tmpDir = folder.newFolder("idx").toPath();
        PersistentVectorStore store = registerPersistent("docs", "idx_docs", tmpDir);

        // Seed a recognisable >int32 value so the round-trip is unambiguous.
        long seeded = (long) Integer.MAX_VALUE + 4_000L;
        store.seedNextNodeIdForTest(seeded);

        // Ingest a handful of vectors so the counter moves above the seed.
        // Skip i=0 so every vector has nonzero magnitude (COSINE similarity
        // rejects the all-zero vector with NaN).
        for (int i = 1; i <= 10; i++) {
            float f = i + 1.0f;
            store.addVector(Bytes.from_int(i), new float[]{
                    f, -f, f * 0.5f, 0.1f * f,
                    f, -f, f * 0.5f, 0.1f * f,
                    f, -f, f * 0.5f, 0.1f * f,
                    f, -f, f * 0.5f, 0.1f * f
            });
        }

        // 1. Engine-level snapshot.
        IndexingServiceEngine.IndexDetails details =
                service.getEngine().describeIndex("default", "docs", "idx_docs");
        assertTrue("engine IndexDetails must reflect the seeded counter, saw "
                        + details.nextNodeId,
                details.nextNodeId >= seeded + 10L);
        assertTrue("engine IndexDetails nextNodeId must be > Integer.MAX_VALUE",
                details.nextNodeId > Integer.MAX_VALUE);

        // 2. gRPC round-trip.
        DescribeIndexResponse response = client.describeIndex("default", "docs", "idx_docs");
        assertEquals("proto next_node_id must match engine IndexDetails",
                details.nextNodeId, response.getNextNodeId());
        assertTrue("proto next_node_id must be > Integer.MAX_VALUE",
                response.getNextNodeId() > Integer.MAX_VALUE);

        // 3. After more ingest the counter must strictly increase across a
        //    second describeIndex round-trip.
        for (int i = 11; i < 30; i++) {
            float f = i + 1.0f;
            store.addVector(Bytes.from_int(i), new float[]{
                    f, -f, f * 0.5f, 0.1f * f,
                    f, -f, f * 0.5f, 0.1f * f,
                    f, -f, f * 0.5f, 0.1f * f,
                    f, -f, f * 0.5f, 0.1f * f
            });
        }
        DescribeIndexResponse response2 = client.describeIndex("default", "docs", "idx_docs");
        assertTrue("next_node_id must strictly increase with ingest",
                response2.getNextNodeId() > response.getNextNodeId());
    }

    /**
     * Reads both Grafana dashboard files and asserts each references
     * the new {@code next_node_id} metric. Jackson is not on the
     * classpath of this module so we fall back to a string scan —
     * sufficient to catch the panel being accidentally dropped.
     */
    @Test
    public void grafanaDashboardsReferenceNewMetric() throws Exception {
        File repoRoot = new File(System.getProperty("user.dir"), "..").getCanonicalFile();
        File[] dashboards = {
                new File(repoRoot,
                        "herddb-kubernetes/src/main/helm/herddb/monitor/grafana/dashboards/"
                                + "indexing-service-dashboard.json"),
                new File(repoRoot,
                        "herddb-kubernetes/src/main/helm/herddb/monitor/grafana/dashboards/"
                                + "herddb-dashboard.json"),
        };
        for (File d : dashboards) {
            assertTrue("dashboard file must exist: " + d, d.exists());
            StringBuilder contents = new StringBuilder();
            try (FileReader reader = new FileReader(d)) {
                char[] buf = new char[8192];
                int n;
                while ((n = reader.read(buf)) > 0) {
                    contents.append(buf, 0, n);
                }
            }
            String serialised = contents.toString();
            assertTrue("dashboard " + d.getName() + " must reference next_node_id",
                    serialised.contains("next_node_id"));
        }
    }
}
