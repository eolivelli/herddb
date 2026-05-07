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
import static org.junit.Assert.assertNotNull;
import herddb.index.vector.VectorIndexManager;
import herddb.indexing.admin.IndexingAdminClient;
import herddb.indexing.proto.DescribeIndexResponse;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.utils.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link IndexingServiceEngine#describeIndex} reads the
 * per-index {@code numShards} property from the schema tracker and that the
 * gRPC {@code DescribeIndexResponse} surfaces it via {@code num_shards} so the
 * admin CLI / dashboards can show what an index was created with (issue #451).
 */
public class DescribeIndexNumShardsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private EmbeddedIndexingService service;

    @Before
    public void setUp() throws Exception {
        service = new EmbeddedIndexingService(
                folder.newFolder("log").toPath(),
                folder.newFolder("data").toPath());
        service.start();
    }

    @After
    public void tearDown() throws Exception {
        if (service != null) {
            service.close();
        }
    }

    private Index vectorIndex(String name, int numShards) {
        Index.Builder b = Index.builder()
                .name(name)
                .table("docs")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("embedding", ColumnTypes.FLOATARRAY);
        // numShards <= 0 means "do not set the property"; the engine must then
        // fall back to its default of 1 (matching getNumShardsForIndex).
        if (numShards > 0) {
            b.property(VectorIndexManager.PROP_NUM_SHARDS, String.valueOf(numShards));
        }
        return b.build();
    }

    @Test
    public void numShardsFromPropertyIsSurfacedOnEngineDescribe() {
        InMemoryVectorStore store = new InMemoryVectorStore("embedding");
        store.addVector(Bytes.from_int(1), new float[]{1f, 0f, 0f});
        service.getEngine().registerIndexForTest(vectorIndex("idx_with_shards", 4), store);

        IndexingServiceEngine.IndexDetails details =
                service.getEngine().describeIndex("default", "docs", "idx_with_shards");
        assertNotNull(details);
        assertEquals("numShards must be read from index.properties via the schema tracker",
                4, details.numShards);
    }

    @Test
    public void numShardsDefaultsToOneWhenPropertyAbsent() {
        InMemoryVectorStore store = new InMemoryVectorStore("embedding");
        store.addVector(Bytes.from_int(1), new float[]{1f, 0f, 0f});
        service.getEngine().registerIndexForTest(vectorIndex("idx_no_shards", -1), store);

        IndexingServiceEngine.IndexDetails details =
                service.getEngine().describeIndex("default", "docs", "idx_no_shards");
        assertNotNull(details);
        assertEquals("missing numShards property must default to 1",
                1, details.numShards);
    }

    @Test
    public void grpcDescribeIndexIncludesNumShards() throws Exception {
        InMemoryVectorStore store = new InMemoryVectorStore("embedding");
        store.addVector(Bytes.from_int(1), new float[]{1f, 0f, 0f});
        service.getEngine().registerIndexForTest(vectorIndex("idx_grpc", 8), store);

        try (IndexingAdminClient client = new IndexingAdminClient(service.getAddress(), 10)) {
            DescribeIndexResponse resp = client.describeIndex("default", "docs", "idx_grpc");
            assertNotNull(resp);
            assertEquals("DescribeIndexResponse.num_shards must echo the index property",
                    8, resp.getNumShards());
        }
    }

    @Test
    public void grpcDescribeIndexDefaultNumShardsIsOne() throws Exception {
        InMemoryVectorStore store = new InMemoryVectorStore("embedding");
        store.addVector(Bytes.from_int(1), new float[]{1f, 0f, 0f});
        service.getEngine().registerIndexForTest(vectorIndex("idx_grpc_default", -1), store);

        try (IndexingAdminClient client = new IndexingAdminClient(service.getAddress(), 10)) {
            DescribeIndexResponse resp = client.describeIndex("default", "docs", "idx_grpc_default");
            assertNotNull(resp);
            assertEquals(1, resp.getNumShards());
        }
    }
}
