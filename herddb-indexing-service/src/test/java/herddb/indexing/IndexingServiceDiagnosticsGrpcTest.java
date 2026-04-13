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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import com.google.protobuf.ByteString;
import herddb.indexing.admin.IndexingAdminClient;
import herddb.indexing.proto.DescribeIndexResponse;
import herddb.indexing.proto.GetEngineStatsResponse;
import herddb.indexing.proto.GetInstanceInfoResponse;
import herddb.indexing.proto.ListIndexesResponse;
import herddb.indexing.proto.PrimaryKeysChunk;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.utils.Bytes;
import io.grpc.StatusRuntimeException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration tests for the new diagnostic RPCs
 * ({@code ListIndexes}, {@code DescribeIndex}, {@code ListPrimaryKeys},
 * {@code GetEngineStats}, {@code GetInstanceInfo}) exercised end-to-end
 * through an in-process {@link EmbeddedIndexingService} and the thin
 * {@link IndexingAdminClient} used by the CLI.
 */
public class IndexingServiceDiagnosticsGrpcTest {

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

    private void seedIndex(String table, String indexName, int vectors) {
        InMemoryVectorStore store = new InMemoryVectorStore("embedding");
        for (int i = 0; i < vectors; i++) {
            store.addVector(Bytes.from_int(i), new float[]{i, -i, i * 0.5f});
        }
        service.getEngine().registerIndexForTest(
                vectorIndex(indexName, table, "default"), store);
    }

    @Test
    public void testListIndexesEmpty() {
        ListIndexesResponse response = client.listIndexes();
        assertEquals(0, response.getIndexesCount());
    }

    @Test
    public void testListIndexesWithSeededData() {
        seedIndex("docs", "idx_docs", 5);
        seedIndex("users", "idx_users", 3);

        ListIndexesResponse response = client.listIndexes();
        assertEquals(2, response.getIndexesCount());

        Set<String> qualifiedNames = new HashSet<>();
        for (herddb.indexing.proto.IndexDescriptor d : response.getIndexesList()) {
            qualifiedNames.add(d.getTable() + "." + d.getIndex() + "=" + d.getVectorCount());
            assertEquals("default", d.getTablespace());
            assertEquals("tailing", d.getStatus());
        }
        assertTrue(qualifiedNames.contains("docs.idx_docs=5"));
        assertTrue(qualifiedNames.contains("users.idx_users=3"));
    }

    @Test
    public void testDescribeIndex() {
        seedIndex("docs", "idx_docs", 4);

        DescribeIndexResponse response = client.describeIndex("default", "docs", "idx_docs");
        assertEquals("docs", response.getBasic().getTable());
        assertEquals("idx_docs", response.getBasic().getIndex());
        assertEquals(4L, response.getBasic().getVectorCount());
        assertEquals("InMemoryVectorStore", response.getStoreClass());
        assertEquals("COSINE", response.getSimilarity());
        assertEquals(1, response.getSegmentCount());
        assertTrue(response.getEstimatedMemoryBytes() > 0);
        assertTrue(!response.getPersistent());
    }

    @Test
    public void testDescribeIndexUnknownReturnsNotFound() {
        try {
            client.describeIndex("default", "ghost", "missing");
            fail("Expected NOT_FOUND");
        } catch (StatusRuntimeException e) {
            assertEquals(io.grpc.Status.Code.NOT_FOUND, e.getStatus().getCode());
        }
    }

    @Test
    public void testListPrimaryKeysStreamsAllPks() {
        int total = 25;
        seedIndex("docs", "idx_docs", total);

        Iterator<PrimaryKeysChunk> stream = client.listPrimaryKeys(
                "default", "docs", "idx_docs", 10, false, 0);

        Set<Bytes> collected = new HashSet<>();
        boolean sawLast = false;
        while (stream.hasNext()) {
            PrimaryKeysChunk chunk = stream.next();
            for (ByteString pk : chunk.getPrimaryKeysList()) {
                collected.add(Bytes.from_array(pk.toByteArray()));
            }
            if (chunk.getLast()) {
                sawLast = true;
                break;
            }
        }
        assertTrue("Server must emit a terminal chunk", sawLast);
        assertEquals(total, collected.size());
        for (int i = 0; i < total; i++) {
            assertTrue(collected.contains(Bytes.from_int(i)));
        }
    }

    @Test
    public void testListPrimaryKeysRespectsLimit() {
        seedIndex("docs", "idx_docs", 50);

        Iterator<PrimaryKeysChunk> stream = client.listPrimaryKeys(
                "default", "docs", "idx_docs", 10, false, 12);

        int total = 0;
        while (stream.hasNext()) {
            PrimaryKeysChunk chunk = stream.next();
            total += chunk.getPrimaryKeysCount();
            if (chunk.getLast()) {
                break;
            }
        }
        assertEquals(12, total);
    }

    @Test
    public void testListPrimaryKeysEmptyIndex() {
        Iterator<PrimaryKeysChunk> stream = client.listPrimaryKeys(
                "default", "missing", "missing", 10, false, 0);
        // Unknown index yields an immediate terminal (empty) chunk.
        assertTrue(stream.hasNext());
        PrimaryKeysChunk chunk = stream.next();
        assertTrue(chunk.getLast());
        assertEquals(0, chunk.getPrimaryKeysCount());
    }

    @Test
    public void testGetEngineStats() {
        seedIndex("docs", "idx_docs", 4);

        GetEngineStatsResponse response = client.getEngineStats();
        assertTrue(response.getUptimeMillis() >= 0);
        assertTrue(response.getTailerRunning());
        assertEquals(1, response.getLoadedIndexCount());
        assertTrue(response.getApplyParallelism() >= 1);
        assertTrue(response.getApplyQueueCapacity() > 0);
        assertTrue(response.getTotalEstimatedMemoryBytes() > 0);
    }

    @Test
    public void testGetInstanceInfo() {
        GetInstanceInfoResponse response = client.getInstanceInfo();
        assertNotNull(response.getInstanceId());
        assertTrue("Instance id should include the bound port",
                response.getInstanceId().contains(":"));
        assertTrue(response.getJvmMaxHeapBytes() > 0);
        assertEquals(1, response.getNumInstances());
        assertEquals(0, response.getInstanceOrdinal());
        assertEquals("memory", response.getStorageType());
    }
}
