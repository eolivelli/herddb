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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.utils.Bytes;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the diagnostic helpers added to {@link IndexingServiceEngine}
 * ({@code listIndexes}, {@code describeIndex}, {@code streamPrimaryKeys},
 * {@code getTotalEstimatedMemoryBytes}) against a seeded in-memory store.
 */
public class IndexingServiceEngineDiagnosticsTest {

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

    private Index vectorIndex(String indexName, String table, String tablespace) {
        return Index.builder()
                .name(indexName)
                .table(table)
                .tablespace(tablespace)
                .type(Index.TYPE_VECTOR)
                .column("embedding", ColumnTypes.FLOATARRAY)
                .build();
    }

    @Test
    public void testListIndexesEmptyInstance() {
        List<IndexingServiceEngine.IndexDescriptor> indexes = service.getEngine().listIndexes();
        assertNotNull(indexes);
        assertTrue("A fresh instance has no loaded indexes", indexes.isEmpty());
    }

    @Test
    public void testListIndexesReturnsSeededEntries() {
        InMemoryVectorStore store = new InMemoryVectorStore("embedding");
        store.addVector(Bytes.from_int(1), new float[]{1f, 0f, 0f});
        store.addVector(Bytes.from_int(2), new float[]{0f, 1f, 0f});
        service.getEngine().registerIndexForTest(
                vectorIndex("idx_docs", "docs", "default"), store);

        List<IndexingServiceEngine.IndexDescriptor> indexes = service.getEngine().listIndexes();
        assertEquals(1, indexes.size());
        IndexingServiceEngine.IndexDescriptor d = indexes.get(0);
        assertEquals("default", d.getTablespace());
        assertEquals("docs", d.getTable());
        assertEquals("idx_docs", d.getIndex());
        assertEquals(2L, d.getVectorCount());
        assertEquals("tailing", d.getStatus());
    }

    @Test
    public void testDescribeIndexInMemoryStore() {
        InMemoryVectorStore store = new InMemoryVectorStore("embedding");
        store.addVector(Bytes.from_int(1), new float[]{1f, 0f, 0f});
        store.addVector(Bytes.from_int(2), new float[]{0f, 1f, 0f});
        store.addVector(Bytes.from_int(3), new float[]{0f, 0f, 1f});
        service.getEngine().registerIndexForTest(
                vectorIndex("idx_docs", "docs", "default"), store);

        IndexingServiceEngine.IndexDetails details =
                service.getEngine().describeIndex("default", "docs", "idx_docs");
        assertNotNull(details);
        assertEquals("docs", details.table);
        assertEquals("idx_docs", details.index);
        assertEquals(3L, details.vectorCount);
        assertEquals("tailing", details.status);
        assertEquals("InMemoryVectorStore", details.storeClass);
        assertFalse("In-memory stores are not persistent", details.persistent);
        assertEquals("COSINE", details.similarity);
        assertEquals(1, details.segmentCount);
        assertEquals(1, details.liveShardCount);
        assertTrue("Estimated memory should be positive", details.estimatedMemoryBytes > 0);
    }

    @Test
    public void testDescribeIndexUnknownReturnsNull() {
        IndexingServiceEngine.IndexDetails details =
                service.getEngine().describeIndex("default", "does-not-exist", "missing");
        assertNull(details);
    }

    @Test
    public void testStreamPrimaryKeysReturnsAllPks() {
        InMemoryVectorStore store = new InMemoryVectorStore("embedding");
        Set<Bytes> expected = new HashSet<>();
        for (int i = 0; i < 25; i++) {
            Bytes pk = Bytes.from_int(i);
            expected.add(pk);
            store.addVector(pk, new float[]{i, -i, i * 0.5f});
        }
        service.getEngine().registerIndexForTest(
                vectorIndex("idx_docs", "docs", "default"), store);

        Set<Bytes> seen = new HashSet<>();
        long visited = service.getEngine().streamPrimaryKeys(
                "default", "docs", "idx_docs", false, 0,
                pk -> {
                    seen.add(pk);
                    return true;
                });
        assertEquals(25L, visited);
        assertEquals(expected, seen);
    }

    @Test
    public void testStreamPrimaryKeysHonoursLimit() {
        InMemoryVectorStore store = new InMemoryVectorStore("embedding");
        for (int i = 0; i < 100; i++) {
            store.addVector(Bytes.from_int(i), new float[]{i, 0f, 0f});
        }
        service.getEngine().registerIndexForTest(
                vectorIndex("idx_docs", "docs", "default"), store);

        long visited = service.getEngine().streamPrimaryKeys(
                "default", "docs", "idx_docs", false, 7,
                pk -> true);
        assertEquals(7L, visited);
    }

    @Test
    public void testStreamPrimaryKeysUnknownIndex() {
        long visited = service.getEngine().streamPrimaryKeys(
                "default", "missing", "missing", false, 0, pk -> true);
        assertEquals(0L, visited);
    }

    @Test
    public void testGetTotalEstimatedMemoryBytesSumsLoadedStores() {
        InMemoryVectorStore s1 = new InMemoryVectorStore("embedding");
        InMemoryVectorStore s2 = new InMemoryVectorStore("embedding");
        s1.addVector(Bytes.from_int(1), new float[]{1f, 2f, 3f});
        s2.addVector(Bytes.from_int(1), new float[]{4f, 5f, 6f});
        s2.addVector(Bytes.from_int(2), new float[]{7f, 8f, 9f});
        service.getEngine().registerIndexForTest(
                vectorIndex("idx_a", "ta", "default"), s1);
        service.getEngine().registerIndexForTest(
                vectorIndex("idx_b", "tb", "default"), s2);

        assertEquals(2, service.getEngine().getLoadedIndexCount());
        long total = service.getEngine().getTotalEstimatedMemoryBytes();
        assertEquals(s1.estimatedMemoryUsageBytes() + s2.estimatedMemoryUsageBytes(), total);
    }

    @Test
    public void testEnginePlumbsTailerAndApplyStats() {
        // The tailer is running in a fresh instance with no entries processed.
        assertTrue(service.getEngine().isTailerRunning());
        assertEquals(0L, service.getEngine().getTailerEntriesProcessed());

        // The apply queue is configured with the default parallelism and a
        // positive capacity; exact numbers depend on the host CPU count.
        assertTrue(service.getEngine().getApplyParallelism() >= 1);
        assertTrue(service.getEngine().getApplyQueueCapacity() > 0);
        assertEquals(0, service.getEngine().getApplyQueueSize());
        assertTrue(service.getEngine().getStartTimeMillis() > 0);
    }
}
