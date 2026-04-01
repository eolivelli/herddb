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

import static org.junit.Assert.*;

import herddb.index.vector.AbstractVectorStore;
import herddb.index.vector.VectorIndexManager;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Table;

import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that the IndexingServiceEngine reads the similarity property from
 * CREATE_INDEX log entries and passes it to the vector store factory.
 */
public class IndexingServiceSimilarityTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testCreateIndexWithEuclideanSimilarity() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);

        // Capture the properties passed to the factory
        AtomicReference<Map<String, String>> capturedProperties = new AtomicReference<>();
        AtomicReference<AbstractVectorStore> createdStore = new AtomicReference<>();
        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDirectory, indexProperties) -> {
            capturedProperties.set(indexProperties);
            InMemoryVectorStore store = new InMemoryVectorStore(vectorColumnName,
                    InMemoryVectorStore.parseSimilarityType(
                            indexProperties != null ? indexProperties.get(VectorIndexManager.PROP_SIMILARITY) : null));
            createdStore.set(store);
            return store;
        });

        try {
            engine.start();

            // Create a table first (needed by schema tracker)
            Table table = Table.builder()
                    .name("mytable")
                    .tablespace("default")
                    .column("pk", ColumnTypes.STRING)
                    .column("vec", ColumnTypes.FLOATARRAY)
                    .primaryKey("pk")
                    .build();
            LogEntry createTable = LogEntryFactory.createTable(table, null);
            engine.applyEntry(new LogSequenceNumber(1, 1), createTable);

            // Create a vector index with similarity=euclidean
            Index index = Index.builder()
                    .name("vidx")
                    .table("mytable")
                    .type(Index.TYPE_VECTOR)
                    .column("vec", ColumnTypes.FLOATARRAY)
                    .property(VectorIndexManager.PROP_SIMILARITY, "euclidean")
                    .build();
            LogEntry createIndex = LogEntryFactory.createIndex(index, null);
            engine.applyEntry(new LogSequenceNumber(1, 2), createIndex);

            // Verify properties were passed through
            assertNotNull("Properties should have been passed to factory", capturedProperties.get());
            assertEquals("euclidean", capturedProperties.get().get(VectorIndexManager.PROP_SIMILARITY));

            // Verify the store was created with EUCLIDEAN similarity
            assertNotNull(createdStore.get());
            assertTrue(createdStore.get() instanceof InMemoryVectorStore);
            assertEquals(InMemoryVectorStore.SimilarityType.EUCLIDEAN,
                    ((InMemoryVectorStore) createdStore.get()).getSimilarityType());
        } finally {
            engine.close();
        }
    }

    @Test
    public void testCreateIndexWithDefaultSimilarity() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);

        // Use the default factory (no custom factory set)
        AtomicReference<AbstractVectorStore> createdStore = new AtomicReference<>();
        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDirectory, indexProperties) -> {
            InMemoryVectorStore store = new InMemoryVectorStore(vectorColumnName,
                    InMemoryVectorStore.parseSimilarityType(
                            indexProperties != null ? indexProperties.get(VectorIndexManager.PROP_SIMILARITY) : null));
            createdStore.set(store);
            return store;
        });

        try {
            engine.start();

            Table table = Table.builder()
                    .name("mytable")
                    .tablespace("default")
                    .column("pk", ColumnTypes.STRING)
                    .column("vec", ColumnTypes.FLOATARRAY)
                    .primaryKey("pk")
                    .build();
            engine.applyEntry(new LogSequenceNumber(1, 1), LogEntryFactory.createTable(table, null));

            // Create a vector index WITHOUT similarity property
            Index index = Index.builder()
                    .name("vidx")
                    .table("mytable")
                    .type(Index.TYPE_VECTOR)
                    .column("vec", ColumnTypes.FLOATARRAY)
                    .build();
            engine.applyEntry(new LogSequenceNumber(1, 2), LogEntryFactory.createIndex(index, null));

            // Verify default is COSINE
            assertNotNull(createdStore.get());
            assertEquals(InMemoryVectorStore.SimilarityType.COSINE,
                    ((InMemoryVectorStore) createdStore.get()).getSimilarityType());
        } finally {
            engine.close();
        }
    }
}
