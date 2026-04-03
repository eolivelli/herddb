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

import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Recall tests for {@link PersistentVectorStore} using the siftsmall dataset
 * (10,000 base vectors, 128 dimensions, EUCLIDEAN similarity).
 * Validates that approximate nearest-neighbor search achieves acceptable recall
 * compared to the exact ground truth.
 */
public class PersistentVectorStoreRecallTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private List<float[]> baseVectors;
    private List<float[]> queryVectors;
    private List<List<Integer>> groundTruth;

    private void loadDataset() {
        baseVectors = SiftLoader.readFvecs(
                getClass().getResourceAsStream("/siftsmall/siftsmall_base.fvecs"));
        queryVectors = SiftLoader.readFvecs(
                getClass().getResourceAsStream("/siftsmall/siftsmall_query.fvecs"));
        groundTruth = SiftLoader.readIvecs(
                getClass().getResourceAsStream("/siftsmall/siftsmall_groundtruth.ivecs"));
    }

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN);
    }

    /**
     * Compute recall@K: fraction of ground truth top-K that appear in the search results.
     */
    private double computeRecall(List<Map.Entry<Bytes, Float>> results, List<Integer> truthTopK, int k) {
        Set<Integer> truthSet = new HashSet<>(truthTopK.subList(0, Math.min(k, truthTopK.size())));
        int hits = 0;
        int limit = Math.min(k, results.size());
        for (int i = 0; i < limit; i++) {
            int ordinal = results.get(i).getKey().to_int();
            if (truthSet.contains(ordinal)) {
                hits++;
            }
        }
        return (double) hits / truthSet.size();
    }

    private double measureRecall(PersistentVectorStore store, int topK) {
        double total = 0;
        for (int q = 0; q < queryVectors.size(); q++) {
            List<Map.Entry<Bytes, Float>> results = store.search(queryVectors.get(q), topK);
            total += computeRecall(results, groundTruth.get(q), topK);
        }
        return total / queryVectors.size();
    }

    @Test
    public void testRecallSingleSegment() throws Exception {
        loadDataset();
        Path tmpDir = tmpFolder.newFolder("single-segment").toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            for (int i = 0; i < baseVectors.size(); i++) {
                store.addVector(Bytes.from_int(i), baseVectors.get(i));
            }
            store.checkpoint();

            double recall10 = measureRecall(store, 10);
            double recall100 = measureRecall(store, 100);
            System.out.printf("Single segment: recall@10=%.4f, recall@100=%.4f%n", recall10, recall100);
            assertTrue("recall@10 should be >= 0.75, was " + recall10, recall10 >= 0.75);
            assertTrue("recall@100 should be >= 0.75, was " + recall100, recall100 >= 0.75);
        }
    }

    @Test
    public void testRecallMultiSegment() throws Exception {
        loadDataset();
        Path tmpDir = tmpFolder.newFolder("multi-segment").toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            int batchSize = baseVectors.size() / 3;
            for (int batch = 0; batch < 3; batch++) {
                int start = batch * batchSize;
                int end = (batch == 2) ? baseVectors.size() : start + batchSize;
                for (int i = start; i < end; i++) {
                    store.addVector(Bytes.from_int(i), baseVectors.get(i));
                }
                store.checkpoint();
            }

            double recall10 = measureRecall(store, 10);
            double recall100 = measureRecall(store, 100);
            System.out.printf("Multi-segment (3 segments): recall@10=%.4f, recall@100=%.4f%n", recall10, recall100);
            assertTrue("recall@10 should be >= 0.70, was " + recall10, recall10 >= 0.70);
            assertTrue("recall@100 should be >= 0.70, was " + recall100, recall100 >= 0.70);
        }
    }

    @Test
    public void testRecallHybrid() throws Exception {
        loadDataset();
        Path tmpDir = tmpFolder.newFolder("hybrid").toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            int splitPoint = (int) (baseVectors.size() * 0.8);
            for (int i = 0; i < splitPoint; i++) {
                store.addVector(Bytes.from_int(i), baseVectors.get(i));
            }
            store.checkpoint();

            for (int i = splitPoint; i < baseVectors.size(); i++) {
                store.addVector(Bytes.from_int(i), baseVectors.get(i));
            }

            double recall10 = measureRecall(store, 10);
            double recall100 = measureRecall(store, 100);
            System.out.printf("Hybrid (on-disk + live): recall@10=%.4f, recall@100=%.4f%n", recall10, recall100);
            assertTrue("recall@10 should be >= 0.70, was " + recall10, recall10 >= 0.70);
            assertTrue("recall@100 should be >= 0.70, was " + recall100, recall100 >= 0.70);
        }
    }
}
