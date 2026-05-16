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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.OnDiskParallelGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.RandomAccessOnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.FusedPQ;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.function.IntFunction;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link GraphWriterFactory}.
 *
 * <p>Proves that the parallel writer ({@link OnDiskParallelGraphIndexWriter})
 * and the sequential writer ({@link OnDiskGraphIndexWriter}) serialize the same
 * {@link OnHeapGraphIndex} into graphs that return identical search results,
 * and that the factory routes to the correct writer based on its static
 * configuration and the requested node count.
 *
 * <p>Plain unit test — exercises jvector and temporary files only, so it does
 * <em>not</em> carry {@code @Category(ClusterTest.class)}.
 */
public class GraphWriterFactoryTest {

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final VectorSimilarityFunction SIM = VectorSimilarityFunction.COSINE;

    private static final int DIM = 16;
    private static final int N = 2000;

    /** Graph + quantization built once and reused across all test methods. */
    private static OnHeapGraphIndex graph;
    private static VectorStorageRandomAccessVectorValues ravv;
    private static ProductQuantization pq;
    private static PQVectors pqv;
    private static VectorFloat<?>[] queries;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @BeforeClass
    public static void buildGraph() throws Exception {
        Random rng = new Random(0xC0FFEE);
        VectorStorage storage = new VectorStorage(N);
        for (int i = 0; i < N; i++) {
            float[] v = new float[DIM];
            for (int j = 0; j < DIM; j++) {
                v[j] = rng.nextFloat();
            }
            storage.set(i, VTS.createFloatVector(v));
        }
        ravv = new VectorStorageRandomAccessVectorValues(storage, DIM, N);

        BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(ravv, SIM);
        GraphIndexBuilder builder = new GraphIndexBuilder(
                bsp, DIM, 16, 100, 1.2f, 1.4f, false, false);
        for (int i = 0; i < N; i++) {
            builder.addGraphNode(i, ravv.getVector(i));
        }
        builder.cleanup();
        graph = (OnHeapGraphIndex) builder.getGraph();

        int pqSubspaces = Math.max(1, DIM / 4);
        pq = ProductQuantization.compute(ravv, pqSubspaces, 256, true);
        pqv = pq.encodeAll(ravv, ForkJoinPool.commonPool());

        queries = new VectorFloat<?>[20];
        Random qrng = new Random(0xBEEF);
        for (int q = 0; q < queries.length; q++) {
            float[] v = new float[DIM];
            for (int j = 0; j < DIM; j++) {
                v[j] = qrng.nextFloat();
            }
            queries[q] = VTS.createFloatVector(v);
        }
    }

    private List<Feature> fusedFeatures() {
        return List.of(new FusedPQ(graph.maxDegree(), pq), new InlineVectors(DIM));
    }

    /**
     * Writes {@link #graph} (with FusedPQ + InlineVectors) to {@code file}
     * through the deterministic {@code openWriter} overload.
     */
    private void writeGraph(Path file, boolean parallel, boolean directBuffers) throws Exception {
        try (RandomAccessOnDiskGraphIndexWriter writer = GraphWriterFactory.openWriter(
                graph, file, fusedFeatures(), parallel, 0, directBuffers)) {
            ImmutableGraphIndex.View view = graph.getView();
            EnumMap<FeatureId, IntFunction<Feature.State>> suppliers = new EnumMap<>(FeatureId.class);
            suppliers.put(FeatureId.FUSED_PQ, ordinal -> new FusedPQ.State(view, pqv, ordinal));
            suppliers.put(FeatureId.INLINE_VECTORS,
                    ordinal -> new InlineVectors.State(ravv.getVector(ordinal)));
            writer.write(suppliers);
        }
    }

    /**
     * Reopens the on-disk graph at {@code file} and runs every query in
     * {@link #queries}. Returns, for each query, the ordered array of result
     * node ordinals followed by the matching scores, flattened so two runs can
     * be compared with {@link org.junit.Assert#assertArrayEquals}.
     */
    private float[] searchAll(Path file, int topK) throws Exception {
        SegmentedMappedReader.Supplier supplier = new SegmentedMappedReader.Supplier(file);
        OnDiskGraphIndex disk = OnDiskGraphIndex.load(supplier);
        try (GraphSearcher searcher = new GraphSearcher(disk)) {
            OnDiskGraphIndex.View view = (OnDiskGraphIndex.View) searcher.getView();
            float[] out = new float[queries.length * topK * 2];
            int pos = 0;
            for (VectorFloat<?> q : queries) {
                ScoreFunction.ExactScoreFunction reranker = view.rerankerFor(q, SIM);
                DefaultSearchScoreProvider ssp = new DefaultSearchScoreProvider(reranker);
                SearchResult result = searcher.search(ssp, topK, topK, 0.0f, 0.0f, Bits.ALL);
                SearchResult.NodeScore[] nodes = result.getNodes();
                for (int i = 0; i < topK; i++) {
                    if (i < nodes.length) {
                        out[pos++] = nodes[i].node;
                        out[pos++] = nodes[i].score;
                    } else {
                        out[pos++] = -1.0f;
                        out[pos++] = Float.NaN;
                    }
                }
            }
            return out;
        } finally {
            disk.close();
        }
    }

    @Test
    public void parallelAndSequentialProduceEquivalentGraphs() throws Exception {
        Path dir = tmpFolder.newFolder().toPath();
        Path sequentialFile = dir.resolve("sequential.idx");
        Path parallelFile = dir.resolve("parallel.idx");

        writeGraph(sequentialFile, false, false);
        writeGraph(parallelFile, true, false);

        float[] sequentialResults = searchAll(sequentialFile, 10);
        float[] parallelResults = searchAll(parallelFile, 10);

        assertArrayEquals(
                "parallel writer must produce a graph that searches identically to the sequential writer",
                sequentialResults, parallelResults, 0.0f);
    }

    @Test
    public void directBuffersOptionProducesEquivalentGraph() throws Exception {
        Path dir = tmpFolder.newFolder().toPath();
        Path sequentialFile = dir.resolve("sequential.idx");
        Path parallelDirectFile = dir.resolve("parallel-direct.idx");

        writeGraph(sequentialFile, false, false);
        writeGraph(parallelDirectFile, true, true);

        assertArrayEquals(
                "parallel writer with direct buffers must produce an equivalent graph",
                searchAll(sequentialFile, 10), searchAll(parallelDirectFile, 10), 0.0f);
    }

    @Test
    public void forcedParallelReturnsParallelWriter() throws Exception {
        Path file = tmpFolder.newFolder().toPath().resolve("forced-parallel.idx");
        try (RandomAccessOnDiskGraphIndexWriter writer = GraphWriterFactory.openWriter(
                graph, file, List.of(new InlineVectors(DIM)), true, 0, false)) {
            assertTrue("parallel=true must yield an OnDiskParallelGraphIndexWriter",
                    writer instanceof OnDiskParallelGraphIndexWriter);
        }
    }

    @Test
    public void forcedSequentialReturnsSequentialWriter() throws Exception {
        Path file = tmpFolder.newFolder().toPath().resolve("forced-sequential.idx");
        try (RandomAccessOnDiskGraphIndexWriter writer = GraphWriterFactory.openWriter(
                graph, file, List.of(new InlineVectors(DIM)), false, 0, false)) {
            assertTrue("parallel=false must yield a (sequential) OnDiskGraphIndexWriter",
                    writer instanceof OnDiskGraphIndexWriter);
        }
    }

    /**
     * The public {@code openWriter} routes to the parallel writer only when the
     * {@code herddb.vectorindex.parallelGraphWrite} flag is enabled <em>and</em>
     * the node count is at or above the configured threshold. The expectations
     * are derived from the static configuration so the test is correct whether
     * the suite runs with the flag off (the default) or forced on.
     */
    @Test
    public void publicOpenWriterRoutesByStaticConfigAndNodeCount() throws Exception {
        Path dir = tmpFolder.newFolder().toPath();

        int aboveThreshold = GraphWriterFactory.PARALLEL_GRAPH_WRITE_MIN_NODES;
        boolean expectParallelAbove = GraphWriterFactory.PARALLEL_GRAPH_WRITE
                && aboveThreshold >= GraphWriterFactory.PARALLEL_GRAPH_WRITE_MIN_NODES;
        try (RandomAccessOnDiskGraphIndexWriter writer = GraphWriterFactory.openWriter(
                graph, dir.resolve("above.idx"), aboveThreshold, List.of(new InlineVectors(DIM)))) {
            assertEquals("routing for node count >= threshold",
                    expectParallelAbove, writer instanceof OnDiskParallelGraphIndexWriter);
        }

        int belowThreshold = Math.max(0, GraphWriterFactory.PARALLEL_GRAPH_WRITE_MIN_NODES - 1);
        boolean expectParallelBelow = GraphWriterFactory.PARALLEL_GRAPH_WRITE
                && belowThreshold >= GraphWriterFactory.PARALLEL_GRAPH_WRITE_MIN_NODES;
        try (RandomAccessOnDiskGraphIndexWriter writer = GraphWriterFactory.openWriter(
                graph, dir.resolve("below.idx"), belowThreshold, List.of(new InlineVectors(DIM)))) {
            assertEquals("routing for node count < threshold",
                    expectParallelBelow, writer instanceof OnDiskParallelGraphIndexWriter);
        }
    }
}
