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

import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.OnDiskParallelGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.RandomAccessOnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Chooses the jvector on-disk graph writer for the single-graph vector write
 * sites ({@code PersistentVectorStore.writeFusedPQGraphToTempFile()} and
 * {@code RemoteSegmentGraphMerger.writeGraph()}).
 *
 * <p>By default HerdDB serializes graphs with the <em>sequential</em>
 * {@link OnDiskGraphIndexWriter}. When the opt-in flag is enabled and the graph
 * is large enough, this factory instead returns jvector's
 * {@link OnDiskParallelGraphIndexWriter}, which writes L0 node records
 * concurrently via an internal worker pool and an {@code AsynchronousFileChannel}.
 *
 * <p>The parallel L0 write path is annotated {@code @Experimental} in jvector,
 * so it stays off by default until validated in benchmarks. Both writers extend
 * {@link RandomAccessOnDiskGraphIndexWriter} and share the same {@code write},
 * {@code writeHeader} and {@code close} API, so they are drop-in replacements.
 *
 * <p>This factory is intentionally not applied to the Phase B per-shard write
 * ({@code writeShardAsFusedPQSegment}): Phase B already runs several shard
 * writers concurrently, so a parallel writer there would oversubscribe the CPU.
 */
final class GraphWriterFactory {

    private static final Logger LOGGER = Logger.getLogger(GraphWriterFactory.class.getName());

    /**
     * Master enable for the parallel writer. System property
     * {@code herddb.vectorindex.parallelGraphWrite}; default {@code false}.
     */
    static final boolean PARALLEL_GRAPH_WRITE =
            Boolean.getBoolean("herddb.vectorindex.parallelGraphWrite");

    /**
     * Only graphs with at least this many nodes are written with the parallel
     * writer; smaller graphs do not amortize the worker-pool setup cost. System
     * property {@code herddb.vectorindex.parallelGraphWriteMinNodes}; default
     * {@code 50000}.
     */
    static final int PARALLEL_GRAPH_WRITE_MIN_NODES =
            Math.max(0, Integer.getInteger(
                    "herddb.vectorindex.parallelGraphWriteMinNodes", 50_000));

    /**
     * Worker thread count handed to the parallel writer. System property
     * {@code herddb.vectorindex.graphWriteParallelism}; default {@code 0},
     * which lets jvector use the number of available processors.
     */
    static final int GRAPH_WRITE_PARALLELISM =
            Math.max(0, Integer.getInteger(
                    "herddb.vectorindex.graphWriteParallelism", 0));

    /**
     * Whether the parallel writer should use direct byte buffers. System
     * property {@code herddb.vectorindex.parallelGraphWriteDirectBuffers};
     * default {@code false}.
     */
    static final boolean PARALLEL_GRAPH_WRITE_DIRECT_BUFFERS =
            Boolean.getBoolean("herddb.vectorindex.parallelGraphWriteDirectBuffers");

    private GraphWriterFactory() {
    }

    /**
     * Opens a graph writer for {@code file}, choosing the parallel writer when
     * the {@link #PARALLEL_GRAPH_WRITE} flag is enabled and {@code nodeCount} is
     * at or above {@link #PARALLEL_GRAPH_WRITE_MIN_NODES}.
     *
     * @param graph     the graph to serialize
     * @param file      the target file path (the parallel writer requires a
     *                  {@link Path} because it uses an asynchronous channel)
     * @param nodeCount the number of nodes in {@code graph}
     * @param features  the features to attach, in builder order
     * @return a writer; the caller must use it in a try-with-resources block
     */
    static RandomAccessOnDiskGraphIndexWriter openWriter(
            ImmutableGraphIndex graph, Path file, int nodeCount,
            List<Feature> features) throws IOException {
        boolean parallel = PARALLEL_GRAPH_WRITE
                && nodeCount >= PARALLEL_GRAPH_WRITE_MIN_NODES;
        return openWriter(graph, file, features, parallel,
                GRAPH_WRITE_PARALLELISM, PARALLEL_GRAPH_WRITE_DIRECT_BUFFERS);
    }

    /**
     * Deterministic variant that builds exactly the writer requested by
     * {@code parallel}, ignoring the static configuration. Used by
     * {@link #openWriter(ImmutableGraphIndex, Path, int, List)} and by tests.
     *
     * @param graph         the graph to serialize
     * @param file          the target file path
     * @param features      the features to attach, in builder order
     * @param parallel      when {@code true} build an
     *                      {@link OnDiskParallelGraphIndexWriter}, otherwise an
     *                      {@link OnDiskGraphIndexWriter}
     * @param workerThreads parallel worker threads ({@code 0} = jvector auto);
     *                      ignored when {@code parallel} is {@code false}
     * @param directBuffers whether the parallel writer uses direct buffers;
     *                      ignored when {@code parallel} is {@code false}
     */
    static RandomAccessOnDiskGraphIndexWriter openWriter(
            ImmutableGraphIndex graph, Path file, List<Feature> features,
            boolean parallel, int workerThreads, boolean directBuffers) throws IOException {
        if (parallel) {
            LOGGER.log(Level.INFO,
                    "vector graph write: using parallel writer for {0} "
                            + "(workerThreads={1}, directBuffers={2})",
                    new Object[]{file,
                            workerThreads == 0 ? "auto" : workerThreads, directBuffers});
            // withParallelWorkerThreads / withParallelDirectBuffers must be
            // called before any with(feature): with(...) narrows the static
            // type to the base AbstractGraphIndexWriter.Builder.
            OnDiskParallelGraphIndexWriter.Builder builder =
                    new OnDiskParallelGraphIndexWriter.Builder(graph, file)
                            .withParallelWorkerThreads(workerThreads)
                            .withParallelDirectBuffers(directBuffers);
            for (Feature feature : features) {
                builder.with(feature);
            }
            return builder.build();
        }
        LOGGER.log(Level.FINE,
                "vector graph write: using sequential writer for {0}", file);
        OnDiskGraphIndexWriter.Builder builder =
                new OnDiskGraphIndexWriter.Builder(graph, file);
        for (Feature feature : features) {
            builder.with(feature);
        }
        return builder.build();
    }
}
