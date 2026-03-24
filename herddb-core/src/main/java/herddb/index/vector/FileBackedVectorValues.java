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

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * A disk-backed {@link RandomAccessVectorValues} that stores vectors in a temporary file.
 * <p>
 * Vectors are stored sequentially by node ID: vector for nodeId N is at file offset
 * {@code N * dimension * Float.BYTES}. This allows lock-free concurrent writes since
 * each nodeId maps to a unique, non-overlapping region.
 * <p>
 * Two implementations are available:
 * <ul>
 *   <li>{@link MmapFileBackedVectorValues} — memory-mapped I/O (fast but consumes virtual memory / page cache)</li>
 *   <li>{@link ChannelFileBackedVectorValues} — positional read/write via FileChannel (lower memory footprint)</li>
 * </ul>
 * The implementation is selected by the system property {@code herddb.vectorindex.file.usemmap}
 * (default {@code false}).
 */
public abstract class FileBackedVectorValues implements RandomAccessVectorValues, Closeable {

    static final String USE_MMAP_PROPERTY = "herddb.vectorindex.file.usemmap";
    static final boolean USE_MMAP = Boolean.getBoolean(USE_MMAP_PROPERTY);

    /**
     * Default maximum size per mapped segment. Must be &lt;= Integer.MAX_VALUE.
     * Using 1 GiB to leave headroom.
     */
    static final int DEFAULT_MAX_SEGMENT_SIZE = 1 << 30; // 1 GiB

    /**
     * Creates a new file-backed vector storage using the default I/O mode.
     *
     * @param dimension    vector dimension
     * @param expectedSize expected number of vectors (used for initial file allocation)
     * @param tempDir      directory for the temporary file
     */
    public static FileBackedVectorValues create(int dimension, long expectedSize, Path tempDir) throws IOException {
        return create(dimension, expectedSize, tempDir, DEFAULT_MAX_SEGMENT_SIZE, USE_MMAP);
    }

    /**
     * Creates a new file-backed vector storage with explicit I/O mode selection.
     */
    static FileBackedVectorValues create(int dimension, long expectedSize, Path tempDir,
                                          int maxSegmentSize, boolean useMmap) throws IOException {
        if (useMmap) {
            return new MmapFileBackedVectorValues(dimension, expectedSize, tempDir, maxSegmentSize);
        } else {
            return new ChannelFileBackedVectorValues(dimension, expectedSize, tempDir);
        }
    }

    /**
     * Store a vector at the given nodeId position.
     * Thread-safe: each nodeId maps to a unique file region.
     */
    public abstract void putVector(int nodeId, VectorFloat<?> vec);

    /**
     * Store a vector from a float array at the given nodeId position.
     */
    public abstract void putVector(int nodeId, float[] floats);
}
