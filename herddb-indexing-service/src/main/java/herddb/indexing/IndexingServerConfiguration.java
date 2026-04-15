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

import herddb.model.TableSpace;
import herddb.server.ServerConfiguration;
import java.util.Properties;

/**
 * Configuration for the IndexingServer and IndexingServiceEngine.
 *
 * @author enrico.olivelli
 */
public final class IndexingServerConfiguration {

    private final Properties properties;

    // gRPC server
    public static final String PROPERTY_GRPC_HOST = "indexing.grpc.host";
    public static final String PROPERTY_GRPC_HOST_DEFAULT = "0.0.0.0";

    public static final String PROPERTY_GRPC_PORT = "indexing.grpc.port";
    public static final int PROPERTY_GRPC_PORT_DEFAULT = 9850;

    // HTTP / metrics
    public static final String PROPERTY_HTTP_ENABLE = "indexing.http.enable";
    public static final boolean PROPERTY_HTTP_ENABLE_DEFAULT = false;

    public static final String PROPERTY_HTTP_HOST = "indexing.http.host";
    public static final String PROPERTY_HTTP_HOST_DEFAULT = "0.0.0.0";

    public static final String PROPERTY_HTTP_PORT = "indexing.http.port";
    public static final int PROPERTY_HTTP_PORT_DEFAULT = 9851;

    // Storage directories
    public static final String PROPERTY_LOG_DIR = "indexing.log.dir";
    public static final String PROPERTY_LOG_DIR_DEFAULT = "txlog";

    /**
     * Directory used by the indexing service for <b>local-only</b> state:
     * {@code watermark.dat} (the commit-log cursor), the
     * {@code RemoteFileDataStorageManager}'s local metadata cache
     * ({@code {dataDir}/remote-metadata}) and its transient scratch space
     * ({@code {dataDir}/remote-tmp}), plus per-segment <em>transient</em>
     * checkpoint work files created by {@code PersistentVectorStore}.
     *
     * <p>None of the files in this directory are ever uploaded to the remote
     * file service — vector graph and map pages flow through
     * {@code DataStorageManager.writeIndexPage} directly to S3. Operators
     * should size this directory for:
     * <ul>
     *   <li>~60–200 MB peak per segment during FusedPQ Phase B,
     *       multiplied by {@code herddb.vectorindex.phaseBSegmentParallelism}
     *       (default 2);</li>
     *   <li>one transient map tmp file per segment reload on restart
     *       (deleted immediately afterwards, ~20–100 MB depending on the
     *       PK width);</li>
     *   <li>the {@code RemoteFileDataStorageManager} local metadata
     *       (checkpoint markers), which grows linearly with the number of
     *       tablespaces/indexes but is typically &lt; 10 MB.</li>
     * </ul>
     *
     * <p><b>Recommended free space</b>: 500 MB.
     */
    public static final String PROPERTY_DATA_DIR = "indexing.data.dir";
    public static final String PROPERTY_DATA_DIR_DEFAULT = "data";

    // Memory
    public static final String PROPERTY_MEMORY_VECTOR_LIMIT = "indexing.memory.vector.limit";
    public static final long PROPERTY_MEMORY_VECTOR_LIMIT_DEFAULT = 0L;

    public static final String PROPERTY_MEMORY_PAGE_SIZE = "indexing.memory.page.size";
    public static final long PROPERTY_MEMORY_PAGE_SIZE_DEFAULT = 1048576L;

    // Vector index tuning
    public static final String PROPERTY_VECTOR_M = "indexing.vector.m";
    public static final int PROPERTY_VECTOR_M_DEFAULT = 16;

    public static final String PROPERTY_VECTOR_BEAM_WIDTH = "indexing.vector.beamWidth";
    public static final int PROPERTY_VECTOR_BEAM_WIDTH_DEFAULT = 100;

    public static final String PROPERTY_VECTOR_NEIGHBOR_OVERFLOW = "indexing.vector.neighborOverflow";
    public static final double PROPERTY_VECTOR_NEIGHBOR_OVERFLOW_DEFAULT = 1.2;

    public static final String PROPERTY_VECTOR_ALPHA = "indexing.vector.alpha";
    public static final double PROPERTY_VECTOR_ALPHA_DEFAULT = 1.4;

    public static final String PROPERTY_VECTOR_FUSED_PQ = "indexing.vector.fusedPQ";
    public static final boolean PROPERTY_VECTOR_FUSED_PQ_DEFAULT = true;

    public static final String PROPERTY_VECTOR_MAX_SEGMENT_SIZE = "indexing.vector.maxSegmentSize";
    public static final long PROPERTY_VECTOR_MAX_SEGMENT_SIZE_DEFAULT = 2147483648L;

    public static final String PROPERTY_VECTOR_MAX_LIVE_GRAPH_SIZE = "indexing.vector.maxLiveGraphSize";
    public static final int PROPERTY_VECTOR_MAX_LIVE_GRAPH_SIZE_DEFAULT = 0;

    public static final String PROPERTY_VECTOR_MAX_LIVE_BYTES_PER_CHECKPOINT =
            "indexing.vector.maxLiveBytesPerCheckpoint";
    public static final long PROPERTY_VECTOR_MAX_LIVE_BYTES_PER_CHECKPOINT_DEFAULT =
            10L * 1024 * 1024 * 1024; // 10 GiB

    // Compaction
    public static final String PROPERTY_COMPACTION_INTERVAL = "indexing.compaction.interval";
    public static final long PROPERTY_COMPACTION_INTERVAL_DEFAULT = 60000L;

    public static final String PROPERTY_COMPACTION_THREADS = "indexing.compaction.threads";
    public static final int PROPERTY_COMPACTION_THREADS_DEFAULT = 2;

    // Apply parallelism
    public static final String PROPERTY_APPLY_PARALLELISM = "indexing.apply.parallelism";
    public static final int PROPERTY_APPLY_PARALLELISM_DEFAULT = 0; // 0 = auto: max(1, availableProcessors/2)

    public static final String PROPERTY_APPLY_QUEUE_CAPACITY = "indexing.apply.queue.capacity";
    public static final int PROPERTY_APPLY_QUEUE_CAPACITY_DEFAULT = 1000;

    /**
     * Tailer-driven watermark checkpoint trigger: after this many entries are
     * processed by the tailer, {@code IndexingServiceEngine} drains pending
     * DML, forces a checkpoint on every {@code PersistentVectorStore}, and
     * (if all checkpoints succeed) saves the watermark.
     *
     * <p>This is a <em>backstop</em> — the primary checkpoint driver is the
     * per-store background compaction loop
     * ({@link #PROPERTY_COMPACTION_INTERVAL}). The tailer trigger exists to
     * guarantee watermark liveness when the compaction loop is idle. It must
     * be large enough that it does not coincide with the compaction loop's
     * cadence during catch-up (see issue #90): a low value causes back-to-back
     * Phase B/C cycles on the tailer thread, starving BK reads.
     */
    public static final String PROPERTY_WATERMARK_CHECKPOINT_INTERVAL_ENTRIES =
            "indexing.watermark.checkpoint.interval.entries";
    public static final long PROPERTY_WATERMARK_CHECKPOINT_INTERVAL_ENTRIES_DEFAULT = 100_000L;

    // Storage
    public static final String PROPERTY_STORAGE_TYPE = "indexing.storage.type";
    public static final String PROPERTY_STORAGE_TYPE_DEFAULT = "file";

    // Remote file storage settings (same keys as ServerConfiguration so config can be copy/pasted)
    public static final String PROPERTY_REMOTE_FILE_SERVERS = "remote.file.servers";
    public static final String PROPERTY_REMOTE_FILE_SERVERS_DEFAULT = "";

    public static final String PROPERTY_REMOTE_FILE_CLIENT_TIMEOUT = "remote.file.client.timeout";
    public static final long PROPERTY_REMOTE_FILE_CLIENT_TIMEOUT_DEFAULT = 1800L; // 30 minutes, in seconds

    public static final String PROPERTY_REMOTE_FILE_CLIENT_RETRIES = "remote.file.client.retries";
    public static final int PROPERTY_REMOTE_FILE_CLIENT_RETRIES_DEFAULT = 10;

    /**
     * Maximum time (in milliseconds) to block at bootstrap waiting for at
     * least one remote file server to be discovered (via ZK) before giving up
     * and failing startup. Guards against a cold-cluster race where the
     * indexing service starts before the file-server pod has registered
     * itself in ZK and the consistent-hash ring is still empty when the first
     * {@code readFile} for the watermark is issued.
     */
    public static final String PROPERTY_REMOTE_FILE_BOOTSTRAP_WAIT_MS =
            "remote.file.bootstrap.wait.ms";
    public static final long PROPERTY_REMOTE_FILE_BOOTSTRAP_WAIT_MS_DEFAULT = 30_000L;

    // Instance identity and clustering
    public static final String PROPERTY_INSTANCE_ID = "indexing.instance.id";
    public static final int PROPERTY_INSTANCE_ID_DEFAULT = 0;

    public static final String PROPERTY_NUM_INSTANCES = "indexing.cluster.numInstances";
    public static final int PROPERTY_NUM_INSTANCES_DEFAULT = 1;

    public static final String PROPERTY_DEFAULT_NUM_SHARDS = "indexing.vector.default.numShards";
    public static final int PROPERTY_DEFAULT_NUM_SHARDS_DEFAULT = 1;

    // Log tailing mode
    public static final String PROPERTY_LOG_TYPE = "indexing.log.type";
    public static final String PROPERTY_LOG_TYPE_DEFAULT = "file";

    // BookKeeper/ZooKeeper settings (for log.type=bookkeeper)
    // Use SAME keys as ServerConfiguration so config can be copy/pasted
    public static final String PROPERTY_ZOOKEEPER_ADDRESS = "server.zookeeper.address";
    public static final String PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT = "localhost:2181";

    public static final String PROPERTY_ZOOKEEPER_SESSION_TIMEOUT = "server.zookeeper.session.timeout";
    public static final int PROPERTY_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT = 40000;

    public static final String PROPERTY_ZOOKEEPER_PATH = "server.zookeeper.path";
    public static final String PROPERTY_ZOOKEEPER_PATH_DEFAULT = "/herd";

    public static final String PROPERTY_BOOKKEEPER_LEDGERS_PATH = "server.bookkeeper.ledgers.path";
    public static final String PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT = "/ledgers";

    public static final String PROPERTY_TABLESPACE_NAME = "indexing.tablespace.name";
    public static final String PROPERTY_TABLESPACE_NAME_DEFAULT = TableSpace.DEFAULT;

    public static final String PROPERTY_TABLESPACE_WAIT_POLL_INTERVAL_MS = "indexing.tablespace.wait.poll.interval.ms";
    public static final int PROPERTY_TABLESPACE_WAIT_POLL_INTERVAL_MS_DEFAULT = 2_000;

    // Server mode — same key as ServerConfiguration so config can be copy/pasted
    public static final String PROPERTY_MODE = "server.mode";
    public static final String PROPERTY_MODE_DEFAULT = ServerConfiguration.PROPERTY_MODE_STANDALONE;

    // Metadata directory — same key as ServerConfiguration
    public static final String PROPERTY_METADATA_DIR = "server.metadata.dir";
    public static final String PROPERTY_METADATA_DIR_DEFAULT = "metadata";

    public IndexingServerConfiguration() {
        this.properties = new Properties();
    }

    public IndexingServerConfiguration(Properties properties) {
        this.properties = new Properties();
        this.properties.putAll(properties);
    }

    /**
     * Copy configuration.
     *
     * @return an independent copy of this configuration
     */
    public IndexingServerConfiguration copy() {
        Properties copy = new Properties();
        copy.putAll(this.properties);
        return new IndexingServerConfiguration(copy);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        final String value = this.properties.getProperty(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    public int getInt(String key, int defaultValue) {
        final String value = this.properties.getProperty(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    public long getLong(String key, long defaultValue) {
        final String value = this.properties.getProperty(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }

    public double getDouble(String key, double defaultValue) {
        final String value = this.properties.getProperty(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Double.parseDouble(value);
    }

    public String getString(String key, String defaultValue) {
        return this.properties.getProperty(key, defaultValue);
    }

    /** Returns a copy of the underlying {@link Properties}. */
    public Properties asProperties() {
        Properties copy = new Properties();
        copy.putAll(this.properties);
        return copy;
    }

    public IndexingServerConfiguration set(String key, Object value) {
        if (value == null) {
            this.properties.remove(key);
        } else {
            this.properties.setProperty(key, value + "");
        }
        return this;
    }

    @Override
    public String toString() {
        return "IndexingServerConfiguration{" + "properties=" + properties + '}';
    }
}
