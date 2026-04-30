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

    public static final String PROPERTY_VECTOR_SEGMENT_PAGE_CACHE_MAX_BYTES =
            "indexing.vector.segmentPageCacheMaxBytes";
    // 0 = auto-size as 1/4 of Netty maxDirectMemory (heap fallback when unavailable)
    public static final long PROPERTY_VECTOR_SEGMENT_PAGE_CACHE_MAX_BYTES_DEFAULT = 0;

    /**
     * Number of bytes to read sequentially from the beginning of each segment's
     * graph file after Phase C of a checkpoint, before saving the watermark (and
     * therefore before {@code EXECUTE WAITFORINDEXES} unblocks). Populating the
     * {@link herddb.remote.SegmentBlockCache} with the entry-point neighbourhood
     * eliminates the 4–7 s cold-start latency observed when the first query batch
     * must stream every block from the remote file server (issue #322).
     *
     * <p>The effective value is resolved in priority order:
     * <ol>
     *   <li>This config key ({@code indexing.vector.segmentCacheWarmupBytes}) in the
     *       indexing-service properties file.</li>
     *   <li>The JVM system property
     *       {@code herddb.vectorindex.segmentCacheWarmupBytes}.</li>
     *   <li>The hard-coded default of 32 MiB.</li>
     * </ol>
     *
     * <p>Set to {@code 0} to disable warmup entirely (e.g. when running in
     * in-memory or local-file mode where there is no remote gRPC overhead).
     * The warmup is best-effort: a per-segment I/O failure is logged as a
     * WARNING and does not abort the watermark save.
     */
    public static final String PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_BYTES =
            "indexing.vector.segmentCacheWarmupBytes";
    /**
     * System property name that provides the JVM-level default for
     * {@link #PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_BYTES}. A value set in the
     * properties file always wins over this system property.
     */
    public static final String SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_BYTES =
            "herddb.vectorindex.segmentCacheWarmupBytes";
    /** Hard-coded fallback: 32 MiB per segment. */
    public static final long PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_BYTES_DEFAULT = 32L * 1024 * 1024;

    // Compaction (checkpoint driver — existing)
    public static final String PROPERTY_COMPACTION_INTERVAL = "indexing.compaction.interval";
    public static final long PROPERTY_COMPACTION_INTERVAL_DEFAULT = 60000L;

    public static final String PROPERTY_COMPACTION_THREADS = "indexing.compaction.threads";
    public static final int PROPERTY_COMPACTION_THREADS_DEFAULT = 2;

    // Vector-index graph-merge compaction — picks N small/mergeable on-disk
    // segments, rebuilds one larger jvector graph from the live vectors,
    // atomically swaps the new segment in, and queues the old files for
    // retention-aware deletion. Runs on a dedicated background thread,
    // independent of the checkpoint driver above.
    public static final String PROPERTY_VECTOR_INDEX_COMPACTION_INTERVAL_MS =
            "vector.index.compaction.intervalMs";
    public static final long PROPERTY_VECTOR_INDEX_COMPACTION_INTERVAL_MS_DEFAULT = 5L * 60_000L;

    /** Minimum total bytes across candidate segments before a compaction run will fire. */
    public static final String PROPERTY_VECTOR_INDEX_COMPACTION_MIN_BYTES =
            "vector.index.compaction.minBytes";
    public static final long PROPERTY_VECTOR_INDEX_COMPACTION_MIN_BYTES_DEFAULT =
            256L * 1024 * 1024; // 256 MB

    /** Hard cap on total bytes a single compaction run may read, bounding disk pressure. */
    public static final String PROPERTY_VECTOR_INDEX_COMPACTION_MAX_BYTES =
            "vector.index.compaction.maxBytes";
    public static final long PROPERTY_VECTOR_INDEX_COMPACTION_MAX_BYTES_DEFAULT =
            1024L * 1024 * 1024; // 1 GB

    /**
     * Count-based compaction trigger (issue #285): fire compaction when the
     * on-disk segment count reaches this ceiling, even if the total byte
     * threshold ({@link #PROPERTY_VECTOR_INDEX_COMPACTION_MIN_BYTES}) has not
     * been met. Prevents unbounded segment accumulation during tailing
     * catch-up when each checkpoint produces many small segments.
     * Set to {@code Integer.MAX_VALUE} to disable the count trigger entirely.
     */
    public static final String PROPERTY_VECTOR_INDEX_COMPACTION_MAX_COUNT =
            "vector.index.compaction.maxCount";
    public static final int PROPERTY_VECTOR_INDEX_COMPACTION_MAX_COUNT_DEFAULT = 200;

    /**
     * How long old segment files remain on-disk after a compaction swap
     * before the reaper may physically delete them. Also gated by
     * {@code shadowAckedGeneration}: reclaim waits for the later of the
     * two signals.
     */
    public static final String PROPERTY_VECTOR_INDEX_COMPACTION_RETENTION_MS =
            "vector.index.compaction.retentionMs";
    public static final long PROPERTY_VECTOR_INDEX_COMPACTION_RETENTION_MS_DEFAULT =
            10L * 60_000L; // 10 min

    /**
     * Enables tiered compaction scaling (issue #354). When {@code true}
     * (the default), the per-cycle byte cap and segment-count cap are
     * multiplied by a tier-dependent factor (2×/4×/8×) when the total
     * on-disk segment count exceeds 100/300/500 segments respectively,
     * keeping compaction throughput proportional to the ingest rate.
     * Set to {@code false} to revert to the flat, unscaled caps.
     */
    public static final String PROPERTY_VECTOR_INDEX_COMPACTION_TIERED_ENABLED =
            "vector.index.compaction.tiered.enabled";
    public static final boolean PROPERTY_VECTOR_INDEX_COMPACTION_TIERED_ENABLED_DEFAULT = true;

    /**
     * Segment-count back-pressure threshold (issue #354).
     * {@code addVector} blocks when the total number of on-disk segments
     * exceeds this value, waking the compaction thread before parking.
     * This prevents the tailer from accumulating an unbounded backlog of
     * segments when compaction cannot keep up with the ingest rate.
     * Default is 500.  Set to {@link Integer#MAX_VALUE} to disable.
     */
    public static final String PROPERTY_VECTOR_INDEX_COMPACTION_BACKPRESSURE_SEGMENTS =
            "vector.index.compaction.backpressure.segments";
    public static final int PROPERTY_VECTOR_INDEX_COMPACTION_BACKPRESSURE_SEGMENTS_DEFAULT = 500;

    /**
     * When {@code true}, the IS does NOT run the per-store
     * {@code vectorIndexCompactionLoop()} thread — compaction is delegated to
     * the external index-optimizer service, which scans the segment registry
     * and merges segments out-of-process. The IS still runs the tailer and
     * checkpoint loop. Default {@code false} (legacy in-IS compaction).
     */
    public static final String PROPERTY_INDEX_OPTIMIZER_ENABLED = "indexing.optimizer.enabled";
    public static final boolean PROPERTY_INDEX_OPTIMIZER_ENABLED_DEFAULT = false;

    /**
     * Pressure-driven IS-local compaction fallback (companion to the external
     * index-optimizer). Even when {@link #PROPERTY_INDEX_OPTIMIZER_ENABLED} is
     * {@code true}, the IS keeps a local compaction thread armed; it stays
     * idle until the locally-observed segment count crosses
     * {@code kickFraction × }{@link #PROPERTY_VECTOR_INDEX_COMPACTION_BACKPRESSURE_SEGMENTS}.
     *
     * <p>Steady state is still optimizer-driven: the local loop only fires when
     * the optimizer cannot keep up with checkpoint output and the IS would
     * otherwise hit the back-pressure ceiling and stall the tailer. Default is
     * {@code 0.7} (350 of 500), giving the optimizer 70% of the budget before
     * the IS-local fallback kicks in.
     *
     * <p>Must be in the open interval {@code (0.0, 1.0)}. Values outside that
     * range are rejected at parse time so an operator cannot accidentally
     * disable both compactors (set to 0) or render the fallback inert (set to
     * 1.0+).
     */
    public static final String PROPERTY_VECTOR_INDEX_COMPACTION_LOCAL_KICK_FRACTION =
            "vector.index.compaction.local.kick.fraction";
    public static final double PROPERTY_VECTOR_INDEX_COMPACTION_LOCAL_KICK_FRACTION_DEFAULT = 0.7d;

    /**
     * Master switch for the IS-local compaction fallback when
     * {@link #PROPERTY_INDEX_OPTIMIZER_ENABLED} is {@code true}. When
     * {@code false}, restores the pre-#357 behaviour: full delegation to the
     * external optimizer, no local fallback. Operators who explicitly want
     * single-actor compaction can set this to {@code false}, accepting that
     * the tailer may stall on back-pressure if the optimizer is slow or
     * temporarily unavailable. Default {@code true}.
     */
    public static final String PROPERTY_VECTOR_INDEX_COMPACTION_LOCAL_ENABLED_WITH_OPTIMIZER =
            "vector.index.compaction.local.enabledWithOptimizer";
    public static final boolean PROPERTY_VECTOR_INDEX_COMPACTION_LOCAL_ENABLED_WITH_OPTIMIZER_DEFAULT = true;

    // Apply parallelism
    public static final String PROPERTY_APPLY_PARALLELISM = "indexing.apply.parallelism";
    public static final int PROPERTY_APPLY_PARALLELISM_DEFAULT = 0; // 0 = auto: max(1, availableProcessors/2)

    public static final String PROPERTY_APPLY_QUEUE_CAPACITY = "indexing.apply.queue.capacity";
    public static final int PROPERTY_APPLY_QUEUE_CAPACITY_DEFAULT = 1000;

    // Search parallelism — parallel fan-out across segments and shards within
    // a single vector search. 0 = auto: max(1, availableProcessors / 2).
    public static final String PROPERTY_VECTOR_SEARCH_PARALLELISM = "indexing.vector.search.parallelism";
    public static final int PROPERTY_VECTOR_SEARCH_PARALLELISM_DEFAULT = 0;

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
     * Maximum number of bytes across in-flight {@code readFile}/{@code readFileRange}
     * gRPC calls that may be staged into a pooled direct
     * {@link io.netty.buffer.ByteBuf} at once. Each call reserves bytes equal
     * to its requested payload length before allocation; the cap therefore
     * bounds peak {@code Netty PoolArena} growth during IS checkpoint Phase
     * C and cache-miss query bursts (see issue #246) independently of
     * request size — a 16 KiB {@code RemoteRandomAccessReader} block takes
     * 16 KiB from the budget, a 4 MiB Phase-C chunk takes 4 MiB. Default
     * 256 MiB leaves ample headroom under a typical 6-10 GiB
     * {@code -XX:MaxDirectMemorySize}. Lower this on memory-constrained IS
     * pods; raise it only when the Netty arena has enough room.
     */
    public static final String PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_READ_BYTES =
            "remote.file.client.max.inflight.read.bytes";
    public static final long PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_READ_BYTES_DEFAULT =
            256L * 1024 * 1024;

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
    public static final long PROPERTY_REMOTE_FILE_BOOTSTRAP_WAIT_MS_DEFAULT = 1_800_000L; // 30 minutes

    // Instance identity and clustering
    public static final String PROPERTY_INSTANCE_ID = "indexing.instance.id";
    public static final int PROPERTY_INSTANCE_ID_DEFAULT = 0;

    public static final String PROPERTY_NUM_INSTANCES = "indexing.cluster.numInstances";
    public static final int PROPERTY_NUM_INSTANCES_DEFAULT = 1;

    /**
     * When {@code true}, this primary engine boots in {@code JOINING} state:
     * it does not load any indexes from local storage, sets the tailer to
     * the live tail of the commit log (skipping the historical entries) and
     * waits for the next {@code INDEXING_SERVICE_REBALANCE} log entry to
     * acquire the schema. The Helm chart sets this on every pod; in
     * production an engine with persistent local state ignores the flag
     * because its watermark and S3 segments already give it everything it
     * needs.
     *
     * <p>This path also activates automatically when the BookKeeper history
     * has been trimmed and the standard START_OF_TIME replay is impossible,
     * but for tests and for explicit operator opt-in we expose it as a
     * property.
     */
    public static final String PROPERTY_BOOTSTRAP_FROM_REBALANCE =
            "indexing.bootstrap.fromRebalance";
    public static final boolean PROPERTY_BOOTSTRAP_FROM_REBALANCE_DEFAULT = false;

    /**
     * Role of this indexing-service process.
     *
     * <ul>
     *   <li>{@link #ROLE_PRIMARY} (default) — tails the commit log, owns a
     *       subset of shards, writes checkpoints to storage.</li>
     *   <li>{@link #ROLE_SHADOW} — read-only replica tied to a specific
     *       primary (see {@link #PROPERTY_SHADOW_OF}). Serves queries from
     *       on-disk segments loaded from the shared remote storage; does
     *       not tail the log and rejects writes. Requires
     *       {@code indexing.storage.type=remote}.</li>
     * </ul>
     */
    public static final String PROPERTY_ROLE = "indexing.role";
    public static final String PROPERTY_ROLE_DEFAULT = "primary";
    public static final String ROLE_PRIMARY = "primary";
    public static final String ROLE_SHADOW = "shadow";

    /**
     * When {@link #PROPERTY_ROLE} is {@code shadow}, the {@code instanceId} of
     * the primary this replica shadows. Must be in
     * {@code [0, indexing.cluster.numInstances - 1]}. Ignored for primaries.
     */
    public static final String PROPERTY_SHADOW_OF = "indexing.shadow.of";
    public static final int PROPERTY_SHADOW_OF_UNSET = -1;

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

    /**
     * Maximum time (in milliseconds) to block during engine start() waiting for the tablespace
     * to be available in the metadata storage manager. On a cold k3s boot, the metadata
     * (which may be replicated from ZooKeeper or another source) can take time to become
     * available. Without a timeout, the loop would block indefinitely.
     * With an explicit timeout, the pod can be restarted or the issue investigated.
     */
    public static final String PROPERTY_TABLESPACE_WAIT_TIMEOUT_MS = "indexing.tablespace.wait.timeout.ms";
    public static final long PROPERTY_TABLESPACE_WAIT_TIMEOUT_MS_DEFAULT = 1_800_000L; // 30 minutes

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

    /**
     * Validates {@link #PROPERTY_ROLE} and {@link #PROPERTY_SHADOW_OF} and any
     * cross-dependency with other properties. Called from the server bootstrap
     * so that misconfigured shadows fail fast with a clear message instead of
     * later, with a less informative error from the storage layer.
     *
     * @throws IllegalArgumentException if the combination is invalid
     */
    public void validateRoleAndShadow() {
        final String role = getString(PROPERTY_ROLE, PROPERTY_ROLE_DEFAULT);
        if (!ROLE_PRIMARY.equals(role) && !ROLE_SHADOW.equals(role)) {
            throw new IllegalArgumentException(
                    PROPERTY_ROLE + " must be '" + ROLE_PRIMARY + "' or '"
                            + ROLE_SHADOW + "', got: '" + role + "'");
        }
        final int numInstances = getInt(PROPERTY_NUM_INSTANCES, PROPERTY_NUM_INSTANCES_DEFAULT);
        if (ROLE_SHADOW.equals(role)) {
            final int shadowOf = getInt(PROPERTY_SHADOW_OF, PROPERTY_SHADOW_OF_UNSET);
            if (shadowOf == PROPERTY_SHADOW_OF_UNSET) {
                throw new IllegalArgumentException(
                        PROPERTY_SHADOW_OF + " must be set when "
                                + PROPERTY_ROLE + "=" + ROLE_SHADOW);
            }
            if (shadowOf < 0 || shadowOf >= numInstances) {
                throw new IllegalArgumentException(
                        PROPERTY_SHADOW_OF + "=" + shadowOf
                                + " is out of range [0, " + (numInstances - 1) + "] for "
                                + PROPERTY_NUM_INSTANCES + "=" + numInstances);
            }
            final String storageType = getString(PROPERTY_STORAGE_TYPE, PROPERTY_STORAGE_TYPE_DEFAULT);
            if (!"remote".equals(storageType)) {
                throw new IllegalArgumentException(
                        PROPERTY_ROLE + "=" + ROLE_SHADOW + " requires "
                                + PROPERTY_STORAGE_TYPE + "=remote, got: '" + storageType + "'");
            }
        }
    }

    /** Returns {@code true} when {@link #PROPERTY_ROLE} is {@link #ROLE_SHADOW}. */
    public boolean isShadow() {
        return ROLE_SHADOW.equals(getString(PROPERTY_ROLE, PROPERTY_ROLE_DEFAULT));
    }

    /**
     * Returns {@code true} when {@link #PROPERTY_BOOTSTRAP_FROM_REBALANCE}
     * is enabled — engine bootstraps via the next REBALANCE log entry rather
     * than replaying the commit log from {@code START_OF_TIME}.
     */
    public boolean isBootstrapFromRebalance() {
        return getBoolean(PROPERTY_BOOTSTRAP_FROM_REBALANCE,
                PROPERTY_BOOTSTRAP_FROM_REBALANCE_DEFAULT);
    }

    @Override
    public String toString() {
        return "IndexingServerConfiguration{" + "properties=" + properties + '}';
    }
}
