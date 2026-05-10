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
package herddb.indexing.optimizer;

import java.math.BigDecimal;
import java.util.Properties;

/**
 * Configuration knobs for the index-optimizer service. Reuses several keys
 * from {@code IndexingServerConfiguration} (zookeeper, metrics, http) and adds
 * optimizer-only ones for the merge policy.
 */
public final class OptimizerConfiguration {

    /** ZooKeeper connection string (e.g. {@code host1:2181,host2:2181}). */
    public static final String PROPERTY_ZOOKEEPER_ADDRESS = "indexoptimizer.zookeeper.address";
    public static final String PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT = "localhost:2181";

    /** ZooKeeper session timeout in milliseconds. */
    public static final String PROPERTY_ZOOKEEPER_SESSION_TIMEOUT = "indexoptimizer.zookeeper.session.timeout";
    public static final int PROPERTY_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT = 40000;

    /** ZooKeeper base path (matches the herddb cluster's {@code server.zookeeper.path}). */
    public static final String PROPERTY_ZOOKEEPER_PATH = "indexoptimizer.zookeeper.path";
    public static final String PROPERTY_ZOOKEEPER_PATH_DEFAULT = "/herd";

    /**
     * Tablespace name the optimizer manages (e.g. {@code "herd"} — the HerdDB default).
     * The optimizer resolves the UUID from ZooKeeper at startup using
     * {@link herddb.cluster.ZookeeperMetadataStorageManager#describeTableSpace(String)}.
     */
    public static final String PROPERTY_TABLESPACE_NAME = "indexoptimizer.tablespace.name";

    /** Polling interval for the registry scan, in milliseconds. */
    public static final String PROPERTY_INTERVAL_MS = "indexoptimizer.interval.ms";
    public static final long PROPERTY_INTERVAL_MS_DEFAULT = 5L * 60_000L;

    /**
     * Minimum number of mergeable segments to consider a merge run. Below this,
     * the policy waits for more segments to accumulate.
     */
    public static final String PROPERTY_MIN_COUNT = "indexoptimizer.merge.min.count";
    public static final int PROPERTY_MIN_COUNT_DEFAULT = 4;

    /**
     * Hard ceiling on segment count before a merge is forced (mirrors the
     * IS-side {@code indexing.vector.compaction.max.count} from issue #285).
     */
    public static final String PROPERTY_MAX_COUNT = "indexoptimizer.merge.max.count";
    public static final int PROPERTY_MAX_COUNT_DEFAULT = 200;

    /** Minimum total size of mergeable segments to consider a merge run. */
    public static final String PROPERTY_MIN_BYTES = "indexoptimizer.merge.min.bytes";
    public static final long PROPERTY_MIN_BYTES_DEFAULT = 256L * 1024 * 1024;

    /** Per-run cap on input segment bytes. */
    public static final String PROPERTY_MAX_BYTES = "indexoptimizer.merge.max.bytes";
    public static final long PROPERTY_MAX_BYTES_DEFAULT = 1024L * 1024 * 1024;

    /**
     * Retention window for DEPRECATED segments before transitioning them to DELETED
     * and removing the multipart files. Should comfortably exceed a search RTT
     * across the cluster so in-flight queries do not see a torn read.
     */
    public static final String PROPERTY_RETENTION_MS = "indexoptimizer.retention.ms";
    public static final long PROPERTY_RETENTION_MS_DEFAULT = 10L * 60_000L;

    /**
     * When {@code true} (the default), the reaper does NOT physically delete
     * graph/map/tombstone files at retention — it only removes the registry
     * znode. Production deployments require the IS-side
     * {@code SegmentAssignmentWatcher} to be wired before flipping this to
     * {@code false}; otherwise the IS will fail to load on restart with
     * file-not-found (review-item B1).
     */
    public static final String PROPERTY_SAFE_MODE_FILE_DELETION =
            "indexoptimizer.safeMode.fileDeletion";
    public static final boolean PROPERTY_SAFE_MODE_FILE_DELETION_DEFAULT = true;

    /** HTTP admin endpoint port (review item E1 + E3). 0 disables. */
    public static final String PROPERTY_HTTP_PORT = "indexoptimizer.http.port";
    public static final int PROPERTY_HTTP_PORT_DEFAULT = 9853;

    /** HTTP admin endpoint bind host. */
    public static final String PROPERTY_HTTP_HOST = "indexoptimizer.http.host";
    public static final String PROPERTY_HTTP_HOST_DEFAULT = "0.0.0.0";

    // -------------------------------------------------------------------------
    // Aggressive merge policy + event-driven scheduling (issue #484)
    // -------------------------------------------------------------------------

    /**
     * Per-graph "graduated" cap used by the {@link MergePolicy.AggressivePolicy}.
     * Segments at or above this size are excluded from the merge candidate set
     * — they are considered "done". Default 8 GiB.
     */
    public static final String PROPERTY_TARGET_MAX_BYTES = "indexoptimizer.merge.target.bytes";
    public static final long PROPERTY_TARGET_MAX_BYTES_DEFAULT = 8L * 1024L * 1024L * 1024L;

    /**
     * Local scratch directory used by the merger to stage downloaded map
     * files and the merged graph + map outputs before upload. Helm injects
     * a PVC mount at {@code /opt/herddb/optimizer-tmp} via the
     * {@code -Dindexoptimizer.tmp.dir=...} system property.
     */
    public static final String PROPERTY_TMP_DIR = "indexoptimizer.tmp.dir";

    /**
     * Debounce window for the ZK persistent-recursive watch that drives
     * event-driven scheduling. Bursts of children-changed events are
     * coalesced into a single {@code runOnce()} after this many millis of
     * quiet, so a hundred new segments at ingest peak still produce a
     * single tick. 0 disables debouncing (every event triggers a tick).
     */
    public static final String PROPERTY_EVENT_DEBOUNCE_MS = "indexoptimizer.event.debounce.ms";
    public static final long PROPERTY_EVENT_DEBOUNCE_MS_DEFAULT = 500L;

    // jvector params used by the merger when rebuilding the merged graph.
    // Defaults aligned to {@code PersistentVectorStore} so the merged segment
    // has the same recall characteristics as in-IS-built segments.

    /**
     * Streaming compaction (issue #485). When {@code true}, the optimizer
     * pod drives the merge through jvector's
     * {@code OnDiskGraphIndexCompactor} (memory bounded by
     * {@code O(taskWindowSize × maxDegree × float[dim])}); when {@code
     * false}, the legacy in-memory {@code GraphIndexBuilder} rebuild path
     * is used instead.
     *
     * <p>The optimizer pod runs in a separate process from the IS, so the
     * IS-side config key {@code vector.index.compaction.streaming.enabled}
     * does NOT reach this pod. Operators must set this key on the
     * optimizer's own configuration to honor the escape hatch on the
     * optimizer side (the IS-local path obeys the IS-side key
     * independently). Default {@code true}; the JVM system property
     * {@code herddb.vectorindex.streamingCompactionEnabled} is consumed
     * as the static initializer for the underlying flag — this config
     * key takes precedence at {@code IndexOptimizerMain.start()}.
     */
    public static final String PROPERTY_MERGE_STREAMING_ENABLED =
            "indexoptimizer.merge.streaming.enabled";
    public static final boolean PROPERTY_MERGE_STREAMING_ENABLED_DEFAULT = true;

    /** jvector graph degree (edges per node). */
    public static final String PROPERTY_MERGE_M = "indexoptimizer.merge.M";
    public static final int PROPERTY_MERGE_M_DEFAULT = 16;

    /** jvector beam-width during graph build. */
    public static final String PROPERTY_MERGE_BEAM_WIDTH = "indexoptimizer.merge.beamWidth";
    public static final int PROPERTY_MERGE_BEAM_WIDTH_DEFAULT = 100;

    /** jvector neighbor-overflow factor during graph build. */
    public static final String PROPERTY_MERGE_NEIGHBOR_OVERFLOW = "indexoptimizer.merge.neighborOverflow";
    public static final float PROPERTY_MERGE_NEIGHBOR_OVERFLOW_DEFAULT = 1.2f;

    /** jvector alpha factor during graph build. */
    public static final String PROPERTY_MERGE_ALPHA = "indexoptimizer.merge.alpha";
    public static final float PROPERTY_MERGE_ALPHA_DEFAULT = 1.4f;

    /**
     * Vector similarity used by the merger. Must match the value used by the
     * IS that produced the inputs, otherwise PQ training and graph
     * construction will be incoherent. Accepted values match
     * {@code io.github.jbellis.jvector.vector.VectorSimilarityFunction}:
     * {@code DOT_PRODUCT}, {@code COSINE}, {@code EUCLIDEAN}.
     */
    public static final String PROPERTY_MERGE_SIMILARITY = "indexoptimizer.merge.similarity";
    public static final String PROPERTY_MERGE_SIMILARITY_DEFAULT = "DOT_PRODUCT";

    // -------------------------------------------------------------------------
    // Remote file service client (issue #484: merger needs a DataStorageManager
    // pointing at remote storage to download/upload segment files). The
    // optimizer accepts a comma-separated static list OR — when empty —
    // discovers servers via ZooKeeper at the standard
    // {@code /herd/file-servers} path.
    // -------------------------------------------------------------------------

    /**
     * Comma-separated list of remote file-service endpoints.
     * When empty (the default), the optimizer discovers file servers via the
     * long-lived {@link herddb.cluster.ZookeeperMetadataStorageManager} that stays
     * open for the pod's lifetime. The built-in {@code fileServersWatcher} in
     * {@code ZookeeperMetadataStorageManager} reacts to ZK children-changed events
     * under {@code /herd/fileServers} and calls the optimizer's
     * {@link herddb.metadata.ServiceDiscoveryListener}, which schedules a merger
     * upgrade tick so a startup-time {@code NoopMerger} is replaced as soon as
     * the file server appears in ZooKeeper (issue #507).
     */
    public static final String PROPERTY_REMOTE_FILE_SERVERS = "indexoptimizer.remote.file.servers";
    public static final String PROPERTY_REMOTE_FILE_SERVERS_DEFAULT = "";

    /** Per-call deadline for the remote-file client, in seconds. */
    public static final String PROPERTY_REMOTE_FILE_TIMEOUT = "indexoptimizer.remote.file.client.timeout";
    public static final long PROPERTY_REMOTE_FILE_TIMEOUT_DEFAULT = 60L;

    /** Max retries on idempotent remote-file operations. */
    public static final String PROPERTY_REMOTE_FILE_RETRIES = "indexoptimizer.remote.file.client.retries";
    public static final int PROPERTY_REMOTE_FILE_RETRIES_DEFAULT = 3;

    /** Back-pressure cap on outstanding read bytes on the remote-file client. */
    public static final String PROPERTY_REMOTE_FILE_MAX_INFLIGHT_READ_BYTES =
            "indexoptimizer.remote.file.client.max.inflight.read.bytes";
    public static final long PROPERTY_REMOTE_FILE_MAX_INFLIGHT_READ_BYTES_DEFAULT = 256L * 1024L * 1024L;

    /** Back-pressure cap on outstanding write bytes on the remote-file client. */
    public static final String PROPERTY_REMOTE_FILE_MAX_INFLIGHT_WRITE_BYTES =
            "indexoptimizer.remote.file.client.max.inflight.write.bytes";
    public static final long PROPERTY_REMOTE_FILE_MAX_INFLIGHT_WRITE_BYTES_DEFAULT = 256L * 1024L * 1024L;

    private final Properties properties;

    public OptimizerConfiguration(Properties properties) {
        this.properties = properties == null ? new Properties() : properties;
    }

    public String getString(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        String v = properties.getProperty(key);
        if (v == null) {
            return defaultValue;
        }
        // Integer.parseInt rejects scientific-notation strings (e.g. "2e+08") emitted
        // by YAML/Helm for large integer literals. BigDecimal handles both forms;
        // intValueExact() throws ArithmeticException on a fractional value, giving a
        // clear startup error instead of silently truncating.
        return new BigDecimal(v).intValueExact();
    }

    public long getLong(String key, long defaultValue) {
        String v = properties.getProperty(key);
        if (v == null) {
            return defaultValue;
        }
        // Long.parseLong rejects scientific-notation strings (e.g. "2.68435456e+08")
        // emitted by YAML/Helm for large integer literals. BigDecimal handles both
        // forms; longValueExact() throws ArithmeticException on a fractional value.
        return new BigDecimal(v).longValueExact();
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String v = properties.getProperty(key);
        return v == null ? defaultValue : Boolean.parseBoolean(v);
    }

    public float getFloat(String key, float defaultValue) {
        String v = properties.getProperty(key);
        if (v == null) {
            return defaultValue;
        }
        return Float.parseFloat(v);
    }
}
