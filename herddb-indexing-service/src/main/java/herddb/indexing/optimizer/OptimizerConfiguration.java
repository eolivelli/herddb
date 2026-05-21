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
     * under {@code /<zookeeper-path>/fileServers} (e.g. {@code /herd/fileServers})
     * and calls the optimizer's {@link herddb.metadata.ServiceDiscoveryListener},
     * which schedules a merger upgrade tick so a startup-time {@code NoopMerger}
     * is replaced as soon as the file server appears in ZooKeeper (issue #507).
     *
     * <p><b>Note on {@code safeModeFileDeletion=false}</b>: if no file servers are
     * visible at optimizer startup, the engine is constructed with
     * {@code safeModeFileDeletion=true} regardless of this property, and a
     * {@code SEVERE} log is emitted. Physical file deletion will remain disabled
     * until a pod restart. Ensure the file-server pod starts before the optimizer
     * to avoid this fallback.
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

    // -------------------------------------------------------------------------
    // Direct S3 access for streaming compaction (issue #609)
    //
    // Mirrors the IndexingServerConfiguration S3 block (issue #381). When
    // {@code indexoptimizer.s3.direct.enabled=true} the optimizer pod attaches
    // an {@link herddb.remote.storage.ObjectStorage} client to its
    // {@link herddb.remote.RemoteFileDataStorageManager} so the
    // {@code RemoteSegmentGraphMerger} downloads input segment graph/map
    // files directly from object storage instead of routing them through the
    // gRPC file-server. This is the optimizer-side counterpart of the
    // streaming-compaction eager-download fast path: it eliminates wire-level
    // contention against the file-server's bounded {@code max.inflight.read.bytes}
    // semaphore and lifts merge throughput on large segments.
    //
    // Access/secret keys are read at runtime from the {@code S3_ACCESS_KEY} and
    // {@code S3_SECRET_KEY} environment variables (never from the properties
    // file) so they are not visible in ConfigMaps or log output.
    // -------------------------------------------------------------------------

    /**
     * Enable direct object-storage download for streaming compaction input
     * segments. Default {@code false}.
     *
     * <p>When {@code true}, the optimizer opens an S3/GCS client and downloads
     * input segment graph/map files for streaming compaction directly from
     * the bucket, bypassing the gRPC file-server. Requires the optimizer to
     * be wired against a {@link herddb.remote.RemoteFileDataStorageManager}
     * — when the DSM is anything else (e.g. test fallbacks) a WARNING is
     * logged and the flag is silently ignored.
     *
     * <p><strong>Note:</strong> the configured bucket ({@link #PROPERTY_S3_BUCKET})
     * is assumed to be pre-provisioned and accessible at startup. A missing
     * bucket surfaces as a storage error on the first merge attempt.
     */
    public static final String PROPERTY_S3_DIRECT_ENABLED = "indexoptimizer.s3.direct.enabled";
    public static final boolean PROPERTY_S3_DIRECT_ENABLED_DEFAULT = false;

    /** S3 endpoint URL override. Leave empty for native AWS S3; set for GCS or MinIO. */
    public static final String PROPERTY_S3_ENDPOINT = "indexoptimizer.s3.endpoint";
    public static final String PROPERTY_S3_ENDPOINT_DEFAULT = "";

    /** S3 bucket name containing the segment data. */
    public static final String PROPERTY_S3_BUCKET = "indexoptimizer.s3.bucket";
    public static final String PROPERTY_S3_BUCKET_DEFAULT = "";

    /** AWS region. Used for native AWS S3; typically {@code "auto"} for GCS. */
    public static final String PROPERTY_S3_REGION = "indexoptimizer.s3.region";
    public static final String PROPERTY_S3_REGION_DEFAULT = "us-east-1";

    /** Optional key prefix within the bucket (e.g. {@code "herddb/"}). */
    public static final String PROPERTY_S3_PREFIX = "indexoptimizer.s3.prefix";
    public static final String PROPERTY_S3_PREFIX_DEFAULT = "";

    /**
     * Enable GCS-compatibility mode on the S3 client:
     * path-style addressing + SDK checksums WHEN_REQUIRED.
     * Required for GCS and MinIO. Default {@code false}.
     */
    public static final String PROPERTY_S3_GCS_COMPATIBILITY = "indexoptimizer.s3.gcs.compatibility";
    public static final boolean PROPERTY_S3_GCS_COMPATIBILITY_DEFAULT = false;

    /**
     * Issue #638: when {@code true} (the default), and direct S3 is enabled
     * via {@link #PROPERTY_S3_DIRECT_ENABLED}, segment uploads produced by
     * the optimizer's streaming compactor go <em>directly</em> to S3/MinIO
     * via the S3 Multipart Upload API (driven by {@code S3TransferManager})
     * instead of being routed through the gRPC file-server.
     *
     * <p>Setting this to {@code false} keeps direct reads on but reverts
     * writes to the gRPC file-server. Mirrors the IS-server flag
     * {@code indexing.s3.direct.write.enabled}.
     */
    public static final String PROPERTY_S3_DIRECT_WRITE_ENABLED =
            "indexoptimizer.s3.direct.write.enabled";
    /**
     * Default is {@code false} for the initial release — mirrors the IS-server
     * flag policy. Enable explicitly after verifying the bulk upload path in
     * your environment.
     */
    public static final boolean PROPERTY_S3_DIRECT_WRITE_ENABLED_DEFAULT = false;

    /**
     * Issue #638: maximum number of bytes that may be in flight across
     * concurrent direct-S3 multipart uploads issued by the optimizer.
     * Independent of any gRPC write-plane budget so the two paths cannot
     * starve one another. Default 512 MiB.
     */
    public static final String PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES =
            "indexoptimizer.remote.file.client.max.inflight.direct.write.bytes";
    public static final long PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES_DEFAULT =
            512L * 1024 * 1024;

    // -------------------------------------------------------------------------
    // K-way single-pass merge (issue #524)
    // -------------------------------------------------------------------------

    /**
     * K-way merge max (issue #524). When {@code >= 2}, the
     * {@link MergePolicy.AggressivePolicy} picks up to this many sub-target
     * segments per cycle and merges them in a single pass, bypassing the
     * {@code perCycleMaxBytes} cap. This collapses N segments into one merge
     * round instead of the O(N) rounds that the byte-cap forces, cutting
     * cumulative vector-processing work from O(N²) to O(N).
     *
     * <p>Default {@code 0} (disabled = legacy byte-cap mode): existing deployments
     * are not silently affected by the larger per-cycle input footprint that k-way
     * implies. Operators opt in by setting this to {@code >= 2} (recommended
     * {@code 8} for the gist1m / 8-initial-segments workload that motivated the
     * issue; raise further for tablespaces with more initial segments) once they
     * have verified the optimizer pod has sufficient heap and local disk for the
     * fan-in. Set back to {@code 0} to revert to legacy byte-cap behaviour
     * ({@code perCycleMaxBytes} re-applies).
     *
     * <p>The hard ceiling {@code optimizer.merge.max.count} (default 200) is
     * always respected regardless of this setting.
     */
    public static final String PROPERTY_MERGE_KWAY_MAX = "indexoptimizer.merge.kway.max";
    public static final int PROPERTY_MERGE_KWAY_MAX_DEFAULT = 0;

    /**
     * Hard cap on the total bytes of source segment files that a single
     * optimizer merge cycle may include (issue #602). After candidate
     * selection, the largest candidates are trimmed until the total
     * {@code sizeBytes} is within this limit. At least 2 candidates are
     * always kept so a merge is still attempted.
     * Default is 100 GiB ({@code 107374182400}).
     * Set to {@link Long#MAX_VALUE} to disable.
     */
    public static final String PROPERTY_MERGE_MAX_INPUT_BYTES =
            "indexoptimizer.merge.max.input.bytes";
    public static final long PROPERTY_MERGE_MAX_INPUT_BYTES_DEFAULT =
            100L * 1024 * 1024 * 1024; // 100 GiB

    // -------------------------------------------------------------------------
    // Horizontal scalability: pod role detection (step 1)
    // -------------------------------------------------------------------------

    /**
     * Explicit pod role override. {@code true} / {@code false} forces leader /
     * worker; {@code auto} (the default) falls back to the environment-variable
     * and hostname-regex heuristics in {@link OptimizerRole#detect}.
     */
    public static final String PROPERTY_ROLE_IS_LEADER = "indexoptimizer.role.is.leader";
    public static final String PROPERTY_ROLE_IS_LEADER_DEFAULT = "auto";

    /**
     * Name of the env var that carries this pod's StatefulSet ordinal. Helm
     * wires this from the K8s downward API field
     * {@code metadata.labels['apps.kubernetes.io/pod-index']}.
     */
    public static final String PROPERTY_ROLE_POD_ORDINAL_ENV = "indexoptimizer.role.pod.ordinal.env";
    public static final String PROPERTY_ROLE_POD_ORDINAL_ENV_DEFAULT = "POD_ORDINAL";

    /** Regex extracting the ordinal from {@code HOSTNAME} when the env var is absent. */
    public static final String PROPERTY_ROLE_HOSTNAME_ORDINAL_REGEX =
            "indexoptimizer.role.hostname.ordinal.regex";
    public static final String PROPERTY_ROLE_HOSTNAME_ORDINAL_REGEX_DEFAULT = "^.*-(\\d+)$";

    /**
     * When {@code false}, the leader produces tasks but never consumes them —
     * pure scheduler mode. Used by the K8s multi-replica acceptance test to
     * prove that a worker pod actually performs the merge work; also a
     * legitimate operator knob for "dedicated scheduler" deployments.
     */
    public static final String PROPERTY_ROLE_LEADER_EXECUTE_TASKS =
            "indexoptimizer.role.leader.execute.tasks";
    public static final boolean PROPERTY_ROLE_LEADER_EXECUTE_TASKS_DEFAULT = true;

    // -------------------------------------------------------------------------
    // Owner-instance selector (step 1 introduces; step 2 makes LEAST_LOADED default)
    // -------------------------------------------------------------------------

    /**
     * Owner-selection policy applied at task creation time. One of
     * {@code FIXED_ZERO}, {@code ROUND_ROBIN}, {@code STATIC}, {@code LEAST_LOADED}.
     * Default since step 2 is {@code LEAST_LOADED} (load-aware assignment via
     * live-instance discovery from ZK ephemerals). Single-IS deployments and
     * tests with no instance ephemerals keep producing {@code ownerInstanceId=0}
     * because {@link ZkIndexingServiceInstanceDirectory} falls back to
     * {@code [0..PROPERTY_INDEXING_NUM_INSTANCES)} (default 0 → empty → selector
     * throws, engine logs and retries on the next tick).
     */
    public static final String PROPERTY_OWNER_SELECTOR_POLICY = "indexoptimizer.owner.selector.policy";
    public static final String PROPERTY_OWNER_SELECTOR_POLICY_DEFAULT = "LEAST_LOADED";

    /**
     * Comma-separated list of instance ordinals consumed cyclically by
     * {@link StaticAssignmentOwnerSelector}. Read only when policy=STATIC.
     */
    public static final String PROPERTY_OWNER_SELECTOR_STATIC_ASSIGNMENT =
            "indexoptimizer.owner.selector.static.assignment";

    /**
     * Number of indexing-service instances the optimizer will assign segments
     * across. {@code 0} (default) means "auto-discover from ZK". The
     * auto-discovery wiring lands in step 2; until then operators that want
     * non-zero owner ordinals must set this knob explicitly.
     */
    public static final String PROPERTY_INDEXING_NUM_INSTANCES = "indexoptimizer.indexing.num.instances";
    public static final int PROPERTY_INDEXING_NUM_INSTANCES_DEFAULT = 0;

    // -------------------------------------------------------------------------
    // Task queue lifecycle knobs (step 5)
    // -------------------------------------------------------------------------

    /**
     * Maximum retries for a failed task before it is moved to {@code POISON}
     * (terminal-for-retry but does not block input re-selection).
     */
    public static final String PROPERTY_TASKS_MAX_ATTEMPTS = "indexoptimizer.tasks.max.attempts";
    public static final int PROPERTY_TASKS_MAX_ATTEMPTS_DEFAULT = 3;

    /**
     * Retention window for terminal {@code DONE} / {@code FAILED} tasks before
     * the orphan scanner GCs them. {@code POISON} uses
     * {@link #PROPERTY_TASKS_POISON_RETENTION_MS} so operators have a longer
     * window to investigate.
     */
    public static final String PROPERTY_TASKS_TERMINAL_RETENTION_MS =
            "indexoptimizer.tasks.terminal.retention.ms";
    public static final long PROPERTY_TASKS_TERMINAL_RETENTION_MS_DEFAULT = 3_600_000L;

    /**
     * Retention window for {@code POISON} tasks. Longer than the regular
     * terminal retention because operators need time to investigate (default
     * 7 days).
     */
    public static final String PROPERTY_TASKS_POISON_RETENTION_MS =
            "indexoptimizer.tasks.poison.retention.ms";
    public static final long PROPERTY_TASKS_POISON_RETENTION_MS_DEFAULT = 604_800_000L;

    /**
     * Minimum age of a {@code CLAIMED} task with no live lease znode before
     * the orphan scanner considers it orphaned and resets / poisons it.
     * Operators MUST configure this to be ≥ 2 × {@code zookeeper.session.timeout}
     * so an actually-alive worker whose session is briefly disconnected does
     * not get its task yanked from under it.
     */
    public static final String PROPERTY_TASKS_ORPHAN_RESET_MS =
            "indexoptimizer.tasks.orphan.reset.ms";
    // 120 s default — comfortably above the 2 × 40 s default ZK session timeout
    // floor enforced at startup (see IndexOptimizerMain.start).
    public static final long PROPERTY_TASKS_ORPHAN_RESET_MS_DEFAULT = 120_000L;

    /**
     * Maximum number of consumer iterations per scheduler tick. Bounds the
     * time spent draining the queue so the leader's producer step still gets
     * cycles when the pod is busy.
     */
    public static final String PROPERTY_CONSUMER_MAX_TASKS_PER_TICK =
            "indexoptimizer.consumer.max.tasks.per.tick";
    public static final int PROPERTY_CONSUMER_MAX_TASKS_PER_TICK_DEFAULT = 4;

    // -------------------------------------------------------------------------
    // Atomic swap protocol (issue #555)
    // -------------------------------------------------------------------------

    /**
     * Maximum time the consumer waits for every interested IS pod to write its
     * ephemeral ack znode under
     * {@code {basePath}/index-segments-acks/{outputSegmentUuid}/{serviceId}}
     * before aborting the swap (issue #555). On timeout, the consumer deletes
     * the staged output's multipart files + znode + acks subtree, leaves
     * inputs ACTIVE, and transitions the task to FAILED (then POISON on
     * {@code maxAttempts}).
     *
     * <p>Default 60 seconds. Should comfortably exceed the worst-case time
     * for the new owner + every shadow replica to download the merged
     * multipart files from remote storage and open them. Increase for very
     * large segments or slow remote-storage backends.
     */
    public static final String PROPERTY_SWAP_ACK_TIMEOUT_MS =
            "indexoptimizer.swap.ack.timeout.ms";
    public static final long PROPERTY_SWAP_ACK_TIMEOUT_MS_DEFAULT = 60_000L;

    /**
     * Minimum age of a stale PROVISIONAL output znode before the orphan
     * scanner considers it abandoned and triggers the abort path (multipart
     * file deletion + znode delete + acks subtree delete) on behalf of a
     * dead optimizer (issue #555). Used together with the AWAITING_ACK
     * task scan: if the task is still AWAITING_ACK and its
     * {@code provisionalOutputCreatedAtEpochMillis} is older than the
     * configured swap-ack timeout, the orphan scanner aborts.
     *
     * <p>Default 600 seconds (10 × swap-ack timeout). Independent of the
     * regular task retention so an operator inspecting a stuck task still
     * has a chance to see the AWAITING_ACK state in {@code /status} for a
     * reasonable window.
     */
    public static final String PROPERTY_PROVISIONAL_GC_MS =
            "indexoptimizer.provisional.gc.ms";
    public static final long PROPERTY_PROVISIONAL_GC_MS_DEFAULT = 600_000L;

    // CRT HTTP-client tuning — shared keys and defaults live in CrtS3HttpClientFactory.
    // All services use the same property name: s3.crt.max.concurrency / s3.crt.read.buffer.size

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
