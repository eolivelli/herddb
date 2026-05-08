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
}
