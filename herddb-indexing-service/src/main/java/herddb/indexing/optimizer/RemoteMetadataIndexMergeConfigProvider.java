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

import herddb.index.vector.VectorIndexManager;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.server.RemoteFileClient;
import herddb.utils.ExtendedDataInputStream;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link IndexMergeConfigProvider} that reads per-index jvector build
 * parameters directly from the checkpoint metadata stored on the remote file
 * server (issue #516).
 *
 * <h3>Metadata source</h3>
 * <p>On every HerdDB checkpoint the leader publishes two files to the remote
 * file server (S3 / MinIO):
 * <ol>
 *   <li>{@code {tablespaceUuid}/_metadata/latest.checkpoint} — the LSN of the
 *       most recent committed checkpoint.</li>
 *   <li>{@code {tablespaceUuid}/_metadata/indexes.{ledger}.{offset}.tablesmetadata}
 *       — a binary blob containing all {@link Index} definitions at that LSN,
 *       including the full {@code properties} map from the original
 *       {@code CREATE VECTOR INDEX ... WITH (...)} DDL.</li>
 * </ol>
 * <p>The {@code properties} map contains the keys defined in
 * {@link VectorIndexManager}: {@code m}, {@code beamWidth},
 * {@code neighborOverflow}, {@code alpha}, {@code similarity}.
 * These are exactly the parameters needed by {@link RemoteSegmentMerger} to
 * rebuild the merged graph with the same characteristics as the original
 * in-IS segments.
 *
 * <h3>Why not the IS gRPC API?</h3>
 * <p>The IS may be in a degraded state — that is often exactly why the
 * optimizer is running a merge. Reading from the remote file server avoids any
 * dependency on IS liveness: if the HerdDB leader has checkpointed at least
 * once, the metadata is available.
 *
 * <h3>Caching</h3>
 * <p>Results are cached per {@code indexUuid} with a configurable TTL
 * (default {@value #DEFAULT_CACHE_TTL_MS} ms). A cache miss triggers a
 * fresh read of the latest checkpoint LSN followed by a read of the index
 * definitions file. This is a small number of remote reads (two short files)
 * and happens at most once per TTL per index.
 *
 * <h3>Error semantics</h3>
 * <p>If the required property ({@code m}, {@code beamWidth}, etc.) is absent
 * from the index definition, this method throws {@link Exception} with a clear
 * message naming the missing key and the index. There is no fallback to
 * global defaults: a missing property indicates a misconfigured index and
 * must be fixed at the DDL level.
 *
 * <h3>Thread safety</h3>
 * <p>Safe for concurrent reads. The cache uses {@link ConcurrentHashMap}.
 */
public final class RemoteMetadataIndexMergeConfigProvider
        implements IndexMergeConfigProvider {

    private static final Logger LOGGER =
            Logger.getLogger(RemoteMetadataIndexMergeConfigProvider.class.getName());

    /** Default cache TTL: 5 minutes. */
    public static final long DEFAULT_CACHE_TTL_MS = 5L * 60_000L;

    private final RemoteFileClient fileClient;
    private final String tablespaceUuid;
    private final long cacheTtlMs;

    /** Cache entries keyed by indexUuid. */
    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();

    private static final class CacheEntry {
        final IndexMergeConfig config;
        final long expiresAtMs;

        CacheEntry(IndexMergeConfig config, long expiresAtMs) {
            this.config = config;
            this.expiresAtMs = expiresAtMs;
        }
    }

    /**
     * @param fileClient     remote file client used to read checkpoint metadata
     * @param tablespaceUuid UUID of the tablespace the optimizer manages
     * @param cacheTtlMs     how long a cached config entry is valid (ms);
     *                       pass {@code <= 0} to use the default 5-minute TTL
     */
    public RemoteMetadataIndexMergeConfigProvider(RemoteFileClient fileClient,
                                                  String tablespaceUuid,
                                                  long cacheTtlMs) {
        this.fileClient = Objects.requireNonNull(fileClient, "fileClient");
        this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.cacheTtlMs = cacheTtlMs > 0 ? cacheTtlMs : DEFAULT_CACHE_TTL_MS;
    }

    /** Convenience constructor using the default cache TTL. */
    public RemoteMetadataIndexMergeConfigProvider(RemoteFileClient fileClient,
                                                  String tablespaceUuid) {
        this(fileClient, tablespaceUuid, DEFAULT_CACHE_TTL_MS);
    }

    @Override
    public IndexMergeConfig getMergeConfig(String tablespace, String tableName,
                                           String indexName, String indexUuid)
            throws Exception {
        long now = System.currentTimeMillis();
        CacheEntry entry = cache.get(indexUuid);
        if (entry != null && now < entry.expiresAtMs) {
            return entry.config;
        }
        IndexMergeConfig fresh = fetchFromRemoteMetadata(tableName, indexName, indexUuid);
        cache.put(indexUuid, new CacheEntry(fresh, now + cacheTtlMs));
        return fresh;
    }

    /**
     * Invalidates the entire cache. Useful after a ZK reconnect or an explicit
     * operator request to force re-reading configs from the remote file server.
     */
    public void invalidateCache() {
        cache.clear();
    }

    // -------------------------------------------------------------------------
    // Internal implementation
    // -------------------------------------------------------------------------

    private IndexMergeConfig fetchFromRemoteMetadata(String tableName,
                                                     String indexName,
                                                     String indexUuid) throws Exception {
        LogSequenceNumber lsn = readLatestCheckpointLsn();
        if (lsn.isStartOfTime()) {
            throw new Exception(
                    "No checkpoint has been published yet for tablespace " + tablespaceUuid
                    + " on the remote file server. The HerdDB leader must complete at least"
                    + " one checkpoint before the optimizer can resolve index build parameters.");
        }
        List<Index> indexes = readIndexDefinitions(lsn);
        // Find the index by UUID (primary key) then by name as a fallback for
        // deployments where the UUID may not be set in older metadata snapshots.
        Index found = null;
        for (Index idx : indexes) {
            if (indexUuid.equals(idx.uuid)) {
                found = idx;
                break;
            }
        }
        if (found == null) {
            // Fallback: match by (table, indexName) in case the UUID changed.
            for (Index idx : indexes) {
                if (tableName.equals(idx.table) && indexName.equals(idx.name)) {
                    found = idx;
                    LOGGER.log(Level.FINE,
                            "found index {0}.{1} by name (uuid mismatch: stored={2}, wanted={3})",
                            new Object[]{tableName, indexName, idx.uuid, indexUuid});
                    break;
                }
            }
        }
        if (found == null) {
            throw new Exception(
                    "Index not found in checkpoint metadata for tablespace " + tablespaceUuid
                    + " at LSN " + lsn + ": table=" + tableName + ", index=" + indexName
                    + ", uuid=" + indexUuid + ". Available indexes: " + summarizeIndexes(indexes));
        }
        return parseConfig(found);
    }

    /**
     * Parses jvector build parameters from {@code Index.properties}. Throws
     * {@link Exception} for any required property that is absent or invalid —
     * no fallback to defaults is applied.
     */
    private static IndexMergeConfig parseConfig(Index idx) throws Exception {
        int graphM = requirePositiveInt(idx, VectorIndexManager.PROP_M);
        int beamWidth = requirePositiveInt(idx, VectorIndexManager.PROP_BEAM_WIDTH);
        float neighborOverflow = requirePositiveFloat(idx, VectorIndexManager.PROP_NEIGHBOR_OVERFLOW);
        float alpha = requirePositiveFloat(idx, VectorIndexManager.PROP_ALPHA);
        String similarityName = requireNonEmptyString(idx, VectorIndexManager.PROP_SIMILARITY);
        VectorSimilarityFunction similarity;
        try {
            similarity = VectorSimilarityFunction.valueOf(similarityName);
        } catch (IllegalArgumentException badName) {
            throw new Exception(
                    "Index " + idx.table + "." + idx.name + " (uuid=" + idx.uuid + ")"
                    + " has unsupported similarity '" + similarityName
                    + "' (expected one of "
                    + Arrays.toString(VectorSimilarityFunction.values()) + ")",
                    badName);
        }
        IndexMergeConfig config = new IndexMergeConfig(graphM, beamWidth, neighborOverflow, alpha, similarity);
        LOGGER.log(Level.INFO,
                "resolved merge config for index {0}.{1} (uuid={2}): {3}",
                new Object[]{idx.table, idx.name, idx.uuid, config});
        return config;
    }

    private static int requirePositiveInt(Index idx, String key) throws Exception {
        String raw = idx.properties == null ? null : idx.properties.get(key);
        if (raw == null || raw.isEmpty()) {
            throw new Exception(
                    "Required property '" + key + "' is absent from index "
                    + idx.table + "." + idx.name + " (uuid=" + idx.uuid + ")."
                    + " Check the CREATE VECTOR INDEX DDL for this index.");
        }
        try {
            int val = Integer.parseInt(raw);
            if (val <= 0) {
                throw new Exception(
                        "Property '" + key + "' must be positive, got " + val
                        + " for index " + idx.table + "." + idx.name
                        + " (uuid=" + idx.uuid + ").");
            }
            return val;
        } catch (NumberFormatException e) {
            throw new Exception(
                    "Property '" + key + "' is not a valid integer ('" + raw + "')"
                    + " for index " + idx.table + "." + idx.name
                    + " (uuid=" + idx.uuid + ").", e);
        }
    }

    private static float requirePositiveFloat(Index idx, String key) throws Exception {
        String raw = idx.properties == null ? null : idx.properties.get(key);
        if (raw == null || raw.isEmpty()) {
            throw new Exception(
                    "Required property '" + key + "' is absent from index "
                    + idx.table + "." + idx.name + " (uuid=" + idx.uuid + ")."
                    + " Check the CREATE VECTOR INDEX DDL for this index.");
        }
        try {
            float val = Float.parseFloat(raw);
            if (val <= 0f) {
                throw new Exception(
                        "Property '" + key + "' must be positive, got " + val
                        + " for index " + idx.table + "." + idx.name
                        + " (uuid=" + idx.uuid + ").");
            }
            return val;
        } catch (NumberFormatException e) {
            throw new Exception(
                    "Property '" + key + "' is not a valid float ('" + raw + "')"
                    + " for index " + idx.table + "." + idx.name
                    + " (uuid=" + idx.uuid + ").", e);
        }
    }

    private static String requireNonEmptyString(Index idx, String key) throws Exception {
        String val = idx.properties == null ? null : idx.properties.get(key);
        if (val == null || val.isEmpty()) {
            throw new Exception(
                    "Required property '" + key + "' is absent from index "
                    + idx.table + "." + idx.name + " (uuid=" + idx.uuid + ")."
                    + " Check the CREATE VECTOR INDEX DDL for this index.");
        }
        return val;
    }

    // -------------------------------------------------------------------------
    // Remote-file metadata read helpers
    // (binary format mirrors SharedCheckpointMetadataManager)
    // -------------------------------------------------------------------------

    /**
     * Reads the latest committed checkpoint LSN from
     * {@code {tablespaceUuid}/_metadata/latest.checkpoint}.
     * Returns {@link LogSequenceNumber#START_OF_TIME} when the file is absent
     * (no checkpoint yet).
     */
    LogSequenceNumber readLatestCheckpointLsn() throws Exception {
        String path = tablespaceUuid + "/_metadata/latest.checkpoint";
        byte[] data;
        try {
            data = fileClient.readFile(path);
        } catch (RuntimeException e) {
            // RemoteFileClient.readFile() wraps I/O failures in RuntimeException.
            throw new Exception("Failed to read latest checkpoint LSN from "
                    + path + ": " + e.getMessage(), e);
        }
        if (data == null) {
            return LogSequenceNumber.START_OF_TIME;
        }
        try (BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(data));
             ExtendedDataInputStream din = new ExtendedDataInputStream(bis)) {
            long version = din.readVLong();
            long flags = din.readVLong();
            if (version != 1 || flags != 0) {
                throw new Exception("Corrupted latest.checkpoint file at " + path
                        + " (version=" + version + ", flags=" + flags + ")");
            }
            din.readUTF(); // tablespace name (informational)
            long ledgerId = din.readZLong();
            long offset = din.readZLong();
            return new LogSequenceNumber(ledgerId, offset);
        } catch (IOException e) {
            throw new Exception("Failed to parse latest.checkpoint from " + path, e);
        }
    }

    /**
     * Reads the list of {@link Index} definitions from
     * {@code {tablespaceUuid}/_metadata/indexes.{lsn}.tablesmetadata}.
     */
    List<Index> readIndexDefinitions(LogSequenceNumber lsn) throws Exception {
        String path = tablespaceUuid + "/_metadata/indexes."
                + lsn.ledgerId + "." + lsn.offset + ".tablesmetadata";
        byte[] data;
        try {
            data = fileClient.readFile(path);
        } catch (RuntimeException e) {
            throw new Exception("Failed to read index definitions from "
                    + path + ": " + e.getMessage(), e);
        }
        if (data == null) {
            if (lsn.isStartOfTime()) {
                return new ArrayList<>();
            }
            throw new Exception("Index definitions file not found at " + path
                    + ". The remote file server may be missing data for checkpoint LSN " + lsn + ".");
        }
        try (BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(data));
             ExtendedDataInputStream din = new ExtendedDataInputStream(bis)) {
            long version = din.readVLong();
            long flags = din.readVLong();
            if (version != 1 || flags != 0) {
                throw new Exception("Corrupted index definitions file at " + path
                        + " (version=" + version + ", flags=" + flags + ")");
            }
            din.readUTF(); // tablespace name (informational)
            din.readZLong(); // ledgerId (informational)
            din.readZLong(); // offset (informational)
            int count = din.readInt();
            List<Index> result = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                byte[] indexData = din.readArray();
                result.add(Index.deserialize(indexData));
            }
            return result;
        } catch (IOException e) {
            throw new Exception("Failed to parse index definitions from " + path, e);
        }
    }

    private static String summarizeIndexes(List<Index> indexes) {
        if (indexes.isEmpty()) {
            return "(none)";
        }
        StringBuilder sb = new StringBuilder();
        for (Index idx : indexes) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(idx.table).append('.').append(idx.name).append('(').append(idx.uuid).append(')');
        }
        return sb.toString();
    }
}
