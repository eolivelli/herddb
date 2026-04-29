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
package herddb.indexing.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import herddb.log.LogSequenceNumber;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Metadata for a segmented-v2 vector index segment. One {@code SegmentMetadata}
 * is stored as the data of a znode at
 * {@code {basePath}/index-segments/{tablespaceUUID}/{indexUUID}/{segmentUUID}}.
 *
 * <p>Sentinels:
 * <ul>
 *   <li>{@code ownerInstanceId} / {@code pendingOwnerInstanceId} use
 *       {@link #NO_INSTANCE} (-1) when not set.</li>
 *   <li>LSN fields use {@link #NO_LSN_LEDGER_ID}/{@link #NO_LSN_OFFSET} (-1, -1)
 *       when no LSN is recorded yet.</li>
 *   <li>{@code retentionUntilEpochMillis} is {@link #NO_RETENTION} (-1) until the
 *       segment is moved to DEPRECATED.</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class SegmentMetadata {

    public static final int NO_INSTANCE = -1;
    public static final long NO_LSN_LEDGER_ID = -1L;
    public static final long NO_LSN_OFFSET = -1L;
    public static final long NO_RETENTION = -1L;
    public static final int NO_SEGMENT_ID = -1;

    /**
     * Current znode JSON schema version (review item D5). Bumped when a
     * non-backward-compatible field rename or structural change lands.
     * Adding a new field does NOT require bumping the version (the
     * {@code @JsonIgnoreProperties(ignoreUnknown = true)} reader handles
     * forward-compatible additions). Bumping IS required when:
     * <ul>
     *   <li>renaming a field in a way the reader can't auto-map;</li>
     *   <li>changing a field's semantics (e.g. units);</li>
     *   <li>removing a required field.</li>
     * </ul>
     * The deserializer rejects payloads with a {@code schemaVersion} the
     * runtime doesn't know.
     */
    public static final int SCHEMA_VERSION = 1;

    /**
     * Highest schema version this code can read. Increase together with
     * {@link #SCHEMA_VERSION} only when forward-compat reading of a future
     * version is supported.
     */
    public static final int MAX_SUPPORTED_SCHEMA_VERSION = 1;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Wire schema version of THIS instance. Always serialized; absent in the
     * JSON means the writer pre-dates the field (the deserializer maps that to
     * {@link #SCHEMA_VERSION} = 1 via the {@code @JsonProperty} default).
     */
    private final int schemaVersion;
    private final String segmentUuid;
    private final String tablespaceUuid;
    private final String tableName;
    private final String indexUuid;
    private final String indexName;
    private final SegmentState state;
    private final int ownerInstanceId;
    private final int pendingOwnerInstanceId;
    /**
     * Original IS-local int segment-id under which the multipart graph/map files
     * were written. Needed by the optimizer's reaper to call
     * {@code DataStorageManager.deleteMultipartIndexFile(tableSpace,
     * indexUuid + "_seg" + segmentId, "graph"/"map")} (review item A8).
     * Defaults to {@link #NO_SEGMENT_ID} (-1) when unknown — in that case the
     * reaper skips file deletion and only removes the znode.
     */
    private final int segmentId;
    private final String graphPath;
    private final String mapPath;
    private final String tombstonePath;
    private final long tombstoneLsnLedgerId;
    private final long tombstoneLsnOffset;
    private final long baseLsnLedgerId;
    private final long baseLsnOffset;
    private final long sizeBytes;
    private final long vectorCount;
    private final long generation;
    private final List<String> replacedBy;
    private final long retentionUntilEpochMillis;
    private final long createdAtEpochMillis;

    @JsonCreator
    public SegmentMetadata(
            @JsonProperty("schemaVersion") Integer schemaVersionOrNull,
            @JsonProperty("segmentUuid") String segmentUuid,
            @JsonProperty("tablespaceUuid") String tablespaceUuid,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("indexUuid") String indexUuid,
            @JsonProperty("indexName") String indexName,
            @JsonProperty("state") SegmentState state,
            @JsonProperty("ownerInstanceId") int ownerInstanceId,
            @JsonProperty("pendingOwnerInstanceId") int pendingOwnerInstanceId,
            @JsonProperty("segmentId") int segmentId,
            @JsonProperty("graphPath") String graphPath,
            @JsonProperty("mapPath") String mapPath,
            @JsonProperty("tombstonePath") String tombstonePath,
            @JsonProperty("tombstoneLsnLedgerId") long tombstoneLsnLedgerId,
            @JsonProperty("tombstoneLsnOffset") long tombstoneLsnOffset,
            @JsonProperty("baseLsnLedgerId") long baseLsnLedgerId,
            @JsonProperty("baseLsnOffset") long baseLsnOffset,
            @JsonProperty("sizeBytes") long sizeBytes,
            @JsonProperty("vectorCount") long vectorCount,
            @JsonProperty("generation") long generation,
            @JsonProperty("replacedBy") List<String> replacedBy,
            @JsonProperty("retentionUntilEpochMillis") long retentionUntilEpochMillis,
            @JsonProperty("createdAtEpochMillis") long createdAtEpochMillis) {
        // Treat absent or zero schemaVersion as v1 so payloads written before the
        // field existed deserialize cleanly (review item D5).
        this.schemaVersion = (schemaVersionOrNull == null || schemaVersionOrNull == 0)
                ? 1 : schemaVersionOrNull;
        this.segmentUuid = Objects.requireNonNull(segmentUuid, "segmentUuid");
        this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.indexUuid = Objects.requireNonNull(indexUuid, "indexUuid");
        this.indexName = Objects.requireNonNull(indexName, "indexName");
        this.state = Objects.requireNonNull(state, "state");
        this.ownerInstanceId = ownerInstanceId;
        this.pendingOwnerInstanceId = pendingOwnerInstanceId;
        this.segmentId = segmentId;
        this.graphPath = graphPath;
        this.mapPath = mapPath;
        this.tombstonePath = tombstonePath;
        this.tombstoneLsnLedgerId = tombstoneLsnLedgerId;
        this.tombstoneLsnOffset = tombstoneLsnOffset;
        this.baseLsnLedgerId = baseLsnLedgerId;
        this.baseLsnOffset = baseLsnOffset;
        this.sizeBytes = sizeBytes;
        this.vectorCount = vectorCount;
        this.generation = generation;
        this.replacedBy = replacedBy == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new java.util.ArrayList<>(replacedBy));
        this.retentionUntilEpochMillis = retentionUntilEpochMillis;
        this.createdAtEpochMillis = createdAtEpochMillis;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public String getSegmentUuid() {
        return segmentUuid;
    }

    public String getTablespaceUuid() {
        return tablespaceUuid;
    }

    public String getTableName() {
        return tableName;
    }

    public String getIndexUuid() {
        return indexUuid;
    }

    public String getIndexName() {
        return indexName;
    }

    public SegmentState getState() {
        return state;
    }

    public int getOwnerInstanceId() {
        return ownerInstanceId;
    }

    public int getPendingOwnerInstanceId() {
        return pendingOwnerInstanceId;
    }

    public int getSegmentId() {
        return segmentId;
    }

    public String getGraphPath() {
        return graphPath;
    }

    public String getMapPath() {
        return mapPath;
    }

    public String getTombstonePath() {
        return tombstonePath;
    }

    public long getTombstoneLsnLedgerId() {
        return tombstoneLsnLedgerId;
    }

    public long getTombstoneLsnOffset() {
        return tombstoneLsnOffset;
    }

    public long getBaseLsnLedgerId() {
        return baseLsnLedgerId;
    }

    public long getBaseLsnOffset() {
        return baseLsnOffset;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public long getVectorCount() {
        return vectorCount;
    }

    public long getGeneration() {
        return generation;
    }

    public List<String> getReplacedBy() {
        return replacedBy;
    }

    public long getRetentionUntilEpochMillis() {
        return retentionUntilEpochMillis;
    }

    public long getCreatedAtEpochMillis() {
        return createdAtEpochMillis;
    }

    public LogSequenceNumber tombstoneLsn() {
        if (tombstoneLsnLedgerId == NO_LSN_LEDGER_ID) {
            return null;
        }
        return new LogSequenceNumber(tombstoneLsnLedgerId, tombstoneLsnOffset);
    }

    public LogSequenceNumber baseLsn() {
        if (baseLsnLedgerId == NO_LSN_LEDGER_ID) {
            return null;
        }
        return new LogSequenceNumber(baseLsnLedgerId, baseLsnOffset);
    }

    public byte[] serialize() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize segment metadata", e);
        }
    }

    public static SegmentMetadata deserialize(byte[] data) throws IOException {
        SegmentMetadata m = MAPPER.readValue(data, SegmentMetadata.class);
        // Reject payloads emitted by a future, non-backward-compatible writer
        // (review item D5). Older writers either omit the schemaVersion field
        // entirely (mapped to 1) or write 1 explicitly — both are accepted.
        if (m.schemaVersion > MAX_SUPPORTED_SCHEMA_VERSION) {
            throw new IOException("unsupported segment metadata schemaVersion: " + m.schemaVersion
                    + " (max supported: " + MAX_SUPPORTED_SCHEMA_VERSION + ")");
        }
        return m;
    }

    public Builder toBuilder() {
        return new Builder()
                .segmentUuid(segmentUuid)
                .tablespaceUuid(tablespaceUuid)
                .tableName(tableName)
                .indexUuid(indexUuid)
                .indexName(indexName)
                .state(state)
                .ownerInstanceId(ownerInstanceId)
                .pendingOwnerInstanceId(pendingOwnerInstanceId)
                .segmentId(segmentId)
                .graphPath(graphPath)
                .mapPath(mapPath)
                .tombstonePath(tombstonePath)
                .tombstoneLsn(tombstoneLsnLedgerId, tombstoneLsnOffset)
                .baseLsn(baseLsnLedgerId, baseLsnOffset)
                .sizeBytes(sizeBytes)
                .vectorCount(vectorCount)
                .generation(generation)
                .replacedBy(replacedBy)
                .retentionUntilEpochMillis(retentionUntilEpochMillis)
                .createdAtEpochMillis(createdAtEpochMillis);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SegmentMetadata)) {
            return false;
        }
        SegmentMetadata that = (SegmentMetadata) o;
        return ownerInstanceId == that.ownerInstanceId
                && pendingOwnerInstanceId == that.pendingOwnerInstanceId
                && tombstoneLsnLedgerId == that.tombstoneLsnLedgerId
                && tombstoneLsnOffset == that.tombstoneLsnOffset
                && baseLsnLedgerId == that.baseLsnLedgerId
                && baseLsnOffset == that.baseLsnOffset
                && sizeBytes == that.sizeBytes
                && vectorCount == that.vectorCount
                && generation == that.generation
                && retentionUntilEpochMillis == that.retentionUntilEpochMillis
                && createdAtEpochMillis == that.createdAtEpochMillis
                && Objects.equals(segmentUuid, that.segmentUuid)
                && Objects.equals(tablespaceUuid, that.tablespaceUuid)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(indexUuid, that.indexUuid)
                && Objects.equals(indexName, that.indexName)
                && state == that.state
                && Objects.equals(graphPath, that.graphPath)
                && Objects.equals(mapPath, that.mapPath)
                && Objects.equals(tombstonePath, that.tombstonePath)
                && Objects.equals(replacedBy, that.replacedBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentUuid, tablespaceUuid, indexUuid, state,
                ownerInstanceId, pendingOwnerInstanceId, generation, sizeBytes);
    }

    @Override
    public String toString() {
        return "SegmentMetadata{"
                + "segmentUuid='" + segmentUuid + '\''
                + ", tablespaceUuid='" + tablespaceUuid + '\''
                + ", indexUuid='" + indexUuid + '\''
                + ", state=" + state
                + ", owner=" + ownerInstanceId
                + ", pendingOwner=" + pendingOwnerInstanceId
                + ", generation=" + generation
                + ", sizeBytes=" + sizeBytes
                + ", vectorCount=" + vectorCount
                + ", baseLsn=" + baseLsnLedgerId + ":" + baseLsnOffset
                + ", tombstoneLsn=" + tombstoneLsnLedgerId + ":" + tombstoneLsnOffset
                + '}';
    }

    /**
     * Mutable builder for {@link SegmentMetadata}.
     */
    public static final class Builder {

        private String segmentUuid;
        private String tablespaceUuid;
        private String tableName;
        private String indexUuid;
        private String indexName;
        private SegmentState state = SegmentState.ACTIVE;
        private int ownerInstanceId = NO_INSTANCE;
        private int pendingOwnerInstanceId = NO_INSTANCE;
        private int segmentId = NO_SEGMENT_ID;
        private String graphPath;
        private String mapPath;
        private String tombstonePath;
        private long tombstoneLsnLedgerId = NO_LSN_LEDGER_ID;
        private long tombstoneLsnOffset = NO_LSN_OFFSET;
        private long baseLsnLedgerId = NO_LSN_LEDGER_ID;
        private long baseLsnOffset = NO_LSN_OFFSET;
        private long sizeBytes;
        private long vectorCount;
        private long generation;
        private List<String> replacedBy = Collections.emptyList();
        private long retentionUntilEpochMillis = NO_RETENTION;
        private long createdAtEpochMillis;

        public Builder segmentUuid(String value) {
            this.segmentUuid = value;
            return this;
        }

        public Builder tablespaceUuid(String value) {
            this.tablespaceUuid = value;
            return this;
        }

        public Builder tableName(String value) {
            this.tableName = value;
            return this;
        }

        public Builder indexUuid(String value) {
            this.indexUuid = value;
            return this;
        }

        public Builder indexName(String value) {
            this.indexName = value;
            return this;
        }

        public Builder state(SegmentState value) {
            this.state = value;
            return this;
        }

        public Builder ownerInstanceId(int value) {
            this.ownerInstanceId = value;
            return this;
        }

        public Builder pendingOwnerInstanceId(int value) {
            this.pendingOwnerInstanceId = value;
            return this;
        }

        public Builder segmentId(int value) {
            this.segmentId = value;
            return this;
        }

        public Builder graphPath(String value) {
            this.graphPath = value;
            return this;
        }

        public Builder mapPath(String value) {
            this.mapPath = value;
            return this;
        }

        public Builder tombstonePath(String value) {
            this.tombstonePath = value;
            return this;
        }

        public Builder tombstoneLsn(long ledgerId, long offset) {
            this.tombstoneLsnLedgerId = ledgerId;
            this.tombstoneLsnOffset = offset;
            return this;
        }

        public Builder tombstoneLsn(LogSequenceNumber lsn) {
            if (lsn == null) {
                this.tombstoneLsnLedgerId = NO_LSN_LEDGER_ID;
                this.tombstoneLsnOffset = NO_LSN_OFFSET;
            } else {
                this.tombstoneLsnLedgerId = lsn.ledgerId;
                this.tombstoneLsnOffset = lsn.offset;
            }
            return this;
        }

        public Builder baseLsn(long ledgerId, long offset) {
            this.baseLsnLedgerId = ledgerId;
            this.baseLsnOffset = offset;
            return this;
        }

        public Builder baseLsn(LogSequenceNumber lsn) {
            if (lsn == null) {
                this.baseLsnLedgerId = NO_LSN_LEDGER_ID;
                this.baseLsnOffset = NO_LSN_OFFSET;
            } else {
                this.baseLsnLedgerId = lsn.ledgerId;
                this.baseLsnOffset = lsn.offset;
            }
            return this;
        }

        public Builder sizeBytes(long value) {
            this.sizeBytes = value;
            return this;
        }

        public Builder vectorCount(long value) {
            this.vectorCount = value;
            return this;
        }

        public Builder generation(long value) {
            this.generation = value;
            return this;
        }

        public Builder replacedBy(List<String> value) {
            this.replacedBy = value == null ? Collections.emptyList() : value;
            return this;
        }

        public Builder retentionUntilEpochMillis(long value) {
            this.retentionUntilEpochMillis = value;
            return this;
        }

        public Builder createdAtEpochMillis(long value) {
            this.createdAtEpochMillis = value;
            return this;
        }

        public SegmentMetadata build() {
            // The builder always stamps the current code's SCHEMA_VERSION; callers
            // never manipulate it directly. Loading from an older payload that lacks
            // the field still produces a SegmentMetadata with schemaVersion=1 because
            // the @JsonCreator default kicks in (review item D5).
            return new SegmentMetadata(SCHEMA_VERSION,
                    segmentUuid, tablespaceUuid, tableName, indexUuid, indexName,
                    state, ownerInstanceId, pendingOwnerInstanceId, segmentId, graphPath, mapPath,
                    tombstonePath, tombstoneLsnLedgerId, tombstoneLsnOffset,
                    baseLsnLedgerId, baseLsnOffset, sizeBytes, vectorCount, generation,
                    replacedBy, retentionUntilEpochMillis, createdAtEpochMillis);
        }
    }
}
