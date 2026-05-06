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

package herddb.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import herddb.log.LogSequenceNumber;
import java.io.IOException;
import java.util.Objects;

/**
 * Published checkpoint state for a given primary indexing-service instanceId.
 * Written at {@code /herddb/indexingServices/state/{instanceId}} after each
 * successful checkpoint so shadow replicas can observe progress and reload
 * their on-disk snapshots.
 *
 * <p>Carries two timestamps with very different semantics:
 * <ul>
 *   <li>{@link #getTimestampMillis()} — wall-clock when the primary
 *       <em>published</em> the checkpoint state. Useful as a heartbeat ("is
 *       the primary still alive?").</li>
 *   <li>{@link #getLastEntryTimestampMillis()} (issue #423) — wall-clock
 *       timestamp of the {@link herddb.log.LogEntry} at
 *       {@code (ledgerId, offset)}. The freshness of the data the shadow
 *       reloaded from disk: {@code now - lastEntryTimestampMillis} is the
 *       time-lag between real-world writes and what the shadow can serve.
 *       {@code 0} means "unknown" (no checkpoint yet).</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class IndexingServiceCheckpointState {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final int instanceId;
    private final long ledgerId;
    private final long offset;
    private final int segmentCount;
    private final long timestampMillis;
    private final long lastEntryTimestampMillis;

    @JsonCreator
    public IndexingServiceCheckpointState(
            @JsonProperty("instanceId") int instanceId,
            @JsonProperty("ledgerId") long ledgerId,
            @JsonProperty("offset") long offset,
            @JsonProperty("segmentCount") int segmentCount,
            @JsonProperty("timestampMillis") long timestampMillis,
            @JsonProperty("lastEntryTimestampMillis") long lastEntryTimestampMillis) {
        this.instanceId = instanceId;
        this.ledgerId = ledgerId;
        this.offset = offset;
        this.segmentCount = segmentCount;
        this.timestampMillis = timestampMillis;
        this.lastEntryTimestampMillis = lastEntryTimestampMillis;
    }

    public int getInstanceId() {
        return instanceId;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getOffset() {
        return offset;
    }

    public int getSegmentCount() {
        return segmentCount;
    }

    public long getTimestampMillis() {
        return timestampMillis;
    }

    /**
     * Wall-clock timestamp (epoch ms) of the {@link herddb.log.LogEntry} at
     * {@code (ledgerId, offset)} — i.e. the freshness of the data the shadow
     * reloaded from disk after this checkpoint. {@code 0} means "unknown".
     * Issue #423.
     */
    public long getLastEntryTimestampMillis() {
        return lastEntryTimestampMillis;
    }

    @JsonIgnore
    public LogSequenceNumber toLogSequenceNumber() {
        return new LogSequenceNumber(ledgerId, offset);
    }

    public byte[] serialize() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize checkpoint state", e);
        }
    }

    public static IndexingServiceCheckpointState deserialize(byte[] data) throws IOException {
        return MAPPER.readValue(data, IndexingServiceCheckpointState.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexingServiceCheckpointState)) {
            return false;
        }
        IndexingServiceCheckpointState that = (IndexingServiceCheckpointState) o;
        return instanceId == that.instanceId
                && ledgerId == that.ledgerId
                && offset == that.offset
                && segmentCount == that.segmentCount
                && timestampMillis == that.timestampMillis
                && lastEntryTimestampMillis == that.lastEntryTimestampMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, ledgerId, offset, segmentCount,
                timestampMillis, lastEntryTimestampMillis);
    }

    @Override
    public String toString() {
        return "IndexingServiceCheckpointState{"
                + "instanceId=" + instanceId
                + ", ledgerId=" + ledgerId
                + ", offset=" + offset
                + ", segmentCount=" + segmentCount
                + ", timestampMillis=" + timestampMillis
                + ", lastEntryTimestampMillis=" + lastEntryTimestampMillis
                + '}';
    }
}
