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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable description of a merge task in the optimizer's ZK-backed task
 * queue. Mirrors the structure of {@link herddb.indexing.segment.SegmentMetadata}:
 * Jackson-serialized JSON, builder + {@link #toBuilder()} pattern,
 * {@link #SCHEMA_VERSION} field with forward-compat read of unknown properties.
 *
 * <p>One task = one persistent znode under
 * {@code <basePath>/index-optimizer/tasks/<tablespaceUuid>/<taskId>}.
 * An ephemeral child {@code lease} carries the worker UUID currently
 * executing it; absence of the lease signals the worker is gone and the
 * orphan scanner may reset / poison the task.
 *
 * <p>Step 3 introduces the type; producer + consumer wiring lands in steps 5
 * and 6.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class OptimizerTask {

    public static final int SCHEMA_VERSION = 1;
    public static final int MAX_SUPPORTED_SCHEMA_VERSION = 1;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final int schemaVersion;
    private final String taskId;
    private final String tablespaceUuid;
    private final String indexUuid;
    private final List<String> inputSegmentUuids;
    private final Map<String, Integer> inputSegmentExpectedVersions;
    private final int targetOwnerInstanceId;
    private final OptimizerTaskState state;
    private final int attempts;
    private final LastError lastError;
    private final WorkerClaim claim;
    private final String outputSegmentUuid;
    private final long createdAtEpochMillis;
    private final long leaderEpoch;

    @JsonCreator
    public OptimizerTask(
            @JsonProperty("schemaVersion") Integer schemaVersionOrNull,
            @JsonProperty("taskId") String taskId,
            @JsonProperty("tablespaceUuid") String tablespaceUuid,
            @JsonProperty("indexUuid") String indexUuid,
            @JsonProperty("inputSegmentUuids") List<String> inputSegmentUuids,
            @JsonProperty("inputSegmentExpectedVersions") Map<String, Integer> inputSegmentExpectedVersions,
            @JsonProperty("targetOwnerInstanceId") int targetOwnerInstanceId,
            @JsonProperty("state") OptimizerTaskState state,
            @JsonProperty("attempts") int attempts,
            @JsonProperty("lastError") LastError lastError,
            @JsonProperty("claim") WorkerClaim claim,
            @JsonProperty("outputSegmentUuid") String outputSegmentUuid,
            @JsonProperty("createdAtEpochMillis") long createdAtEpochMillis,
            @JsonProperty("leaderEpoch") long leaderEpoch) {
        this.schemaVersion = schemaVersionOrNull == null ? SCHEMA_VERSION : schemaVersionOrNull;
        this.taskId = Objects.requireNonNull(taskId, "taskId");
        this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.indexUuid = Objects.requireNonNull(indexUuid, "indexUuid");
        this.inputSegmentUuids = inputSegmentUuids == null
                ? Collections.emptyList()
                : List.copyOf(inputSegmentUuids);
        this.inputSegmentExpectedVersions = inputSegmentExpectedVersions == null
                ? Collections.emptyMap()
                : Map.copyOf(inputSegmentExpectedVersions);
        this.targetOwnerInstanceId = targetOwnerInstanceId;
        this.state = Objects.requireNonNull(state, "state");
        this.attempts = attempts;
        this.lastError = lastError;
        this.claim = claim;
        this.outputSegmentUuid = outputSegmentUuid;
        this.createdAtEpochMillis = createdAtEpochMillis;
        this.leaderEpoch = leaderEpoch;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getTablespaceUuid() {
        return tablespaceUuid;
    }

    public String getIndexUuid() {
        return indexUuid;
    }

    public List<String> getInputSegmentUuids() {
        return inputSegmentUuids;
    }

    public Map<String, Integer> getInputSegmentExpectedVersions() {
        return inputSegmentExpectedVersions;
    }

    public int getTargetOwnerInstanceId() {
        return targetOwnerInstanceId;
    }

    public OptimizerTaskState getState() {
        return state;
    }

    public int getAttempts() {
        return attempts;
    }

    public LastError getLastError() {
        return lastError;
    }

    public WorkerClaim getClaim() {
        return claim;
    }

    public String getOutputSegmentUuid() {
        return outputSegmentUuid;
    }

    public long getCreatedAtEpochMillis() {
        return createdAtEpochMillis;
    }

    public long getLeaderEpoch() {
        return leaderEpoch;
    }

    public byte[] serialize() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize optimizer task", e);
        }
    }

    public static OptimizerTask deserialize(byte[] data) throws IOException {
        OptimizerTask t = MAPPER.readValue(data, OptimizerTask.class);
        if (t.schemaVersion > MAX_SUPPORTED_SCHEMA_VERSION) {
            throw new IOException("unsupported optimizer task schemaVersion: " + t.schemaVersion
                    + " (max supported: " + MAX_SUPPORTED_SCHEMA_VERSION + ")");
        }
        return t;
    }

    public Builder toBuilder() {
        return new Builder()
                .taskId(taskId)
                .tablespaceUuid(tablespaceUuid)
                .indexUuid(indexUuid)
                .inputSegmentUuids(inputSegmentUuids)
                .inputSegmentExpectedVersions(inputSegmentExpectedVersions)
                .targetOwnerInstanceId(targetOwnerInstanceId)
                .state(state)
                .attempts(attempts)
                .lastError(lastError)
                .claim(claim)
                .outputSegmentUuid(outputSegmentUuid)
                .createdAtEpochMillis(createdAtEpochMillis)
                .leaderEpoch(leaderEpoch);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OptimizerTask)) {
            return false;
        }
        OptimizerTask that = (OptimizerTask) o;
        return schemaVersion == that.schemaVersion
                && targetOwnerInstanceId == that.targetOwnerInstanceId
                && attempts == that.attempts
                && createdAtEpochMillis == that.createdAtEpochMillis
                && leaderEpoch == that.leaderEpoch
                && Objects.equals(taskId, that.taskId)
                && Objects.equals(tablespaceUuid, that.tablespaceUuid)
                && Objects.equals(indexUuid, that.indexUuid)
                && Objects.equals(inputSegmentUuids, that.inputSegmentUuids)
                && Objects.equals(inputSegmentExpectedVersions, that.inputSegmentExpectedVersions)
                && state == that.state
                && Objects.equals(lastError, that.lastError)
                && Objects.equals(claim, that.claim)
                && Objects.equals(outputSegmentUuid, that.outputSegmentUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaVersion, taskId, tablespaceUuid, indexUuid,
                inputSegmentUuids, inputSegmentExpectedVersions, targetOwnerInstanceId,
                state, attempts, lastError, claim, outputSegmentUuid,
                createdAtEpochMillis, leaderEpoch);
    }

    @Override
    public String toString() {
        return "OptimizerTask{taskId=" + taskId + ", indexUuid=" + indexUuid
                + ", state=" + state + ", inputs=" + inputSegmentUuids.size()
                + ", attempts=" + attempts + ", targetOwner=" + targetOwnerInstanceId
                + ", leaderEpoch=" + leaderEpoch + '}';
    }

    /**
     * Liveness signal recorded by the worker when it claims a task. Pairs with
     * the ephemeral lease znode under the task path.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class WorkerClaim {

        private final String workerId;
        private final long claimedAtEpochMillis;

        @JsonCreator
        public WorkerClaim(
                @JsonProperty("workerId") String workerId,
                @JsonProperty("claimedAtEpochMillis") long claimedAtEpochMillis) {
            this.workerId = Objects.requireNonNull(workerId, "workerId");
            this.claimedAtEpochMillis = claimedAtEpochMillis;
        }

        public String getWorkerId() {
            return workerId;
        }

        public long getClaimedAtEpochMillis() {
            return claimedAtEpochMillis;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof WorkerClaim)) {
                return false;
            }
            WorkerClaim that = (WorkerClaim) o;
            return claimedAtEpochMillis == that.claimedAtEpochMillis
                    && Objects.equals(workerId, that.workerId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(workerId, claimedAtEpochMillis);
        }

        @Override
        public String toString() {
            return "WorkerClaim{workerId=" + workerId + ", at=" + claimedAtEpochMillis + '}';
        }
    }

    /**
     * Recorded by the worker (or the orphan scanner) when a task fails. Survives
     * the task state transition to FAILED / POISON so operators can diagnose.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class LastError {

        private final String workerId;
        private final String message;
        private final long atEpochMillis;

        @JsonCreator
        public LastError(
                @JsonProperty("workerId") String workerId,
                @JsonProperty("message") String message,
                @JsonProperty("atEpochMillis") long atEpochMillis) {
            this.workerId = workerId;
            this.message = message;
            this.atEpochMillis = atEpochMillis;
        }

        public String getWorkerId() {
            return workerId;
        }

        public String getMessage() {
            return message;
        }

        public long getAtEpochMillis() {
            return atEpochMillis;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LastError)) {
                return false;
            }
            LastError that = (LastError) o;
            return atEpochMillis == that.atEpochMillis
                    && Objects.equals(workerId, that.workerId)
                    && Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(workerId, message, atEpochMillis);
        }

        @Override
        public String toString() {
            return "LastError{workerId=" + workerId + ", at=" + atEpochMillis
                    + ", message=" + message + '}';
        }
    }

    /** Mutable builder. */
    public static final class Builder {

        private String taskId;
        private String tablespaceUuid;
        private String indexUuid;
        private List<String> inputSegmentUuids = Collections.emptyList();
        private Map<String, Integer> inputSegmentExpectedVersions = Collections.emptyMap();
        private int targetOwnerInstanceId;
        private OptimizerTaskState state = OptimizerTaskState.PENDING;
        private int attempts;
        private LastError lastError;
        private WorkerClaim claim;
        private String outputSegmentUuid;
        private long createdAtEpochMillis;
        private long leaderEpoch;

        public Builder taskId(String value) {
            this.taskId = value;
            return this;
        }

        public Builder tablespaceUuid(String value) {
            this.tablespaceUuid = value;
            return this;
        }

        public Builder indexUuid(String value) {
            this.indexUuid = value;
            return this;
        }

        public Builder inputSegmentUuids(List<String> value) {
            this.inputSegmentUuids = value == null
                    ? Collections.emptyList()
                    : List.copyOf(value);
            return this;
        }

        public Builder inputSegmentExpectedVersions(Map<String, Integer> value) {
            this.inputSegmentExpectedVersions = value == null
                    ? Collections.emptyMap()
                    : new LinkedHashMap<>(value);
            return this;
        }

        public Builder targetOwnerInstanceId(int value) {
            this.targetOwnerInstanceId = value;
            return this;
        }

        public Builder state(OptimizerTaskState value) {
            this.state = value;
            return this;
        }

        public Builder attempts(int value) {
            this.attempts = value;
            return this;
        }

        public Builder lastError(LastError value) {
            this.lastError = value;
            return this;
        }

        public Builder claim(WorkerClaim value) {
            this.claim = value;
            return this;
        }

        public Builder outputSegmentUuid(String value) {
            this.outputSegmentUuid = value;
            return this;
        }

        public Builder createdAtEpochMillis(long value) {
            this.createdAtEpochMillis = value;
            return this;
        }

        public Builder leaderEpoch(long value) {
            this.leaderEpoch = value;
            return this;
        }

        public OptimizerTask build() {
            return new OptimizerTask(SCHEMA_VERSION, taskId, tablespaceUuid, indexUuid,
                    inputSegmentUuids, inputSegmentExpectedVersions, targetOwnerInstanceId,
                    state, attempts, lastError, claim, outputSegmentUuid,
                    createdAtEpochMillis, leaderEpoch);
        }
    }
}
