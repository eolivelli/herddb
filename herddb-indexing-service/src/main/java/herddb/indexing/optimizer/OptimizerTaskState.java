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

/**
 * State machine for a merge task in the ZK-backed task queue. Producers
 * (the leader) create tasks in {@link #PENDING}; consumers (any pod) flip them
 * to {@link #CLAIMED} once they hold the ephemeral lease, then to
 * {@link #AWAITING_ACK} once the merged output has been staged in ZK as
 * {@code PROVISIONAL} (issue #555). Tasks in {@code AWAITING_ACK} wait for
 * every interested IS pod (the primary owning {@code targetOwnerInstanceId}
 * plus every shadow replica of that primary) to write an ephemeral
 * acknowledgement node; once that happens the consumer performs the atomic
 * swap (output {@code PROVISIONAL → ACTIVE} + every input {@code ACTIVE →
 * DEPRECATED}) via {@code ZooKeeper.multi(...)} and transitions the task to
 * {@link #DONE}. On ack timeout, repeated CAS exhaustion, or merger failure
 * the task transitions to {@link #FAILED}; tasks that exceed the configured
 * retry budget land in {@link #POISON} and require operator intervention.
 *
 * <p>Two helpers express the invariants used elsewhere:
 * <ul>
 *   <li>{@link #isTerminal()}: producer skips task GC scans on non-terminal
 *       tasks; orphan scanner only resets non-terminal states.</li>
 *   <li>{@link #blocksInputs()}: producer excludes input segment UUIDs
 *       referenced by tasks in these states when picking new candidates so
 *       no two concurrent tasks target the same input. {@link #POISON} does
 *       NOT block — its inputs may be re-picked by a future leader tick.</li>
 * </ul>
 */
public enum OptimizerTaskState {
    PENDING,
    CLAIMED,
    /**
     * Issue #555. The consumer has uploaded the merged output to remote
     * storage AND created its znode in ZK with state {@code PROVISIONAL}.
     * The task now waits for every interested IS pod to write an ephemeral
     * acknowledgement node under
     * {@code {basePath}/index-segments-acks/{outputSegmentUuid}/{serviceId}}.
     * When all expected acks land, any optimizer pod can perform the atomic
     * swap (output {@code PROVISIONAL → ACTIVE} + every input
     * {@code ACTIVE → DEPRECATED}) via {@code ZooKeeper.multi(...)} and
     * transition the task to {@code DONE}. On ack timeout or repeated CAS
     * exhaustion the task is aborted: the output's multipart files + znode
     * + acks tree are deleted, inputs remain {@code ACTIVE}, and the task
     * transitions {@code FAILED → POISON} per {@code maxAttempts}.
     * The lease is released on the {@code CLAIMED → AWAITING_ACK} transition
     * so any optimizer pod may resume the wait — the swap operation is
     * idempotent (CAS-ordered).
     */
    AWAITING_ACK,
    DONE,
    FAILED,
    POISON;

    public boolean isTerminal() {
        return this == DONE || this == FAILED || this == POISON;
    }

    public boolean blocksInputs() {
        // AWAITING_ACK blocks the inputs because the swap is still pending —
        // a parallel producer must not assign the same inputs to a different
        // merge while we are waiting for acknowledgements.
        return this == PENDING || this == CLAIMED || this == AWAITING_ACK;
    }
}
