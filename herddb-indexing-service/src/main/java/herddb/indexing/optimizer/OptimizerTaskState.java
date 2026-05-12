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
 * {@link #DONE} on success or {@link #FAILED} on error. Tasks that exceed the
 * configured retry budget land in {@link #POISON} and require operator
 * intervention.
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
    DONE,
    FAILED,
    POISON;

    public boolean isTerminal() {
        return this == DONE || this == FAILED || this == POISON;
    }

    public boolean blocksInputs() {
        return this == PENDING || this == CLAIMED;
    }
}
