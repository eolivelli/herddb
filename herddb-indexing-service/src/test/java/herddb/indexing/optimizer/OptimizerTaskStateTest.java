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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.EnumSet;
import java.util.Set;
import org.junit.Test;

/**
 * Pins the two predicate helpers on {@link OptimizerTaskState}: a future
 * refactor that changes {@code isTerminal()} or {@code blocksInputs()} fails
 * these assertions and is forced to consider the producer / consumer logic
 * that depends on them.
 */
public class OptimizerTaskStateTest {

    @Test
    public void isTerminalCoversExactlyDoneFailedPoison() {
        Set<OptimizerTaskState> terminal = EnumSet.noneOf(OptimizerTaskState.class);
        for (OptimizerTaskState s : OptimizerTaskState.values()) {
            if (s.isTerminal()) {
                terminal.add(s);
            }
        }
        assertEquals(EnumSet.of(OptimizerTaskState.DONE, OptimizerTaskState.FAILED, OptimizerTaskState.POISON),
                terminal);
    }

    @Test
    public void blocksInputsCoversExactlyPendingClaimedAndAwaitingAck() {
        Set<OptimizerTaskState> blockers = EnumSet.noneOf(OptimizerTaskState.class);
        for (OptimizerTaskState s : OptimizerTaskState.values()) {
            if (s.blocksInputs()) {
                blockers.add(s);
            }
        }
        // Issue #555: AWAITING_ACK is a non-terminal state in which the inputs
        // are still ACTIVE in ZK and the swap multi-op has not fired yet — a
        // parallel producer must not assign those inputs to a different merge.
        assertEquals(EnumSet.of(OptimizerTaskState.PENDING, OptimizerTaskState.CLAIMED,
                        OptimizerTaskState.AWAITING_ACK),
                blockers);
    }

    @Test
    public void awaitingAckIsNotTerminalButBlocksInputs() {
        assertFalse(OptimizerTaskState.AWAITING_ACK.isTerminal());
        assertTrue(OptimizerTaskState.AWAITING_ACK.blocksInputs());
    }

    @Test
    public void poisonIsTerminalButDoesNotBlockInputs() {
        assertTrue(OptimizerTaskState.POISON.isTerminal());
        assertFalse(OptimizerTaskState.POISON.blocksInputs());
    }
}
