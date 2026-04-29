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

package herddb.core;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.BeginTransactionStatement;
import org.junit.Test;

/**
 * Verifies that {@link TableSpaceManager#processAbandonedTransactions()} does
 * not prematurely roll back transactions that were merely blocked by a long
 * Phase C checkpoint (issue #342: file-server restart stalling Phase C and
 * causing the activator to abandon still-live VectorBench transactions).
 *
 * <p>The guard logic shifts the abandonment cutoff back to
 * {@code (checkpointStart - timeout)} for the first {@code timeout} ms after
 * a checkpoint finishes. Once that cooling period expires the normal timeout
 * resumes, so genuinely idle transactions are still cleaned up.
 */
public class AbandonedTransactionCheckpointProtectionTest extends BaseTestcase {

    /**
     * Scenario: a checkpoint ran for longer than the abandoned-transaction
     * timeout. The moment the checkpoint finishes, {@code processAbandonedTransactions}
     * must NOT roll back transactions whose last activity fell inside the
     * checkpoint window. After the cooling period expires the transaction IS
     * eventually cleaned up.
     */
    @Test
    public void testTransactionProtectedDuringCheckpointWindow() throws Exception {
        // Short timeout so the test runs quickly.
        final long timeoutMs = 300;
        manager.setAbandonedTransactionsTimeout(timeoutMs);

        // Open a transaction and do not commit it (simulates a VectorBench implicit tx).
        long tx = ((TransactionResult) manager.executeStatement(
                new BeginTransactionStatement(tableSpace),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION)).getTransactionId();

        TableSpaceManager tsm = manager.getTableSpaceManager(tableSpace);

        // Wait long enough for the transaction to be "old" (> timeout).
        Thread.sleep(timeoutMs + 100);

        // Simulate: a checkpoint started just before the transaction was last
        // active and ended just now. The cooling window is still open.
        long now = System.currentTimeMillis();
        // Checkpoint started (timeoutMs + 200ms) ago — before the transaction
        // was last touched — and finished 50 ms ago (still within the window).
        tsm.setLastCheckpointStartTs(now - timeoutMs - 200);
        tsm.setLastCheckpointTimestamp(now - 50);

        // The transaction should be PROTECTED: last activity falls inside the
        // [cpStart - timeout, now] window so the cutoff is pushed back.
        tsm.processAbandonedTransactions();
        assertFalse(
                "Transaction should not be abandoned while checkpoint cooling window is open",
                tsm.getTransactions().isEmpty());

        // Now advance time: simulate the cooling period expiring by setting
        // checkpoint end to (timeout + 1 ms) ago.
        now = System.currentTimeMillis();
        tsm.setLastCheckpointTimestamp(now - timeoutMs - 1);
        // checkpointStartTs stays the same — it's now old enough that
        // cpAwareCutoff > abandonedTransactionTimeout no longer holds.

        // The transaction is now OLD ENOUGH to be abandoned normally.
        // Wait one more timeout period to ensure the transaction truly crosses
        // the normal cutoff regardless of small clock skew.
        Thread.sleep(timeoutMs + 100);

        tsm.processAbandonedTransactions();
        assertTrue(
                "Transaction should be abandoned once the cooling period expires",
                tsm.getTransactions().isEmpty());
    }

    /**
     * Sanity: without any checkpoint history, the normal timeout applies and
     * transactions are abandoned as usual.
     */
    @Test
    public void testNormalAbandonmentWithoutCheckpoint() throws Exception {
        final long timeoutMs = 300;
        manager.setAbandonedTransactionsTimeout(timeoutMs);

        long tx = ((TransactionResult) manager.executeStatement(
                new BeginTransactionStatement(tableSpace),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION)).getTransactionId();

        TableSpaceManager tsm = manager.getTableSpaceManager(tableSpace);

        // No checkpoint has run (lastCheckpointStartTs = 0, lastCheckpointTimestamp = 0).
        // Normal abandonment must still work.
        tsm.processAbandonedTransactions();
        assertFalse(
                "Transaction not yet timed-out, should not be abandoned",
                tsm.getTransactions().isEmpty());

        Thread.sleep(timeoutMs + 100);

        tsm.processAbandonedTransactions();
        assertTrue(
                "Transaction should be abandoned after timeout with no checkpoint protection",
                tsm.getTransactions().isEmpty());
    }
}
