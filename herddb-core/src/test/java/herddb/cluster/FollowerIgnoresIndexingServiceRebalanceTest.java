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

package herddb.cluster;

import static herddb.core.TestUtils.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.ClusterTest;
import herddb.core.DBManager;
import herddb.core.ReplicatedLogtestcase;
import herddb.model.StatementEvaluationContext;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * In classic HerdDB cluster mode (BookKeeper + ZooKeeper), Followers tail the
 * commit log to maintain a replicated tablespace. The
 * {@link herddb.log.LogEntryType#INDEXING_SERVICE_REBALANCE} entry is
 * meaningful only to indexing-service replicas — the database itself has no
 * state to mutate from it directly. This test verifies that:
 * <ul>
 *   <li>A Follower stays healthy across a REBALANCE entry written by the
 *       Leader (the apply chain has a default no-op for unknown entry types,
 *       so the new type is silently tolerated).</li>
 *   <li>The {@code defaultIndexingNumInstances} property reaches the Follower
 *       via the ZooKeeper-based tablespace metadata propagation, so a
 *       subsequent CREATE INDEX issued after Follower promotion would stamp
 *       the new value.</li>
 *   <li>The REBALANCE entry survives a leader restart — i.e. the recovery
 *       apply path replays it without choking, and a fresh leader can issue
 *       another REBALANCE successfully.</li>
 * </ul>
 *
 * @author enrico.olivelli
 */
@Category(ClusterTest.class)
public class FollowerIgnoresIndexingServiceRebalanceTest extends ReplicatedLogtestcase {

    private static final String TS = "rb_ts";

    @Test
    public void followerStaysHealthyAndSeesTablespacePropertyUpdate() throws Exception {
        try (DBManager leader = startDBManager("node1")) {
            leader.executeStatement(new CreateTableSpaceStatement(TS,
                    new HashSet<>(Arrays.asList("node1", "node2")),
                    "node1", 1, 0, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            assertTrue(leader.waitForTablespace(TS, 30_000, true));

            try (DBManager follower = startDBManager("node2")) {
                assertTrue(follower.waitForTablespace(TS, 30_000, false));

                // Issue REBALANCE on the leader. Apart from updating ZK metadata,
                // the entry rides the BK commit log; if the apply chain on
                // either node choked, that node's tablespace would be marked
                // failed and subsequent operations would error.
                execute(leader, TS, "EXECUTE INDEXING_SERVICE_REBALANCE '" + TS + "', 4",
                        Collections.emptyList(), TransactionContext.NO_TRANSACTION);

                // Wait for ZK metadata propagation to the follower
                long deadline = System.currentTimeMillis() + 15_000;
                int observed = -1;
                while (System.currentTimeMillis() < deadline) {
                    TableSpace tsFollower = follower.getMetadataStorageManager()
                            .describeTableSpace(TS);
                    if (tsFollower != null && tsFollower.defaultIndexingNumInstances == 4) {
                        observed = 4;
                        break;
                    }
                    Thread.sleep(100);
                }
                assertEquals("follower should observe the new defaultIndexingNumInstances",
                        4, observed);

                // Follower remains alive and able to handle further activity:
                // issue a SECOND rebalance and verify both nodes converge.
                execute(leader, TS, "EXECUTE INDEXING_SERVICE_REBALANCE '" + TS + "', 8",
                        Collections.emptyList(), TransactionContext.NO_TRANSACTION);

                deadline = System.currentTimeMillis() + 15_000;
                observed = -1;
                while (System.currentTimeMillis() < deadline) {
                    TableSpace tsFollower = follower.getMetadataStorageManager()
                            .describeTableSpace(TS);
                    if (tsFollower != null && tsFollower.defaultIndexingNumInstances == 8) {
                        observed = 8;
                        break;
                    }
                    Thread.sleep(100);
                }
                assertEquals(8, observed);

                TableSpace tsLeader = leader.getMetadataStorageManager().describeTableSpace(TS);
                assertNotNull(tsLeader);
                assertEquals(8, tsLeader.defaultIndexingNumInstances);
            }
        }
    }

    /**
     * The REBALANCE log entry must survive a leader restart — the recovery
     * apply path replays every entry from the persisted ledger before the
     * tablespace becomes leadable, so an unhandled or broken entry would
     * stall startup.
     */
    @Test
    public void rebalanceEntrySurvivesLeaderRestart() throws Exception {
        try (DBManager firstLeader = startDBManager("node1")) {
            firstLeader.executeStatement(new CreateTableSpaceStatement(TS,
                    new HashSet<>(Collections.singletonList("node1")),
                    "node1", 1, 0, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            assertTrue(firstLeader.waitForTablespace(TS, 30_000, true));

            execute(firstLeader, TS, "EXECUTE INDEXING_SERVICE_REBALANCE '" + TS + "', 2",
                    Collections.emptyList(), TransactionContext.NO_TRANSACTION);
            assertEquals(2, firstLeader.getMetadataStorageManager()
                    .describeTableSpace(TS).defaultIndexingNumInstances);
        }

        try (DBManager restarted = startDBManager("node1")) {
            assertTrue(restarted.waitForTablespace(TS, 30_000, true));
            assertEquals(2, restarted.getMetadataStorageManager()
                    .describeTableSpace(TS).defaultIndexingNumInstances);
            execute(restarted, TS, "EXECUTE INDEXING_SERVICE_REBALANCE '" + TS + "', 8",
                    Collections.emptyList(), TransactionContext.NO_TRANSACTION);
            assertEquals(8, restarted.getMetadataStorageManager()
                    .describeTableSpace(TS).defaultIndexingNumInstances);
        }
    }
}
