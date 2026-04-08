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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import herddb.core.ClusterTest;
import herddb.log.LogSequenceNumber;
import herddb.utils.ZKTestEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * ZooKeeper-backed tests for the replica-checkpoint-LSN tracking API used by the
 * shared-storage retention logic.
 */
@Category(ClusterTest.class)
public class ZkReplicaCheckpointLsnTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;
    private static final String TS = "ts-uuid-1";

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookieAndInitCluster();
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    /** No replicas registered → null. */
    @Test
    public void noReplicas_minIsNull() throws Exception {
        try (ZookeeperMetadataStorageManager m = newManager()) {
            m.start();
            assertNull(m.getMinReplicaCheckpointLsn(TS));
        }
    }

    /** Single replica: min is its own LSN. Updating re-publishes the same znode. */
    @Test
    public void singleReplica_publishAndRead() throws Exception {
        try (ZookeeperMetadataStorageManager m = newManager()) {
            m.start();
            LogSequenceNumber a = new LogSequenceNumber(3, 100);
            m.publishReplicaCheckpointLsn(TS, "nodeA", a);
            assertEquals(a, m.getMinReplicaCheckpointLsn(TS));

            LogSequenceNumber b = new LogSequenceNumber(3, 500);
            m.publishReplicaCheckpointLsn(TS, "nodeA", b);
            assertEquals(b, m.getMinReplicaCheckpointLsn(TS));
        }
    }

    /** Two replicas: min is the smaller. */
    @Test
    public void twoReplicas_minIsSmallest() throws Exception {
        try (ZookeeperMetadataStorageManager m = newManager()) {
            m.start();
            m.publishReplicaCheckpointLsn(TS, "fast", new LogSequenceNumber(5, 200));
            m.publishReplicaCheckpointLsn(TS, "slow", new LogSequenceNumber(3, 50));
            assertEquals(new LogSequenceNumber(3, 50), m.getMinReplicaCheckpointLsn(TS));
        }
    }

    /** Explicit unregister removes the replica from the min computation. */
    @Test
    public void unregister_dropsReplica() throws Exception {
        try (ZookeeperMetadataStorageManager m = newManager()) {
            m.start();
            m.publishReplicaCheckpointLsn(TS, "fast", new LogSequenceNumber(5, 200));
            m.publishReplicaCheckpointLsn(TS, "slow", new LogSequenceNumber(3, 50));
            m.unregisterReplicaCheckpointLsn(TS, "slow");
            assertEquals("only fast remains",
                    new LogSequenceNumber(5, 200), m.getMinReplicaCheckpointLsn(TS));

            m.unregisterReplicaCheckpointLsn(TS, "fast");
            assertNull(m.getMinReplicaCheckpointLsn(TS));
        }
    }

    /**
     * Replica znodes are ephemeral: when a replica's ZK session closes, its entry
     * auto-disappears so the leader no longer waits for it.
     */
    @Test
    public void ephemeralReplica_sessionCloseRemoves() throws Exception {
        try (ZookeeperMetadataStorageManager leader = newManager()) {
            leader.start();

            // Use a separate ZK client session for the "replica"
            try (ZookeeperMetadataStorageManager replica = newManager()) {
                replica.start();
                replica.publishReplicaCheckpointLsn(TS, "nodeR", new LogSequenceNumber(4, 7));
                assertNotNull(leader.getMinReplicaCheckpointLsn(TS));
                assertEquals(new LogSequenceNumber(4, 7), leader.getMinReplicaCheckpointLsn(TS));
            } // replica closes session -> ephemeral znode goes away

            // Wait a short moment for ZK to clean up
            long deadline = System.currentTimeMillis() + 10_000;
            while (leader.getMinReplicaCheckpointLsn(TS) != null
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertNull("ephemeral znode removed after session close",
                    leader.getMinReplicaCheckpointLsn(TS));
        }
    }

    /**
     * Unknown tablespace → null.
     */
    @Test
    public void unknownTablespace_returnsNull() throws Exception {
        try (ZookeeperMetadataStorageManager m = newManager()) {
            m.start();
            assertNull(m.getMinReplicaCheckpointLsn("nonexistent-uuid"));
        }
    }

    private ZookeeperMetadataStorageManager newManager() {
        return new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath());
    }
}
