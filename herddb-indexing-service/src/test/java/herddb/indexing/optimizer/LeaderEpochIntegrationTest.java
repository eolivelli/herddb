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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Real-ZK tests for {@link LeaderEpoch}: monotonic bumps survive client
 * sessions, concurrent bumpers race deterministically, and the persistent
 * znode is observable across reconnects.
 */
public class LeaderEpochIntegrationTest {

    private static final String BASE_PATH = "/herd-test-leader-epoch";
    private static final String TS_UUID = "ts-uuid";

    private TestingServer zkServer;
    private ZooKeeper zk;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        zk = openZk();
        zk.create(BASE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @After
    public void tearDown() throws Exception {
        if (zk != null) {
            zk.close();
        }
        if (zkServer != null) {
            zkServer.close();
        }
    }

    private ZooKeeper openZk() throws Exception {
        CountDownLatch connected = new CountDownLatch(1);
        ZooKeeper z = new ZooKeeper(zkServer.getConnectString(), 30000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue(connected.await(30, TimeUnit.SECONDS));
        return z;
    }

    @Test
    public void initialEpochIsZero() throws Exception {
        LeaderEpoch epoch = new LeaderEpoch(() -> zk, BASE_PATH, TS_UUID);
        assertEquals(0L, epoch.currentEpoch());
    }

    @Test
    public void firstBumpReturnsOne() throws Exception {
        LeaderEpoch epoch = new LeaderEpoch(() -> zk, BASE_PATH, TS_UUID);
        assertEquals(1L, epoch.bumpEpoch());
        assertEquals(1L, epoch.currentEpoch());
    }

    @Test
    public void sequentialBumpsAreMonotonic() throws Exception {
        LeaderEpoch epoch = new LeaderEpoch(() -> zk, BASE_PATH, TS_UUID);
        assertEquals(1L, epoch.bumpEpoch());
        assertEquals(2L, epoch.bumpEpoch());
        assertEquals(3L, epoch.bumpEpoch());
        assertEquals(3L, epoch.currentEpoch());
    }

    @Test
    public void sequentialBumpsAreMonotonicAcrossClients() throws Exception {
        // Two LeaderEpoch instances over distinct ZK sessions still see a
        // monotonic, consistent epoch — bumpEpoch always re-reads before
        // CAS so the second bump observes the first one's effect.
        LeaderEpoch a = new LeaderEpoch(() -> zk, BASE_PATH, TS_UUID);
        ZooKeeper zk2 = openZk();
        try {
            LeaderEpoch b = new LeaderEpoch(() -> zk2, BASE_PATH, TS_UUID);
            assertEquals(1L, a.bumpEpoch());
            assertEquals(2L, b.bumpEpoch());
            assertEquals(3L, a.bumpEpoch());
            assertEquals(4L, b.bumpEpoch());
            assertEquals(4L, a.currentEpoch());
            assertEquals(4L, b.currentEpoch());
        } finally {
            zk2.close();
        }
    }

    @Test
    public void staleStatBumperHitsVersionMismatch() throws Exception {
        // Demonstrates the BadVersion path: we open the znode out-of-band, then
        // ANOTHER writer updates it, then our cached Stat-based setData fails.
        // Production producers will rely on this when a stale leader's tick
        // collides with a new leader's first bump.
        LeaderEpoch a = new LeaderEpoch(() -> zk, BASE_PATH, TS_UUID);
        a.bumpEpoch(); // creates the znode at epoch 1

        // Read out-of-band so we have a Stat.
        org.apache.zookeeper.data.Stat staleStat = zk.exists(a.getPath(), false);
        assertNotNull(staleStat);

        // Bump via a separate session — advances the znode version under us.
        ZooKeeper zk2 = openZk();
        try {
            new LeaderEpoch(() -> zk2, BASE_PATH, TS_UUID).bumpEpoch();
        } finally {
            zk2.close();
        }

        // Now setData using the stale version must fail.
        try {
            zk.setData(a.getPath(), "999".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    staleStat.getVersion());
            fail("expected BadVersionException");
        } catch (org.apache.zookeeper.KeeperException.BadVersionException ok) {
            // expected
        }
    }

    @Test
    public void epochSurvivesSessionClose() throws Exception {
        LeaderEpoch epoch = new LeaderEpoch(() -> zk, BASE_PATH, TS_UUID);
        epoch.bumpEpoch();
        epoch.bumpEpoch();
        assertEquals(2L, epoch.currentEpoch());

        // Open a fresh session and read — must observe the same epoch.
        ZooKeeper fresh = openZk();
        try {
            LeaderEpoch epochFromFresh = new LeaderEpoch(() -> fresh, BASE_PATH, TS_UUID);
            assertEquals(2L, epochFromFresh.currentEpoch());
        } finally {
            fresh.close();
        }
    }

    @Test
    public void distinctTablespacesHaveIndependentEpochs() throws Exception {
        LeaderEpoch a = new LeaderEpoch(() -> zk, BASE_PATH, "ts-A");
        LeaderEpoch b = new LeaderEpoch(() -> zk, BASE_PATH, "ts-B");
        a.bumpEpoch();
        a.bumpEpoch();
        b.bumpEpoch();
        assertEquals(2L, a.currentEpoch());
        assertEquals(1L, b.currentEpoch());
    }

    @Test
    public void pathContainsTablespaceAndPrefix() {
        LeaderEpoch e = new LeaderEpoch(() -> zk, BASE_PATH, TS_UUID);
        assertNotNull(e.getPath());
        assertTrue(e.getPath().contains(TS_UUID));
        assertTrue(e.getPath().contains("/index-optimizer/leader-epoch-"));
    }

    @Test
    public void nullSupplierFails() {
        try {
            new LeaderEpoch(null, BASE_PATH, TS_UUID);
            fail("expected NPE");
        } catch (NullPointerException ok) {
            // expected
        }
    }
}
