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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.List;
import java.util.Optional;
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
 * Real-ZK integration tests for the {@link OptimizerTaskRegistry} CRUD and
 * lease lifecycle. Mirrors the test seam used by
 * {@code OptimizerLeaderLockTest} (curator-test {@code TestingServer}, raw
 * {@link ZooKeeper}).
 */
public class OptimizerTaskRegistryIntegrationTest {

    private static final String BASE_PATH = "/herd-test-task-registry";
    private static final String TS_UUID = "ts-uuid";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private OptimizerTaskRegistry registry;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        CountDownLatch connected = new CountDownLatch(1);
        zk = new ZooKeeper(zkServer.getConnectString(), 30000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue(connected.await(30, TimeUnit.SECONDS));
        zk.create(BASE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        registry = new OptimizerTaskRegistry(() -> zk, BASE_PATH);
        registry.ensureRoot();
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

    private OptimizerTask sampleTask(String taskId) {
        return OptimizerTask.builder()
                .taskId(taskId)
                .tablespaceUuid(TS_UUID)
                .indexUuid("idx-uuid")
                .inputSegmentUuids(List.of("seg-a", "seg-b"))
                .targetOwnerInstanceId(0)
                .state(OptimizerTaskState.PENDING)
                .createdAtEpochMillis(1_700_000_000_000L)
                .leaderEpoch(1L)
                .build();
    }

    @Test
    public void createAndReadBackTask() throws Exception {
        OptimizerTask t = sampleTask("t-1");
        registry.createTask(t);
        Optional<VersionedOptimizerTask> read = registry.getTask(TS_UUID, "t-1");
        assertTrue(read.isPresent());
        assertEquals(t, read.get().task());
        assertEquals(0, read.get().zkVersion());
    }

    @Test
    public void createRejectsDuplicateTaskId() throws Exception {
        registry.createTask(sampleTask("t-dup"));
        try {
            registry.createTask(sampleTask("t-dup"));
            fail("expected TaskAlreadyExists");
        } catch (OptimizerTaskRegistryException.TaskAlreadyExists ok) {
            // expected
        }
    }

    @Test
    public void listTasksReturnsAllTasks() throws Exception {
        registry.createTask(sampleTask("t-1"));
        registry.createTask(sampleTask("t-2"));
        registry.createTask(sampleTask("t-3"));
        List<VersionedOptimizerTask> all = registry.listTasks(TS_UUID);
        assertEquals(3, all.size());
    }

    @Test
    public void listTasksOnUnknownTablespaceReturnsEmpty() throws Exception {
        List<VersionedOptimizerTask> empty = registry.listTasks("never-touched");
        assertEquals(0, empty.size());
    }

    @Test
    public void casUpdateAdvancesVersion() throws Exception {
        registry.createTask(sampleTask("t-cas"));
        VersionedOptimizerTask v0 = registry.getTask(TS_UUID, "t-cas").orElseThrow();
        OptimizerTask updated = v0.task().toBuilder()
                .state(OptimizerTaskState.CLAIMED)
                .claim(new OptimizerTask.WorkerClaim("worker-1", 1_700_000_001_000L))
                .build();
        VersionedOptimizerTask v1 = registry.casUpdateTask(v0, updated);
        assertNotEquals(v0.zkVersion(), v1.zkVersion());
        assertEquals(OptimizerTaskState.CLAIMED, v1.task().getState());
    }

    @Test
    public void casUpdateFailsOnStaleVersion() throws Exception {
        registry.createTask(sampleTask("t-cas-stale"));
        VersionedOptimizerTask v0 = registry.getTask(TS_UUID, "t-cas-stale").orElseThrow();
        // First update succeeds.
        registry.casUpdateTask(v0,
                v0.task().toBuilder().state(OptimizerTaskState.CLAIMED).build());
        // Second update with the original version must fail with VersionMismatch.
        try {
            registry.casUpdateTask(v0,
                    v0.task().toBuilder().state(OptimizerTaskState.FAILED).build());
            fail("expected VersionMismatch");
        } catch (OptimizerTaskRegistryException.VersionMismatch ok) {
            // expected
        }
    }

    @Test
    public void casUpdateRejectsKeyChanges() throws Exception {
        registry.createTask(sampleTask("t-keys"));
        VersionedOptimizerTask v0 = registry.getTask(TS_UUID, "t-keys").orElseThrow();
        OptimizerTask renamed = v0.task().toBuilder().taskId("t-keys-renamed").build();
        try {
            registry.casUpdateTask(v0, renamed);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            // expected
        }
    }

    @Test
    public void casDeleteRemovesTask() throws Exception {
        registry.createTask(sampleTask("t-del"));
        VersionedOptimizerTask v0 = registry.getTask(TS_UUID, "t-del").orElseThrow();
        registry.casDeleteTask(v0);
        assertFalse(registry.getTask(TS_UUID, "t-del").isPresent());
    }

    @Test
    public void casDeleteFailsOnStaleVersionOrMissing() throws Exception {
        try {
            registry.casDeleteTask(new VersionedOptimizerTask(sampleTask("t-never"), 0));
            fail("expected TaskNotFound");
        } catch (OptimizerTaskRegistryException.TaskNotFound ok) {
            // expected
        }
    }

    // -------------------------------------------------------------------------
    // lease lifecycle
    // -------------------------------------------------------------------------

    @Test
    public void createLeaseFirstWinsSecondReturnsFalse() throws Exception {
        registry.createTask(sampleTask("t-lease"));
        assertTrue(registry.createLease(TS_UUID, "t-lease", "worker-A"));
        assertFalse(registry.createLease(TS_UUID, "t-lease", "worker-B"));
    }

    @Test
    public void leaseExistsReflectsCreateDelete() throws Exception {
        registry.createTask(sampleTask("t-lease-x"));
        assertFalse(registry.leaseExists(TS_UUID, "t-lease-x"));
        registry.createLease(TS_UUID, "t-lease-x", "worker-A");
        assertTrue(registry.leaseExists(TS_UUID, "t-lease-x"));
        registry.deleteLease(TS_UUID, "t-lease-x");
        assertFalse(registry.leaseExists(TS_UUID, "t-lease-x"));
    }

    @Test
    public void deleteLeaseIsIdempotent() throws Exception {
        registry.createTask(sampleTask("t-lease-idem"));
        // deleting a lease that doesn't exist is a no-op
        registry.deleteLease(TS_UUID, "t-lease-idem");
        registry.deleteLease(TS_UUID, "t-lease-idem");
        // and again after creating + deleting
        registry.createLease(TS_UUID, "t-lease-idem", "worker-A");
        registry.deleteLease(TS_UUID, "t-lease-idem");
        registry.deleteLease(TS_UUID, "t-lease-idem");
    }

    @Test
    public void readLeaseHolderReturnsWorkerId() throws Exception {
        registry.createTask(sampleTask("t-lease-holder"));
        registry.createLease(TS_UUID, "t-lease-holder", "worker-holder-id");
        assertEquals(Optional.of("worker-holder-id"),
                registry.readLeaseHolder(TS_UUID, "t-lease-holder"));
    }

    @Test
    public void leaseAutoDeletesOnSessionExpiry() throws Exception {
        registry.createTask(sampleTask("t-lease-expire"));
        // Open a second ZK session and claim from that session, then close it.
        CountDownLatch connected = new CountDownLatch(1);
        ZooKeeper otherZk = new ZooKeeper(zkServer.getConnectString(), 4000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        try {
            assertTrue(connected.await(30, TimeUnit.SECONDS));
            OptimizerTaskRegistry otherRegistry =
                    new OptimizerTaskRegistry(() -> otherZk, BASE_PATH);
            assertTrue(otherRegistry.createLease(TS_UUID, "t-lease-expire", "ephemeral-worker"));
            assertTrue(registry.leaseExists(TS_UUID, "t-lease-expire"));
        } finally {
            otherZk.close();
        }
        // Poll until the ephemeral lease is gone (ZK server's session expiry).
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            if (!registry.leaseExists(TS_UUID, "t-lease-expire")) {
                return;
            }
            Thread.sleep(100);
        }
        fail("ephemeral lease did not auto-delete after session closure");
    }

    @Test
    public void casDeleteRemovesLeaseChildFirst() throws Exception {
        // Producer's "orphan task already-deprecated → delete task" path must
        // remove the lease child first because ZK rejects delete on a node
        // with children. casDeleteTask handles this transparently.
        registry.createTask(sampleTask("t-cas-del-lease"));
        VersionedOptimizerTask v0 = registry.getTask(TS_UUID, "t-cas-del-lease").orElseThrow();
        registry.createLease(TS_UUID, "t-cas-del-lease", "worker-X");
        assertTrue(registry.leaseExists(TS_UUID, "t-cas-del-lease"));
        registry.casDeleteTask(v0);
        assertFalse(registry.getTask(TS_UUID, "t-cas-del-lease").isPresent());
    }

    @Test
    public void listTasksFiresWatcherOnNewTask() throws Exception {
        // Pre-create one task so the tablespace parent znode exists and
        // listTasks(ts, watcher) can actually arm a getChildren watcher.
        registry.createTask(sampleTask("t-watch-bootstrap"));
        CountDownLatch fired = new CountDownLatch(1);
        registry.listTasks(TS_UUID, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                fired.countDown();
            }
        });
        registry.createTask(sampleTask("t-watch"));
        assertTrue("watcher must fire on child creation",
                fired.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void rootPathMatchesContract() {
        assertNotNull(registry.getRegistryRootPath());
        assertTrue(registry.getRegistryRootPath().endsWith("/index-optimizer/tasks"));
    }

    @Test
    public void corruptedTaskZnodeSurfacesAsTypedException() throws Exception {
        // Pre-create a real task so the path layout exists.
        registry.createTask(sampleTask("t-corrupt"));
        String taskPath = registry.taskPath(TS_UUID, "t-corrupt");
        // Overwrite the task's payload with bytes that are not valid OptimizerTask JSON.
        zk.setData(taskPath, "{not valid json}".getBytes(), -1);
        try {
            registry.getTask(TS_UUID, "t-corrupt");
            fail("expected OptimizerTaskRegistryException");
        } catch (OptimizerTaskRegistryException ok) {
            // expected — typed exception wraps the underlying IOException
            assertTrue("message must mention the task id, got: " + ok.getMessage(),
                    ok.getMessage().contains("t-corrupt"));
        }
    }
}
