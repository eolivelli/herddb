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
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogSequenceNumber;
import herddb.metadata.IndexingServiceInstanceDescriptor;
import herddb.model.NodeMetadata;
import herddb.model.TableSpace;
import java.util.Properties;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end multi-pod horizontal-scalability tests for the optimizer.
 *
 * <p>Spawns two in-process {@link IndexOptimizerMain} instances pointed at the
 * same ZK + segment registry, one as LEADER (POD_ORDINAL=0) and one as WORKER
 * (POD_ORDINAL=1). Pre-populates segments and verifies that:
 * <ol>
 *   <li>Both pods make progress when leaderExecutesTasks=true.</li>
 *   <li>With leaderExecutesTasks=false the leader produces tasks but only the
 *       worker pod executes them — this is the contract the step-10 K8s
 *       acceptance test relies on.</li>
 * </ol>
 */
public class MultiPodOptimizerHorizontalScaleTest {

    private static final String TS_NAME = "herd";
    private static final String TS_UUID = "ts-mp";
    private static final String IDX_UUID = "idx-mp";
    private static final String BASE_PATH = "/herd-test-multi-pod";

    private TestingServer zkServer;
    private ZookeeperMetadataStorageManager zkMeta;
    private SegmentRegistryClient registry;
    private org.apache.zookeeper.ZooKeeper rawZk;
    /**
     * Issue #555: stands in for the IS pods that would write swap-ack znodes
     * after adopting a PROVISIONAL merge output, so the optimizer's atomic
     * swap can commit in these no-real-IS integration tests.
     */
    private SwapAckSimulator ackSimulator;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        java.util.concurrent.CountDownLatch connected = new java.util.concurrent.CountDownLatch(1);
        rawZk = new org.apache.zookeeper.ZooKeeper(zkServer.getConnectString(), 30000,
                event -> {
                    if (event.getState()
                            == org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected) {
                        connected.countDown();
                    }
                });
        assertTrue(connected.await(30, java.util.concurrent.TimeUnit.SECONDS));
        rawZk.create(BASE_PATH, new byte[0],
                org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                org.apache.zookeeper.CreateMode.PERSISTENT);
        zkMeta = new ZookeeperMetadataStorageManager(zkServer.getConnectString(), 30000, BASE_PATH);
        zkMeta.start(true);
        // Register a tablespace so IndexOptimizerMain.start() resolves the UUID.
        TableSpace ts = TableSpace.builder()
                .leader("node-0")
                .replica("node-0")
                .name(TS_NAME)
                .uuid(TS_UUID)
                .build();
        zkMeta.registerNode(NodeMetadata.builder().nodeId("node-0").host("localhost").port(7000).build());
        zkMeta.registerTableSpace(ts);
        // Register one primary indexing-service instance so LEAST_LOADED has a target.
        zkMeta.registerIndexingServiceInstance(
                IndexingServiceInstanceDescriptor.primary("svc-0", "127.0.0.1:9000", 0));
        registry = new SegmentRegistryClient(() -> rawZk, BASE_PATH);
        registry.ensureRoot();
        ackSimulator = new SwapAckSimulator(registry, zkMeta, TS_UUID);
    }

    @After
    public void tearDown() throws Exception {
        if (ackSimulator != null) {
            ackSimulator.close();
        }
        if (zkMeta != null) {
            zkMeta.close();
        }
        if (rawZk != null) {
            rawZk.close();
        }
        if (zkServer != null) {
            zkServer.close();
        }
    }

    private void seedActive(String segUuid, long sizeBytes) throws Exception {
        SegmentMetadata m = SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(sizeBytes)
                .vectorCount(100L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
        registry.createSegment(m);
    }

    private Properties basicProps(boolean leaderExecutesTasks) {
        Properties props = new Properties();
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT, "30000");
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH, BASE_PATH);
        props.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, TS_NAME);
        props.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "300");
        props.setProperty(OptimizerConfiguration.PROPERTY_RETENTION_MS, "60000");
        props.setProperty(OptimizerConfiguration.PROPERTY_MAX_COUNT, "100");
        props.setProperty(OptimizerConfiguration.PROPERTY_MAX_BYTES, "9999999999");
        props.setProperty(OptimizerConfiguration.PROPERTY_TARGET_MAX_BYTES, "9999999999");
        props.setProperty(OptimizerConfiguration.PROPERTY_MERGE_KWAY_MAX, "8");
        props.setProperty(OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_POLICY, "FIXED_ZERO");
        props.setProperty(OptimizerConfiguration.PROPERTY_HTTP_PORT, "0");
        props.setProperty(OptimizerConfiguration.PROPERTY_ROLE_LEADER_EXECUTE_TASKS,
                Boolean.toString(leaderExecutesTasks));
        // orphan reset must be >= 2 × session timeout (startup validation).
        props.setProperty(OptimizerConfiguration.PROPERTY_TASKS_ORPHAN_RESET_MS, "60000");
        return props;
    }

    private IndexOptimizerMain startPod(int podOrdinal, boolean leaderExecutesTasks) throws Exception {
        return startPod(podOrdinal, leaderExecutesTasks, new InMemorySegmentMerger(), null);
    }

    private IndexOptimizerMain startPod(int podOrdinal, boolean leaderExecutesTasks,
                                        InMemorySegmentMerger merger, Properties overrides)
            throws Exception {
        Properties props = basicProps(leaderExecutesTasks);
        // Inject role via explicit config — HOSTNAME-based detection wouldn't work in-process.
        props.setProperty(OptimizerConfiguration.PROPERTY_ROLE_IS_LEADER,
                podOrdinal == 0 ? "true" : "false");
        if (overrides != null) {
            for (String key : overrides.stringPropertyNames()) {
                props.setProperty(key, overrides.getProperty(key));
            }
        }
        IndexOptimizerMain pod = new IndexOptimizerMain(new OptimizerConfiguration(props), merger);
        pod.start();
        return pod;
    }

    private void waitUntil(java.util.function.BooleanSupplier cond, long maxMillis) throws Exception {
        long deadline = System.currentTimeMillis() + maxMillis;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        org.junit.Assert.fail("condition was not met within " + maxMillis + " ms");
    }

    private long countActiveOutputs() throws Exception {
        long out = 0;
        for (VersionedSegmentMetadata v : registry.listSegments(TS_UUID, IDX_UUID)) {
            if (v.metadata().getState() == SegmentState.ACTIVE
                    && v.metadata().getReplacedBy() != null
                    && !v.metadata().getReplacedBy().isEmpty()) {
                // existing inputs are not replacedBy anything — outputs from a merge
                // do not have replacedBy set either; differentiate by checking
                // pre-seeded UUIDs vs newly-generated ones. Use the createdAt field
                // instead: outputs have a recent createdAt > 0.
            }
        }
        // Simpler: count ACTIVE segments whose segmentUuid is NOT one of the
        // pre-seeded UUIDs (those start with "seg-pre-").
        out = 0;
        for (VersionedSegmentMetadata v : registry.listSegments(TS_UUID, IDX_UUID)) {
            if (v.metadata().getState() == SegmentState.ACTIVE
                    && !v.metadata().getSegmentUuid().startsWith("seg-pre-")) {
                out++;
            }
        }
        return out;
    }

    @Test
    public void bothPodsMakeProgressWhenLeaderExecutesTasks() throws Exception {
        for (int i = 0; i < 6; i++) {
            seedActive("seg-pre-" + i, 1_000_000L);
        }
        IndexOptimizerMain leader = startPod(0, /* leaderExecutesTasks */ true);
        IndexOptimizerMain worker = startPod(1, /* leaderExecutesTasks */ true);
        try {
            // Wait until at least one merge has landed and all PENDING/CLAIMED tasks drain.
            waitUntil(() -> {
                try {
                    return countActiveOutputs() >= 1 && noPendingOrClaimedTasks();
                } catch (Exception e) {
                    return false;
                }
            }, 30_000L);
            // At least one of the pods executed work. With FIXED_ZERO selector and
            // kwayMax=8, the single producer creates one big task that the first
            // consumer to grab it processes. In a single-pod run that's the leader;
            // in two-pod runs either can win. Assert that the SUM is positive.
            long combined = leader.getTasksCompletedTotal() + worker.getTasksCompletedTotal();
            assertTrue("combined tasksCompletedTotal must be > 0", combined > 0);
            assertTrue(leader.getTasksCreatedTotal() > 0);
        } finally {
            worker.shutdown();
            leader.shutdown();
        }
    }

    @Test
    public void onlyWorkerExecutesWhenLeaderExecuteTasksDisabled() throws Exception {
        for (int i = 0; i < 6; i++) {
            seedActive("seg-pre-" + i, 1_000_000L);
        }
        IndexOptimizerMain leader = startPod(0, /* leaderExecutesTasks */ false);
        IndexOptimizerMain worker = startPod(1, /* leaderExecutesTasks */ true);
        try {
            waitUntil(() -> {
                try {
                    return countActiveOutputs() >= 1 && noPendingOrClaimedTasks();
                } catch (Exception e) {
                    return false;
                }
            }, 30_000L);
            // Leader produced but never executed.
            assertTrue("leader must have produced tasks", leader.getTasksCreatedTotal() > 0);
            assertEquals("leader must not have executed any task",
                    0L, leader.getTasksCompletedTotal());
            assertTrue("worker must have executed at least one task",
                    worker.getTasksCompletedTotal() > 0);
        } finally {
            worker.shutdown();
            leader.shutdown();
        }
    }

    @Test
    public void leaderRoleDetectionVisibleViaApi() throws Exception {
        seedActive("seg-pre-0", 1_000_000L);
        seedActive("seg-pre-1", 1_000_000L);
        IndexOptimizerMain leader = startPod(0, true);
        IndexOptimizerMain worker = startPod(1, true);
        try {
            assertEquals(OptimizerRole.LEADER, leader.getRole());
            assertEquals(OptimizerRole.WORKER, worker.getRole());
        } finally {
            worker.shutdown();
            leader.shutdown();
        }
    }

    @Test
    public void workerCrashMidClaimIsRecoveredByAnotherPod() throws Exception {
        // Acceptance test for the failure-recovery contract: a worker pod
        // claims a task, starts merging, then dies. Another pod (the leader,
        // configured with leaderExecuteTasks=true) must observe the orphaned
        // CLAIMED task once the lease ephemeral is gone, reset it to PENDING
        // via the orphan scanner, and then drain it to completion.
        //
        // To keep the wall-clock short we cap session.timeout at the curator-
        // test minimum (4 s) and orphan.reset.ms at 2× session.timeout (8 s).
        // The test deletes the worker's lease znode directly to simulate the
        // ZK session expiry without actually waiting for it.
        for (int i = 0; i < 4; i++) {
            seedActive("seg-pre-" + i, 1_000_000L);
        }

        // Worker pod (will be crashed): override session timeout + orphan reset
        // so the test runs in <30 s. Inject a merger hook that latches forever
        // — once the worker claims a task it will sleep inside merge().
        java.util.concurrent.CountDownLatch hookEntered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch hookRelease = new java.util.concurrent.CountDownLatch(1);
        InMemorySegmentMerger workerMerger = new InMemorySegmentMerger();
        workerMerger.setHook(() -> {
            hookEntered.countDown();
            try {
                hookRelease.await(60, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });
        Properties fastZk = new Properties();
        fastZk.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT, "4000");
        fastZk.setProperty(OptimizerConfiguration.PROPERTY_TASKS_ORPHAN_RESET_MS, "8000");

        // Leader-only (leaderExecuteTasks=false) so the leader does not race
        // the worker for the first claim — the worker must win deterministically.
        IndexOptimizerMain leader = startPod(0, /* leaderExecutesTasks */ false,
                new InMemorySegmentMerger(), fastZk);
        IndexOptimizerMain worker = startPod(1, /* leaderExecutesTasks */ false,
                workerMerger, fastZk);

        try {
            // Wait until the worker enters the merger hook — confirms a task is
            // CLAIMED on the worker.
            assertTrue("worker must claim a task and enter merge() within 30 s",
                    hookEntered.await(30, java.util.concurrent.TimeUnit.SECONDS));

            // Locate the CLAIMED task and verify it's owned by the worker's UUID.
            OptimizerTaskRegistry taskRegistry = new OptimizerTaskRegistry(() -> rawZk, BASE_PATH);
            VersionedOptimizerTask claimedTask = null;
            for (VersionedOptimizerTask vt : taskRegistry.listTasks(TS_UUID)) {
                if (vt.task().getState() == OptimizerTaskState.CLAIMED) {
                    claimedTask = vt;
                    break;
                }
            }
            assertNotNull("expected at least one CLAIMED task before the simulated crash", claimedTask);

            // Simulate worker crash: delete the lease ephemeral directly (any ZK
            // client with write access can — the orphan scanner's contract is
            // "missing lease + age >= orphan.reset.ms ⇒ reset"), then shut down
            // the worker pod. Real-world equivalents: ZK session expiry, pod
            // SIGKILL, network partition.
            rawZk.delete(taskRegistry.leasePath(TS_UUID, claimedTask.task().getTaskId()), -1);
            // Release the hook so the worker's blocked merge thread can exit
            // before we shut down the pod (avoids a noisy ERROR from the
            // interrupted hook).
            hookRelease.countDown();
            worker.shutdown();

            // Bring up a fresh worker pod (ordinal 2 — we re-use ordinal 1's
            // worker role via explicit config). Start it AFTER the worker we
            // crashed so the claim race is forced onto this fresh pod.
            InMemorySegmentMerger recoveryMerger = new InMemorySegmentMerger();
            // Switch the leader to leaderExecuteTasks=true so it ALSO drains;
            // this is the steady-state production deployment. The leader pod's
            // tick already runs the orphan scanner; with the lease gone and
            // age >= 8 s, the scanner resets the task and the leader's drain
            // path consumes it.
            leader.shutdown();
            leader = startPod(0, /* leaderExecutesTasks */ true,
                    recoveryMerger, fastZk);

            // Wait for the orphan scanner to reset the task AND a worker to
            // drain it. The leader pod (now with leaderExecuteTasks=true)
            // does both. Allow up to 30 s: orphan.reset.ms = 8 s + a few
            // scheduler ticks.
            final OptimizerTaskRegistry taskRegistryFinal = taskRegistry;
            final String claimedTaskId = claimedTask.task().getTaskId();
            waitUntil(() -> {
                try {
                    java.util.Optional<VersionedOptimizerTask> vt =
                            taskRegistryFinal.getTask(TS_UUID, claimedTaskId);
                    return vt.isPresent()
                            && vt.get().task().getState() == OptimizerTaskState.DONE;
                } catch (Exception e) {
                    return false;
                }
            }, 30_000L);

            // The recovery merger ran the work — invocation count must be >= 1.
            assertTrue("recovery worker must have invoked the merger",
                    recoveryMerger.getInvocationCount() >= 1);

            // No PENDING/CLAIMED tasks remain.
            for (VersionedOptimizerTask vt : taskRegistryFinal.listTasks(TS_UUID)) {
                assertEquals("no PENDING/CLAIMED tasks may remain after recovery: " + vt.task(),
                        false, vt.task().getState().blocksInputs());
            }
        } finally {
            hookRelease.countDown(); // belt-and-suspenders
            try {
                worker.shutdown();
            } catch (RuntimeException ignored) {
                // already shut down
            }
            try {
                leader.shutdown();
            } catch (RuntimeException ignored) {
                // already shut down
            }
        }
    }

    @Test
    public void singlePodLeaderAndWorkerEquivalentToLegacy() throws Exception {
        // With replicas=1 the single pod is both leader and worker (default
        // leaderExecutesTasks=true). Verify the full merge cycle still completes
        // end-to-end through the task queue.
        for (int i = 0; i < 4; i++) {
            seedActive("seg-pre-" + i, 1_000_000L);
        }
        IndexOptimizerMain pod = startPod(0, true);
        try {
            waitUntil(() -> {
                try {
                    return countActiveOutputs() >= 1 && noPendingOrClaimedTasks();
                } catch (Exception e) {
                    return false;
                }
            }, 30_000L);
            assertTrue(pod.getTasksCreatedTotal() > 0);
            assertTrue(pod.getTasksCompletedTotal() > 0);
            assertNotNull(pod.getEngine());
        } finally {
            pod.shutdown();
        }
    }

    private boolean noPendingOrClaimedTasks() throws Exception {
        OptimizerTaskRegistry taskRegistry = new OptimizerTaskRegistry(() -> rawZk, BASE_PATH);
        for (VersionedOptimizerTask vt : taskRegistry.listTasks(TS_UUID)) {
            if (vt.task().getState().blocksInputs()) {
                return false;
            }
        }
        return true;
    }
}
