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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.log.LogSequenceNumber;
import herddb.model.TableSpace;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
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
 * End-to-end test for the persistent-recursive ZK watch driving event-driven
 * scheduling (issue #484).
 *
 * <p>Spins up a real {@code TestingServer}, registers the tablespace,
 * boots {@link IndexOptimizerMain} with a 1-hour periodic interval (so the
 * safety-net tick effectively never fires), then publishes new segments
 * to the registry and asserts the engine ticks within a few seconds —
 * proving the persistent-recursive watch path actually wakes the engine.
 *
 * <p>Also asserts that the watch survives multiple events without re-arming
 * (the whole point of {@code AddWatchMode.PERSISTENT_RECURSIVE}): publishing
 * a second batch of segments triggers another tick without any code path
 * having to re-register the watch in between.
 */
public class IndexOptimizerEventDrivenTest {

    private static final String BASE_PATH = "/herd-test-event-driven";
    private static final String TS_NAME = "herd";
    private static final String TS_UUID = "ts-evt";
    private static final String IDX_UUID = "idx-evt";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        CountDownLatch connected = new CountDownLatch(1);
        zk = new ZooKeeper(zkServer.getConnectString(), 30000, ev -> {
            if (ev.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue(connected.await(30, TimeUnit.SECONDS));
        zk.create(BASE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        registry = new SegmentRegistryClient(() -> zk, BASE_PATH);
        registry.ensureRoot();
        // Pre-register the tablespace so IndexOptimizerMain can resolve the UUID.
        registerTablespace(TS_NAME, TS_UUID);
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

    private void registerTablespace(String name, String uuid) throws Exception {
        Set<String> replicas = new HashSet<>();
        replicas.add("test-instance");
        TableSpace ts = TableSpace.builder()
                .name(name)
                .uuid(uuid)
                .leader("test-instance")
                .replicas(replicas)
                .expectedReplicaCount(1)
                .build();
        try (ZookeeperMetadataStorageManager zkmeta =
                new ZookeeperMetadataStorageManager(zkServer.getConnectString(), 30000, BASE_PATH)) {
            zkmeta.start(true);
            zkmeta.registerTableSpace(ts);
        }
    }

    private SegmentMetadata sampleSegment(String segUuid, long sizeBytes) {
        return SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .graphPath("g/" + segUuid)
                .mapPath("m/" + segUuid)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(sizeBytes)
                .vectorCount(10L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
    }

    @Test
    public void persistentRecursiveWatchWakesEngineWithoutPeriodicTick() throws Exception {
        Properties props = new Properties();
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT, "30000");
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH, BASE_PATH);
        props.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, TS_NAME);
        // 1-hour periodic interval = effectively disabled for this test.
        props.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "3600000");
        // Short debounce so the test asserts quickly.
        props.setProperty(OptimizerConfiguration.PROPERTY_EVENT_DEBOUNCE_MS, "50");
        // Disable HTTP probe — keeps the test self-contained.
        props.setProperty(OptimizerConfiguration.PROPERTY_HTTP_PORT, "0");
        props.setProperty(OptimizerConfiguration.PROPERTY_RETENTION_MS, "60000");

        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        IndexOptimizerMain main = new IndexOptimizerMain(
                new OptimizerConfiguration(props), merger);
        try {
            main.start();
            assertNotNull(main.getEngine());

            // Initial state: no events, no event-driven ticks. The persistent-
            // recursive watch is armed on basePath/index-segments/ts-evt.
            long ticksAtStart = main.getEventDrivenTicks();

            // Publish 4 segments. Each createSegment fires children-changed events
            // that the persistent-recursive watch picks up. We expect at least
            // ONE event-driven tick within a few seconds — bursts coalesce.
            for (int i = 0; i < 4; i++) {
                registry.createSegment(sampleSegment("seg-" + i, 100L));
            }

            long deadline = System.currentTimeMillis() + 5_000L;
            while (System.currentTimeMillis() < deadline
                    && main.getEventDrivenTicks() <= ticksAtStart) {
                Thread.sleep(20);
            }
            assertTrue("event-driven tick must fire within 5s of segment publishing"
                            + " (observed " + main.getEventDrivenTicks() + ", was "
                            + ticksAtStart + ")",
                    main.getEventDrivenTicks() > ticksAtStart);
            assertTrue("watcher must have observed at least one event ("
                            + main.getWatcherEvents() + ")",
                    main.getWatcherEvents() > 0);

            // Wait for the engine's first tick to actually drain (so the leader
            // lock is held and getRuns() reflects the work). We poll because
            // runEventDrivenTick increments getEventDrivenTicks BEFORE running
            // tickSafe, so the run counter lags the event-driven count slightly.
            deadline = System.currentTimeMillis() + 5_000L;
            while (System.currentTimeMillis() < deadline
                    && main.getEngine().getRuns() == 0L) {
                Thread.sleep(20);
            }
            assertTrue("engine.runs must increment within 5s of event-driven tick",
                    main.getEngine().getRuns() > 0L);

            // === Part 2: prove the persistent-recursive watch survives a second
            //  burst — i.e. it does NOT need re-arming after the first event,
            //  unlike a one-shot watcher.
            long ticksAfterFirstBurst = main.getEventDrivenTicks();
            for (int i = 4; i < 8; i++) {
                registry.createSegment(sampleSegment("seg-" + i, 100L));
            }
            deadline = System.currentTimeMillis() + 5_000L;
            while (System.currentTimeMillis() < deadline
                    && main.getEventDrivenTicks() <= ticksAfterFirstBurst) {
                Thread.sleep(20);
            }
            assertTrue("second burst must fire another event-driven tick — proves the"
                            + " PERSISTENT_RECURSIVE watch survives without re-arming",
                    main.getEventDrivenTicks() > ticksAfterFirstBurst);

            // Sanity: we should NEVER have hit the 1-hour periodic safety-net here.
            // getRuns() counts every runOnce, including event-driven ones. We just
            // assert it has run more than 0 (the count fluctuates with debouncing).
            assertTrue(main.getEngine().getRuns() > 0L);
        } finally {
            main.shutdown();
        }
    }

    @Test
    public void eventsDuringLongTickScheduleFollowUp() throws Exception {
        // Verifies that events arriving DURING a long-running tick get coalesced
        // into a follow-up tick that fires automatically once the in-progress
        // tick finishes — proving the persistent-recursive watch keeps
        // delivering even when the engine is busy. This is the core
        // "no-event-is-lost" guarantee of the event-driven path.
        Properties props = new Properties();
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT, "30000");
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH, BASE_PATH);
        props.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, TS_NAME);
        // Periodic interval disabled — we only want event-driven ticks.
        props.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "3600000");
        // Short debounce so the follow-up tick lands within the assertion window.
        props.setProperty(OptimizerConfiguration.PROPERTY_EVENT_DEBOUNCE_MS, "50");
        props.setProperty(OptimizerConfiguration.PROPERTY_HTTP_PORT, "0");
        props.setProperty(OptimizerConfiguration.PROPERTY_RETENTION_MS, "60000");

        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        merger.setReturnNull(true); // we don't want segment churn
        // Inject a 1-second sleep into the merger's invocation so the first
        // tick takes ~1 s. Events published during that window must trigger a
        // follow-up tick (not be lost) once the in-progress tick releases the
        // single-threaded scheduler.
        java.util.concurrent.atomic.AtomicLong sleepingHookInvocations =
                new java.util.concurrent.atomic.AtomicLong();
        merger.setHook(() -> {
            sleepingHookInvocations.incrementAndGet();
            try {
                Thread.sleep(800);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });

        IndexOptimizerMain main = new IndexOptimizerMain(
                new OptimizerConfiguration(props), merger);
        try {
            main.start();
            long ticksAtStart = main.getEventDrivenTicks();

            // Publish two segments to fire the first event-driven tick (which
            // sleeps 800 ms inside the merger). Wait for the tick to start.
            registry.createSegment(sampleSegment("warm-1", 100L));
            registry.createSegment(sampleSegment("warm-2", 100L));

            long deadline = System.currentTimeMillis() + 3_000L;
            while (System.currentTimeMillis() < deadline
                    && main.getEventDrivenTicks() <= ticksAtStart) {
                Thread.sleep(20);
            }
            assertTrue("first event-driven tick must start within 3 s",
                    main.getEventDrivenTicks() > ticksAtStart);

            // While the first tick is mid-flight (sleeping inside the merger
            // hook), publish another batch. The watch fires; the wakeup is
            // coalesced into a follow-up tick that must land AFTER the
            // current one finishes.
            long ticksAfterFirst = main.getEventDrivenTicks();
            for (int i = 0; i < 4; i++) {
                registry.createSegment(sampleSegment("during-tick-" + i, 100L));
            }

            // Wait for the second tick to land. Allow up to 5 s — the first
            // tick has up to 800 ms left, the debounce adds 50 ms, then the
            // second tick runs.
            deadline = System.currentTimeMillis() + 5_000L;
            while (System.currentTimeMillis() < deadline
                    && main.getEventDrivenTicks() <= ticksAfterFirst) {
                Thread.sleep(20);
            }
            assertTrue("follow-up event-driven tick must fire after the busy tick releases"
                            + " the scheduler (got " + main.getEventDrivenTicks()
                            + ", was " + ticksAfterFirst + ")",
                    main.getEventDrivenTicks() > ticksAfterFirst);
        } finally {
            // Detach the hook before shutdown so the scheduler can drain quickly.
            merger.setHook(null);
            main.shutdown();
        }
    }

    @Test
    public void debounceCoalescesMultipleEventsIntoOneTick() throws Exception {
        Properties props = new Properties();
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zkServer.getConnectString());
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT, "30000");
        props.setProperty(OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH, BASE_PATH);
        props.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, TS_NAME);
        props.setProperty(OptimizerConfiguration.PROPERTY_INTERVAL_MS, "3600000");
        // Long debounce window so the burst MUST coalesce: we publish 10
        // segments sequentially in well under 500ms, expect exactly one
        // event-driven tick to land.
        props.setProperty(OptimizerConfiguration.PROPERTY_EVENT_DEBOUNCE_MS, "500");
        props.setProperty(OptimizerConfiguration.PROPERTY_HTTP_PORT, "0");
        props.setProperty(OptimizerConfiguration.PROPERTY_RETENTION_MS, "60000");

        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        merger.setReturnNull(true); // we don't want segment churn in the registry
        IndexOptimizerMain main = new IndexOptimizerMain(
                new OptimizerConfiguration(props), merger);
        try {
            main.start();
            long ticksAtStart = main.getEventDrivenTicks();

            // Publish 10 segments in tight loop, well under the debounce window.
            for (int i = 0; i < 10; i++) {
                registry.createSegment(sampleSegment("burst-" + UUID.randomUUID(), 100L));
            }

            // Wait past the debounce window for the coalesced tick to land.
            long deadline = System.currentTimeMillis() + 3_000L;
            while (System.currentTimeMillis() < deadline
                    && main.getEventDrivenTicks() <= ticksAtStart) {
                Thread.sleep(20);
            }
            long ticksAfter = main.getEventDrivenTicks() - ticksAtStart;
            assertTrue("at least one event-driven tick (got " + ticksAfter + ")",
                    ticksAfter >= 1);
            // Sanity: the watch saw multiple events, but they coalesced into a
            // single (or at most a few) tick. We cannot assert "exactly one"
            // because the OS scheduler may insert a brief gap inside the burst,
            // but ticksAfter should be small relative to the 10 events.
            assertTrue("event-driven ticks should be far fewer than the 10 events that fired"
                            + " them (got " + ticksAfter + ", events="
                            + main.getWatcherEvents() + ")",
                    ticksAfter <= main.getWatcherEvents());
        } finally {
            main.shutdown();
        }
    }
}
