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
import static org.junit.Assert.assertTrue;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentRegistryException;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogSequenceNumber;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit-level coverage for the new optimizer observability counters and gauges
 * exposed at {@code GET /metrics}: compaction breakdown
 * (failures / aborts / declined / last-duration-ms), observed-segment counts
 * by state, and the last-tick wall-clock anchor.
 */
public class OptimizerObservabilityMetricsTest {

    private static final String BASE_PATH = "/herd-test-optimizer-metrics";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;
    private final AtomicLong fakeClock = new AtomicLong(1_000_000L);

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
        registry = new SegmentRegistryClient(() -> zk, BASE_PATH);
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

    private SegmentMetadata sample(String segUuid, long sizeBytes) {
        return SegmentMetadata.builder()
                .segmentUuid(segUuid).tablespaceUuid(TS_UUID).tableName("docs")
                .indexUuid(IDX_UUID).indexName("docs_v1")
                .state(SegmentState.ACTIVE).ownerInstanceId(0)
                .graphPath("g/" + segUuid).mapPath("m/" + segUuid)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(sizeBytes).vectorCount(10L).generation(1L).createdAtEpochMillis(0L)
                .build();
    }

    @Test
    public void initialCountersAreSentinelsBeforeFirstRun() {
        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(2, 1000, 0L, Long.MAX_VALUE),
                60_000L, () -> 0, fakeClock::get);
        assertEquals("never-run sentinel", -1L, engine.getLastRunAtMillis());
        assertEquals("never-merged sentinel", -1L, engine.getLastMergeDurationMs());
        assertEquals(0L, engine.getObservedIndexes());
        assertEquals(0L, engine.getObservedActiveSegments());
        assertEquals(0L, engine.getObservedDeprecatedSegments());
        assertEquals(0L, engine.getObservedTransferringSegments());
        assertEquals(0L, engine.getObservedProvisionalSegments());
        assertEquals(0L, engine.getMergeFailuresTotal());
        assertEquals(0L, engine.getMergeAbortsRevalidateFailedTotal());
        assertEquals(0L, engine.getMergeDeclinedTotal());
        assertEquals(0L, engine.getRelocationsInitiatedTotal());
        assertEquals(0L, engine.getRelocationsCompletedTotal());
        assertEquals(0L, engine.getRelocationsAbortedTotal());
    }

    @Test
    public void successfulMergeAdvancesLastMergeDurationAndLastRun() throws Exception {
        registry.createSegment(sample("seg-a", 100L));
        registry.createSegment(sample("seg-b", 200L));
        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(2, 1000, 0L, Long.MAX_VALUE),
                60_000L, () -> 0, fakeClock::get);

        long beforeMs = fakeClock.get();
        // The merger does no I/O — duration will be near-zero. Inflate the
        // clock manually mid-merge so the last-merge-duration counter has a
        // testable signal. We do this by advancing the clock between the
        // two reads runForIndex performs.
        merger.setHook(() -> fakeClock.addAndGet(125));
        engine.runOnce();

        assertEquals("lastRunAt advances to the run's start",
                beforeMs, engine.getLastRunAtMillis());
        assertEquals("merger ran exactly once", 1, merger.getInvocationCount());
        assertEquals("merge duration matches the clock advance during merge",
                125L, engine.getLastMergeDurationMs());
        assertEquals(1L, engine.getSegmentsMerged());
        assertEquals(0L, engine.getMergeFailuresTotal());
        assertEquals(0L, engine.getMergeAbortsRevalidateFailedTotal());
    }

    @Test
    public void mergerThrowingBumpsFailureCounterAndDoesNotAdvanceMergeDuration() throws Exception {
        registry.createSegment(sample("seg-a", 100L));
        registry.createSegment(sample("seg-b", 200L));
        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        merger.setFailureMode(true);

        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(2, 1000, 0L, Long.MAX_VALUE),
                60_000L, () -> 0, fakeClock::get);
        engine.runOnce();

        assertEquals("failure counter advances", 1L, engine.getMergeFailuresTotal());
        assertEquals("no segments merged on failure", 0L, engine.getSegmentsMerged());
        assertEquals("last-merge-duration stays at sentinel — no successful merge yet",
                -1L, engine.getLastMergeDurationMs());
    }

    @Test
    public void mergerDecliningBumpsDeclinedCounter() throws Exception {
        registry.createSegment(sample("seg-a", 100L));
        registry.createSegment(sample("seg-b", 200L));
        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        merger.setReturnNull(true);

        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(2, 1000, 0L, Long.MAX_VALUE),
                60_000L, () -> 0, fakeClock::get);
        engine.runOnce();

        assertEquals(1L, engine.getMergeDeclinedTotal());
        assertEquals(0L, engine.getSegmentsMerged());
        assertEquals(0L, engine.getMergeFailuresTotal());
    }

    @Test
    public void revalidateFailedBumpsAbortCounter() throws Exception {
        registry.createSegment(sample("seg-a", 100L));
        registry.createSegment(sample("seg-b", 200L));
        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(2, 1000, 0L, Long.MAX_VALUE),
                60_000L, () -> 0, fakeClock::get);

        // Inject drift mid-merge: deprecate seg-a after merger.merge() completes
        // but before revalidate runs. Use the merger hook to do so.
        merger.setHook(() -> {
            try {
                VersionedSegmentMetadata v = registry.getSegment(TS_UUID, IDX_UUID, "seg-a")
                        .orElseThrow();
                registry.casUpdateSegment(v, v.metadata().toBuilder()
                        .state(SegmentState.DEPRECATED)
                        .replacedBy(java.util.Collections.singletonList("competitor"))
                        .retentionUntilEpochMillis(System.currentTimeMillis() + 60_000L)
                        .build());
            } catch (SegmentRegistryException e) {
                throw new RuntimeException(e);
            }
        });

        engine.runOnce();

        assertEquals("revalidate-failed counter advances",
                1L, engine.getMergeAbortsRevalidateFailedTotal());
        assertEquals("no merged-segment published on abort", 0L, engine.getSegmentsMerged());
        assertEquals("the merger DID run — failure counter stays 0",
                0L, engine.getMergeFailuresTotal());
    }

    @Test
    public void observedSegmentCountsAggregateAcrossStates() throws Exception {
        // Drop one segment per state into the registry and verify the
        // observed-* gauges expose them after a single tick.
        registry.createSegment(sample("active-1", 100L));
        registry.createSegment(sample("active-2", 100L));
        registry.createSegment(sample("transferring-1", 100L));
        registry.createSegment(sample("provisional-1", 100L));
        registry.createSegment(sample("deprecated-1", 100L));

        // Mutate the last three into non-ACTIVE states.
        VersionedSegmentMetadata t = registry.getSegment(TS_UUID, IDX_UUID, "transferring-1")
                .orElseThrow();
        registry.casUpdateSegment(t, t.metadata().toBuilder()
                .state(SegmentState.TRANSFERRING).pendingOwnerInstanceId(1).build());
        VersionedSegmentMetadata p = registry.getSegment(TS_UUID, IDX_UUID, "provisional-1")
                .orElseThrow();
        registry.casUpdateSegment(p, p.metadata().toBuilder()
                .state(SegmentState.PROVISIONAL).build());
        VersionedSegmentMetadata d = registry.getSegment(TS_UUID, IDX_UUID, "deprecated-1")
                .orElseThrow();
        registry.casUpdateSegment(d, d.metadata().toBuilder()
                .state(SegmentState.DEPRECATED)
                .replacedBy(java.util.Collections.singletonList("merged"))
                .retentionUntilEpochMillis(fakeClock.get() + 600_000L)
                .build());

        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                // High thresholds so the policy declines and we observe
                // pure observability numbers without any state changes
                // from inside the tick.
                new MergePolicy.SmallestFirstPolicy(/* minCount */ 100, 200, 1L << 60, 1L << 61),
                60_000L, () -> 0, fakeClock::get);
        engine.runOnce();

        assertEquals(2L, engine.getObservedActiveSegments());
        assertEquals(1L, engine.getObservedTransferringSegments());
        assertEquals(1L, engine.getObservedProvisionalSegments());
        assertEquals(1L, engine.getObservedDeprecatedSegments());
        assertEquals(1L, engine.getObservedIndexes());
    }

    @Test
    public void httpMetricsHandlerIncludesAllNewLines() throws Exception {
        // Bring up the OptimizerHttpServer with a real engine, scrape
        // /metrics, and verify the new counter / gauge names appear.
        registry.createSegment(sample("seg-a", 100L));
        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(2, 1000, 0L, Long.MAX_VALUE),
                60_000L, () -> 0, fakeClock::get);
        engine.runOnce();

        // Bring up the HTTP server on a free port.
        OptimizerHttpServer http = new OptimizerHttpServer("127.0.0.1", 0, engine);
        http.start();
        try {
            int port = http.getBoundPort();
            String body = readBody("http://127.0.0.1:" + port + "/metrics");

            // New counters / gauges must appear by name.
            assertTrue("merge_failures_total line expected",
                    body.contains("herddb_optimizer_merge_failures_total"));
            assertTrue("merge_aborts_revalidate_failed_total line expected",
                    body.contains("herddb_optimizer_merge_aborts_revalidate_failed_total"));
            assertTrue("merge_declined_total line expected",
                    body.contains("herddb_optimizer_merge_declined_total"));
            assertTrue("last_merge_duration_ms line expected",
                    body.contains("herddb_optimizer_last_merge_duration_ms"));
            assertTrue("last_run_at_seconds line expected",
                    body.contains("herddb_optimizer_last_run_at_seconds"));
            assertTrue("observed_indexes line expected",
                    body.contains("herddb_optimizer_observed_indexes"));
            assertTrue("observed_segments line expected with state=active label",
                    body.contains("herddb_optimizer_observed_segments{state=\"active\"}"));
            assertTrue("observed_segments line expected with state=transferring label",
                    body.contains("herddb_optimizer_observed_segments{state=\"transferring\"}"));
            assertTrue("relocations_initiated_total line expected",
                    body.contains("herddb_optimizer_relocations_initiated_total"));
            assertTrue("relocations_completed_total line expected",
                    body.contains("herddb_optimizer_relocations_completed_total"));
            assertTrue("relocations_aborted_total line expected",
                    body.contains("herddb_optimizer_relocations_aborted_total"));
        } finally {
            http.close();
        }
    }

    private static String readBody(String urlString) throws Exception {
        java.net.URL url = new java.net.URL(urlString);
        java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5_000);
        conn.setReadTimeout(5_000);
        try (java.io.InputStream in = conn.getInputStream()) {
            byte[] all = in.readAllBytes();
            return new String(all, java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
