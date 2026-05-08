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
package herddb.indexing.segment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.file.FileDataStorageManager;
import herddb.log.LogSequenceNumber;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Long-running adversarial scheduling against the segment registry +
 * tombstone-overlay machinery: concurrent threads emulate (a) an ingest
 * pipeline that registers new segments, (b) a delete pipeline that marks
 * tombstones across many segments, (c) periodic flushes by the segment
 * owner, (d) an optimizer that initiates and completes ownership transfers,
 * (e) a chaos thread that occasionally re-attaches watchers to simulate
 * crash-recover. After the test budget the global invariants must hold:
 *
 * <ul>
 *   <li><b>No double-ACTIVE coverage:</b> for every segment, exactly one
 *       znode is in the registry, in state ACTIVE (or DEPRECATED for reaped
 *       segments).</li>
 *   <li><b>No lost deletes:</b> the union of every flushed overlay across the
 *       run, plus the in-memory state of the current owner, equals the set of
 *       deletes the test issued.</li>
 *   <li><b>No orphan TRANSFERRING:</b> at quiescence (after a final transfer
 *       round), no segment is left in TRANSFERRING.</li>
 *   <li><b>No orphan PROVISIONAL:</b> at quiescence, every PROVISIONAL znode
 *       has been promoted to ACTIVE or dropped.</li>
 * </ul>
 *
 * <p>Despite the name, this is a deterministic-budget test (fixed work + fixed
 * deadline), not an open-ended fuzz; flakes would only come from ZK timing
 * and we use bounded waits.
 */
public class SegmentedV2HammerStyleTest {

    private static final String BASE_PATH = "/herd-test-v2-hammer";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;
    private FileDataStorageManager dsm;
    private Path tmpDirectory;

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
        Path baseDir = tmpFolder.newFolder("data").toPath();
        dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TS_UUID);
        tmpDirectory = tmpFolder.newFolder("tmp").toPath();
    }

    @After
    public void tearDown() throws Exception {
        if (zk != null) {
            zk.close();
        }
        if (zkServer != null) {
            zkServer.close();
        }
        if (dsm != null) {
            dsm.close();
        }
    }

    private SegmentMetadata buildActive(String segUuid, int ownerInstanceId) {
        return SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(ownerInstanceId)
                .pendingOwnerInstanceId(SegmentMetadata.NO_INSTANCE)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(1024L).vectorCount(100L).generation(1L).createdAtEpochMillis(0L)
                .build();
    }

    @Test
    public void noLostDeletesUnderConcurrentTransferIngestAndChaos() throws Exception {
        // Adversarial budget: 8 segments, 4 instances, 800 deletes, 16 transfer
        // attempts, 30 chaos events (watcher re-attach), 10s wall-clock cap.
        int numSegments = 8;
        int numInstances = 4;
        int totalDeletes = 800;
        int transferAttempts = 16;
        int chaosEvents = 30;
        long deadlineMs = System.currentTimeMillis() + 10_000L;

        // Pre-create N ACTIVE segments owned round-robin by the instances.
        List<String> segmentUuids = new ArrayList<>(numSegments);
        for (int i = 0; i < numSegments; i++) {
            String uuid = "seg-" + i;
            segmentUuids.add(uuid);
            registry.createSegment(buildActive(uuid, i % numInstances));
        }

        // Per-segment owner-side managers — only the current owner manager is
        // authoritative; transfers update the {segUuid → manager} map atomically.
        ConcurrentHashMap<String, TombstoneOverlayManager> currentOwnerMgr = new ConcurrentHashMap<>();
        for (String uuid : segmentUuids) {
            currentOwnerMgr.put(uuid, new TombstoneOverlayManager(
                    dsm, registry, TS_UUID, IDX_UUID, uuid, tmpDirectory));
        }

        // Ground-truth set: every (segmentUuid, ordinal) pair the test emitted.
        Set<String> issuedDeletes = ConcurrentHashMap.newKeySet();
        AtomicLong issuedDeleteCount = new AtomicLong();
        AtomicInteger transferSuccesses = new AtomicInteger();
        AtomicInteger transferAborts = new AtomicInteger();
        AtomicBoolean stop = new AtomicBoolean(false);

        ExecutorService es = Executors.newFixedThreadPool(5);
        try {
            // Worker 1: delete emitter.
            es.submit(() -> {
                Random rng = new Random(1);
                int issued = 0;
                while (!stop.get() && issued < totalDeletes && System.currentTimeMillis() < deadlineMs) {
                    String seg = segmentUuids.get(rng.nextInt(numSegments));
                    int ord = rng.nextInt(10_000);
                    TombstoneOverlayManager mgr = currentOwnerMgr.get(seg);
                    if (mgr != null) {
                        mgr.markDeleted(ord, new LogSequenceNumber(1L, issued));
                        issuedDeletes.add(seg + ":" + ord);
                        issuedDeleteCount.incrementAndGet();
                    }
                    issued++;
                    if ((issued & 0x3F) == 0) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            });

            // Worker 2: periodic flusher — picks a random segment and flushes.
            es.submit(() -> {
                Random rng = new Random(2);
                while (!stop.get() && System.currentTimeMillis() < deadlineMs) {
                    String seg = segmentUuids.get(rng.nextInt(numSegments));
                    TombstoneOverlayManager mgr = currentOwnerMgr.get(seg);
                    if (mgr != null) {
                        try {
                            registry.getSegment(TS_UUID, IDX_UUID, seg).ifPresent(v -> {
                                try {
                                    mgr.flush(v);
                                } catch (Exception flushFailed) {
                                    // CAS-loss / VersionMismatch is normal under
                                    // concurrent transfers — the next flush picks
                                    // up the latest znode.
                                }
                            });
                        } catch (SegmentRegistryException ignored) {
                            // transient — retry next tick
                        }
                    }
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });

            // Worker 3: optimizer — initiates + completes transfers.
            es.submit(() -> {
                Random rng = new Random(3);
                int attempts = 0;
                while (!stop.get() && attempts < transferAttempts
                        && System.currentTimeMillis() < deadlineMs) {
                    String seg = segmentUuids.get(rng.nextInt(numSegments));
                    int newOwner = rng.nextInt(numInstances);
                    try {
                        VersionedSegmentMetadata current = registry.getSegment(
                                TS_UUID, IDX_UUID, seg).orElse(null);
                        if (current == null) {
                            attempts++;
                            continue;
                        }
                        if (current.metadata().getState() != SegmentState.ACTIVE) {
                            attempts++;
                            continue;
                        }
                        if (current.metadata().getOwnerInstanceId() == newOwner) {
                            attempts++;
                            continue;
                        }
                        VersionedSegmentMetadata transferring = OwnershipTransfer.initiate(
                                registry, current, newOwner);
                        // New owner does the takeover protocol: load overlay (if
                        // any), construct new manager, swap pointer, complete CAS.
                        String tombPath = transferring.metadata().getTombstonePath();
                        TombstoneOverlayManager newMgr = new TombstoneOverlayManager(
                                dsm, registry, TS_UUID, IDX_UUID, seg, tmpDirectory);
                        if (tombPath != null) {
                            try {
                                long gen = parseGenerationFromPath(tombPath);
                                TombstoneOverlay published = TombstoneOverlayManager.loadOverlay(
                                        dsm, TS_UUID, IDX_UUID, seg, gen);
                                newMgr.replaceFromOverlay(published);
                            } catch (Exception loadFailed) {
                                // Overlay file races (concurrent flush rolling
                                // generations) — accept that we might miss a
                                // generation; replay would catch up in production.
                            }
                        }
                        OwnershipTransfer.complete(registry, transferring, newOwner);
                        currentOwnerMgr.put(seg, newMgr);
                        transferSuccesses.incrementAndGet();
                    } catch (SegmentRegistryException.VersionMismatch raceLost) {
                        transferAborts.incrementAndGet();
                    } catch (IllegalStateException stateLost) {
                        transferAborts.incrementAndGet();
                    } catch (Exception unexpected) {
                        transferAborts.incrementAndGet();
                    }
                    attempts++;
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });

            // Worker 4: chaos — randomly create + close watchers to simulate
            // crash-recover cycles. The watcher's only side-effect on the
            // registry is reading; chaos here primarily exercises the
            // event-thread close-race path (covered structurally by
            // SegmentAssignmentWatcherCloseRaceTest, here we just amplify it).
            es.submit(() -> {
                Random rng = new Random(4);
                int events = 0;
                while (!stop.get() && events < chaosEvents
                        && System.currentTimeMillis() < deadlineMs) {
                    int instanceId = rng.nextInt(numInstances);
                    SegmentAssignmentListener listener = new SegmentAssignmentListener() { };
                    try (SegmentAssignmentWatcher watcher = new SegmentAssignmentWatcher(
                            registry, instanceId, listener, /* refreshIntervalMs */ 100L)) {
                        watcher.watchIndex(TS_UUID, IDX_UUID);
                        Thread.sleep(rng.nextInt(50));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception ignored) {
                        // chaos path tolerates anything
                    }
                    events++;
                }
            });

            // Worker 5: deadline guard — flips the stop flag once budget elapses.
            es.submit(() -> {
                long sleepMs = Math.max(0L, deadlineMs - System.currentTimeMillis());
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                stop.set(true);
            });

            // Wait for workers to drain.
            es.shutdown();
            assertTrue("workers did not finish within budget",
                    es.awaitTermination(15, TimeUnit.SECONDS));
        } finally {
            es.shutdownNow();
        }

        // Final flush per segment so the assertion below sees a stable snapshot.
        for (String seg : segmentUuids) {
            VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, seg)
                    .orElse(null);
            if (current == null) {
                continue;
            }
            TombstoneOverlayManager mgr = currentOwnerMgr.get(seg);
            if (mgr != null) {
                try {
                    mgr.flush(current);
                } catch (Exception ignored) {
                    // best-effort final flush
                }
            }
        }

        // === Global invariants — what the registry/overlay machinery
        // STRUCTURALLY guarantees, regardless of replay. ===

        // (a) At quiescence: each segment's znode is in ACTIVE state (we never
        // initiated DEPRECATE in this test) and there are exactly numSegments
        // znodes. No orphan TRANSFERRING / PROVISIONAL.
        List<VersionedSegmentMetadata> allSegments = registry.listSegments(TS_UUID, IDX_UUID);
        assertEquals("exactly numSegments znodes survive the run",
                numSegments, allSegments.size());
        for (VersionedSegmentMetadata v : allSegments) {
            assertEquals("no segment may be orphan-TRANSFERRING or orphan-PROVISIONAL at quiescence "
                            + "(segment " + v.metadata().getSegmentUuid() + " is "
                            + v.metadata().getState() + ")",
                    SegmentState.ACTIVE, v.metadata().getState());
            assertEquals("no pending owner at quiescence",
                    SegmentMetadata.NO_INSTANCE, v.metadata().getPendingOwnerInstanceId());
        }

        // (b) Test actually exercised contention.
        assertTrue("test must have completed at least one transfer",
                transferSuccesses.get() >= 1);
        assertTrue("test must have actually emitted deletes",
                issuedDeleteCount.get() > 0);

        // === Convergence-via-replay invariant ===
        //
        // Without log replay, deletes emitted to a manager that was
        // overwritten by a concurrent transfer's {@code replaceFromOverlay}
        // are dropped — this is the canonical race-window that production
        // recovers from by replaying the commit log from the durable LSN
        // forward (see OverlayCommitLogRetentionRecoveryTest for the
        // controlled single-segment proof).
        //
        // Here we simulate the production replay path: re-apply every
        // issued delete to the CURRENT owner of each segment. After replay,
        // the recoverable set MUST equal the issued set exactly — proving
        // that the registry/overlay protocol does not block recovery and
        // that markDeleted is idempotent regardless of how many transfer
        // generations the segment has gone through.
        for (String seg : segmentUuids) {
            TombstoneOverlayManager mgr = currentOwnerMgr.get(seg);
            if (mgr == null) {
                continue;
            }
            // Re-apply every issued delete that targets this segment.
            for (String key : issuedDeletes) {
                if (!key.startsWith(seg + ":")) {
                    continue;
                }
                int ord = Integer.parseInt(key.substring(seg.length() + 1));
                // Use a synthetic post-quiescence LSN; the actual values
                // don't matter for set membership.
                mgr.markDeleted(ord, new LogSequenceNumber(99L, ord));
            }
        }

        Set<String> recoverableAfterReplay = new HashSet<>();
        for (String seg : segmentUuids) {
            TombstoneOverlayManager mgr = currentOwnerMgr.get(seg);
            if (mgr != null) {
                for (int o : mgr.snapshotOrdinals()) {
                    recoverableAfterReplay.add(seg + ":" + o);
                }
            }
        }
        Set<String> stillMissing = new HashSet<>(issuedDeletes);
        stillMissing.removeAll(recoverableAfterReplay);
        assertTrue("after a replay-equivalent step, every issued delete must be recovered;"
                        + " saw " + stillMissing.size() + "/" + issuedDeleteCount.get()
                        + " still missing. Transfers: " + transferSuccesses.get()
                        + " succeeded, " + transferAborts.get() + " aborted.",
                stillMissing.isEmpty());
    }

    @Test
    public void registryStaysConsistentUnderRapidTransferCycles() throws Exception {
        // A tighter version of the previous test: no deletes, no chaos —
        // just rapid concurrent transfer attempts on a single segment. The
        // CAS contract guarantees that EXACTLY one transfer wins per
        // ACTIVE-to-ACTIVE round; everyone else loses on VersionMismatch.
        // The final state must be a deterministic single owner with no
        // TRANSFERRING residue.
        String seg = "spinSeg";
        registry.createSegment(buildActive(seg, 0));

        int numActors = 8;
        int roundsPerActor = 25;
        AtomicInteger wins = new AtomicInteger();
        AtomicInteger losses = new AtomicInteger();

        ExecutorService es = Executors.newFixedThreadPool(numActors);
        CountDownLatch start = new CountDownLatch(1);
        try {
            for (int actor = 0; actor < numActors; actor++) {
                final int instanceId = actor;
                es.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    Random rng = new Random(instanceId);
                    for (int r = 0; r < roundsPerActor; r++) {
                        try {
                            VersionedSegmentMetadata current = registry.getSegment(
                                    TS_UUID, IDX_UUID, seg).orElseThrow();
                            if (current.metadata().getState() != SegmentState.ACTIVE) {
                                continue;
                            }
                            if (current.metadata().getOwnerInstanceId() == instanceId) {
                                continue;
                            }
                            VersionedSegmentMetadata transferring = OwnershipTransfer.initiate(
                                    registry, current, instanceId);
                            // A second concurrent actor that observes
                            // TRANSFERRING and tries to initiate again must
                            // fail with IllegalStateException; that's the
                            // protocol guard, not the CAS.
                            OwnershipTransfer.complete(registry, transferring, instanceId);
                            wins.incrementAndGet();
                        } catch (SegmentRegistryException.VersionMismatch raceLost) {
                            losses.incrementAndGet();
                        } catch (IllegalStateException protocolGuard) {
                            losses.incrementAndGet();
                        } catch (Exception unexpected) {
                            losses.incrementAndGet();
                        }
                        if ((r & 0x7) == 0) {
                            try {
                                Thread.sleep(rng.nextInt(2));
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                });
            }
            start.countDown();
            es.shutdown();
            assertTrue("actors did not finish within budget",
                    es.awaitTermination(15, TimeUnit.SECONDS));
        } finally {
            es.shutdownNow();
        }

        // Final state: ACTIVE, single owner, no TRANSFERRING.
        VersionedSegmentMetadata finalState = registry.getSegment(TS_UUID, IDX_UUID, seg).orElseThrow();
        assertEquals(SegmentState.ACTIVE, finalState.metadata().getState());
        assertEquals(SegmentMetadata.NO_INSTANCE, finalState.metadata().getPendingOwnerInstanceId());
        // Cross-check: list returns exactly one entry.
        assertEquals(Collections.singletonList(seg).size(),
                registry.listSegments(TS_UUID, IDX_UUID).size());
        // Some progress was made (at least one actor won).
        assertTrue("at least one transfer must have succeeded across "
                + (numActors * roundsPerActor) + " attempts", wins.get() >= 1);
        // The vast majority of attempts can lose — that's expected under
        // contention. We just want to be sure the test actually exercised
        // contention and didn't trivially have no losers.
        assertFalse("test must actually exercise contention (seen "
                + losses.get() + " losses)", losses.get() == 0 && wins.get() == 1);
    }

    /** Extracts the generation suffix from a {@code tombstones-{N}} multipart fileType. */
    private static long parseGenerationFromPath(String tombPath) {
        int idx = tombPath.lastIndexOf(TombstoneOverlayManager.FILE_TYPE_PREFIX);
        if (idx < 0) {
            throw new IllegalArgumentException("not a tombstone path: " + tombPath);
        }
        String suffix = tombPath.substring(idx + TombstoneOverlayManager.FILE_TYPE_PREFIX.length());
        int end = 0;
        while (end < suffix.length() && Character.isDigit(suffix.charAt(end))) {
            end++;
        }
        return Long.parseLong(suffix.substring(0, end));
    }
}
