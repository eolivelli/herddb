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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.file.FileDataStorageManager;
import herddb.log.LogSequenceNumber;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
 * Validates the concurrency contract documented for {@link TombstoneOverlayManager#flush}
 * (review item A5):
 * <ul>
 *   <li>{@code markDeleted} calls during the lock-free upload window land in the
 *       in-memory set but are NOT in the published overlay; the next flush picks them up.</li>
 *   <li>A simulated ownership-transfer that loads the published overlay into a fresh
 *       manager and replays the in-flight LSN range produces the same final tombstone set
 *       as the original owner — proving idempotent recovery.</li>
 *   <li>Flushes with no new tombstones since the last publish are skipped (no
 *       redundant overlay file uploaded — review item A5 perf optimization).</li>
 * </ul>
 */
public class TombstoneOverlayConcurrentFlushTest {

    private static final String BASE_PATH = "/herd-test-A5";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";
    private static final String SEG_UUID = "seguid";

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
    }

    private VersionedSegmentMetadata createActiveSegment() throws Exception {
        SegmentMetadata segment = SegmentMetadata.builder()
                .segmentUuid(SEG_UUID)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(1024L)
                .vectorCount(10L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
        registry.createSegment(segment);
        return registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
    }

    @Test
    public void redundantFlushIsSkipped() throws Exception {
        VersionedSegmentMetadata current = createActiveSegment();
        TombstoneOverlayManager mgr = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);
        mgr.markDeleted(1, new LogSequenceNumber(1L, 1L));
        VersionedSegmentMetadata afterFirst = mgr.flush(current).orElseThrow();
        assertEquals(1L, mgr.getLastPublishedGeneration());

        // No markDeleted between flushes — second call must NOT upload a redundant
        // overlay file (review item A5 optimization).
        java.util.Optional<VersionedSegmentMetadata> noop = mgr.flush(afterFirst);
        assertFalse("flush with no new tombstones must be a no-op", noop.isPresent());
        assertEquals(1L, mgr.getLastPublishedGeneration());
    }

    @Test
    public void newOwnerLoadAndReplayConvergesUnderConcurrentMarkDeleted() throws Exception {
        // The contract: if the previous owner had tombstones in flight when the
        // ownership transferred, the new owner downloads the published overlay AND
        // replays log entries from tombstoneLsn to current. The replayed entries are
        // idempotent set-adds, so the final tombstone set converges.
        VersionedSegmentMetadata current = createActiveSegment();
        TombstoneOverlayManager owner = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);

        // Phase 1: mark some tombstones, flush.
        owner.markDeleted(1, new LogSequenceNumber(1L, 1L));
        owner.markDeleted(2, new LogSequenceNumber(1L, 2L));
        owner.markDeleted(3, new LogSequenceNumber(1L, 3L));
        VersionedSegmentMetadata afterFlush = owner.flush(current).orElseThrow();
        assertEquals(1L, owner.getLastPublishedGeneration());

        // Phase 2: in-flight tombstones arrive AFTER the flush snapshot completed.
        // Simulating the lock-free upload window: any tombstone with LSN > published
        // tombstoneLsn that landed before the flush returned must be representable in
        // the new owner's state via overlay-load + log-replay.
        owner.markDeleted(4, new LogSequenceNumber(2L, 0L));
        owner.markDeleted(5, new LogSequenceNumber(2L, 1L));
        owner.markDeleted(2, new LogSequenceNumber(2L, 2L)); // duplicate — idempotent

        // Snapshot the previous-owner's "true" final state.
        int[] previousOwnerSnapshot = owner.snapshotOrdinals();

        // === Simulate ownership transfer ===
        TombstoneOverlay published = TombstoneOverlayManager.loadOverlay(
                dsm, TS_UUID, IDX_UUID, SEG_UUID, /* generation */ 1L);

        TombstoneOverlayManager newOwner = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);
        newOwner.replaceFromOverlay(published);

        // Tail-replay: the new owner re-applies the in-flight tombstones that are at
        // LSNs > published tombstoneLsn. In production these come from the commit-log
        // tailer; in this test we synthesize them.
        newOwner.markDeleted(4, new LogSequenceNumber(2L, 0L));
        newOwner.markDeleted(5, new LogSequenceNumber(2L, 1L));
        newOwner.markDeleted(2, new LogSequenceNumber(2L, 2L));

        // The new owner's in-memory state must equal the previous owner's.
        assertArrayEquals(previousOwnerSnapshot, newOwner.snapshotOrdinals());
    }

    @Test
    public void concurrentMarkDeletedDuringFlushIsNotLost() throws Exception {
        // Spawn a flush + a concurrent stream of markDeleted calls; verify the
        // post-flush manager state contains every tombstone we asked for, and the
        // next flush picks up the in-flight ones.
        VersionedSegmentMetadata current = createActiveSegment();
        TombstoneOverlayManager mgr = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);

        // Pre-populate so the first flush has work to do.
        for (int i = 0; i < 100; i++) {
            mgr.markDeleted(i, new LogSequenceNumber(1L, i));
        }

        ExecutorService es = Executors.newFixedThreadPool(2);
        Set<Integer> expected = new TreeSet<>();
        for (int i = 0; i < 100; i++) {
            expected.add(i);
        }
        try {
            CountDownLatch start = new CountDownLatch(1);
            // Worker A: flushes once.
            Future<VersionedSegmentMetadata> flushFuture = es.submit(() -> {
                start.await();
                return mgr.flush(current).orElseThrow();
            });
            // Worker B: keeps adding tombstones during the flush window.
            Future<Set<Integer>> markFuture = es.submit(() -> {
                start.await();
                Set<Integer> added = new HashSet<>();
                for (int i = 100; i < 200; i++) {
                    mgr.markDeleted(i, new LogSequenceNumber(2L, i));
                    added.add(i);
                }
                return added;
            });
            start.countDown();
            VersionedSegmentMetadata afterFlush = flushFuture.get(15, TimeUnit.SECONDS);
            Set<Integer> added = markFuture.get(15, TimeUnit.SECONDS);
            expected.addAll(added);

            // The in-memory state contains EVERYTHING (pre-flush + during-flush adds).
            int[] snapshot = mgr.snapshotOrdinals();
            assertEquals(expected.size(), snapshot.length);
            for (int o : snapshot) {
                assertTrue("unexpected ordinal " + o, expected.contains(o));
            }

            // The published overlay (gen 1) has at least the pre-flush 100 tombstones.
            // It may or may not have any of the in-flight ones depending on race timing.
            TombstoneOverlay gen1 = TombstoneOverlayManager.loadOverlay(
                    dsm, TS_UUID, IDX_UUID, SEG_UUID, 1L);
            assertTrue("published overlay must contain at least the 100 pre-flush tombstones",
                    gen1.ordinalCount() >= 100);
            assertTrue("published overlay must not contain more than the in-memory state",
                    gen1.ordinalCount() <= 200);

            // A second flush picks up everything that wasn't in gen1.
            mgr.flush(afterFlush).orElseThrow();
            TombstoneOverlay gen2 = TombstoneOverlayManager.loadOverlay(
                    dsm, TS_UUID, IDX_UUID, SEG_UUID, 2L);
            assertEquals("gen 2 contains the full set",
                    expected.size(), gen2.ordinalCount());
        } finally {
            es.shutdownNow();
        }
    }
}
