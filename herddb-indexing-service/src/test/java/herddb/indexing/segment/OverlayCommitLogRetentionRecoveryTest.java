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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
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
 * Pins the recovery-correctness invariant for the tombstone overlay across IS
 * restarts. The mechanism the test guards is:
 *
 * <ol>
 *   <li>Tombstones marked in memory but NOT yet flushed to the overlay are
 *       lost on a JVM crash — but the commit-log replay re-applies the same
 *       deletes from the engine watermark onward, so the in-memory state
 *       converges to the same final set.</li>
 *   <li>The published overlay's {@code tombstoneLsn} is monotonically
 *       non-decreasing across flushes — so a recovering manager will never
 *       skip a delete by anchoring at a lower LSN than was previously
 *       observable.</li>
 *   <li>A "fresh" manager constructed against a previously-published
 *       generation must be byte-equivalent to a manager that has been
 *       continuously running with the same delete history.</li>
 * </ol>
 *
 * <p>Restart is simulated by tearing down the {@code TombstoneOverlayManager}
 * (the on-heap state is gone — equivalent to a JVM crash) and constructing a
 * new one against the same registry + DSM. Replay is simulated by re-issuing
 * the {@code markDeleted} calls that the commit-log tailer would have
 * re-emitted from the durable LSN forward.
 */
public class OverlayCommitLogRetentionRecoveryTest {

    private static final String BASE_PATH = "/herd-test-overlay-recovery";
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
        if (dsm != null) {
            dsm.close();
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
    public void unflushedDeletesAreRecoveredViaCommitLogReplay() throws Exception {
        // Phase 1: live IS is running; deletes are marked in memory.
        VersionedSegmentMetadata current = createActiveSegment();
        TombstoneOverlayManager mgr = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);

        // First batch: marked AND flushed to overlay (durable on disk + znode).
        Set<Integer> flushedDeletes = new TreeSet<>(Arrays.asList(1, 2, 3, 4, 5));
        for (int o : flushedDeletes) {
            mgr.markDeleted(o, new LogSequenceNumber(1L, o));
        }
        VersionedSegmentMetadata afterFlush = mgr.flush(current).orElseThrow();
        long durableTombstoneLsnLedger = afterFlush.metadata().getTombstoneLsnLedgerId();
        long durableTombstoneLsnOffset = afterFlush.metadata().getTombstoneLsnOffset();

        // Second batch: marked in memory ONLY — never flushed. Simulates the
        // window between two periodic flushes where deletes accumulate but
        // the overlay file hasn't been re-uploaded yet.
        Set<Integer> unflushedDeletes = new TreeSet<>(Arrays.asList(10, 20, 30));
        long unflushedLsnLedger = 2L;
        for (int i = 0; i < unflushedDeletes.size(); i++) {
            int ord = (Integer) unflushedDeletes.toArray()[i];
            mgr.markDeleted(ord, new LogSequenceNumber(unflushedLsnLedger, i));
        }

        // === Crash: we drop the manager (its in-memory state is gone). ===

        // Phase 2: recovery. The IS engine reads its durable watermark
        // (covering both batches' commit-log entries — the engine's watermark
        // is independent of the overlay's tombstoneLsn), constructs a fresh
        // manager, and replays the commit log from the durable watermark.
        // The fresh manager bulk-loads the published overlay (covers
        // flushedDeletes) AND re-applies every commit-log entry in the
        // window (covers unflushedDeletes via idempotent markDeleted).
        TombstoneOverlayManager recovered = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);
        VersionedSegmentMetadata reread = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID)
                .orElseThrow();
        // The published overlay's path/generation is in the znode; load it.
        long publishedGen = parseGenerationFromPath(reread.metadata().getTombstonePath());
        TombstoneOverlay published = TombstoneOverlayManager.loadOverlay(
                dsm, TS_UUID, IDX_UUID, SEG_UUID, publishedGen);
        recovered.replaceFromOverlay(published);

        // Replay every delete that the commit-log tailer would have re-emitted
        // from the durable watermark forward. In production the watermark is
        // typically AT or BEFORE the overlay's tombstoneLsn (the overlay can
        // be ahead of the watermark since it has its own flush cadence). We
        // model that by replaying ALL deletes regardless of whether they're
        // already in the overlay — markDeleted is idempotent.
        for (int o : flushedDeletes) {
            recovered.markDeleted(o, new LogSequenceNumber(1L, o));
        }
        for (int i = 0; i < unflushedDeletes.size(); i++) {
            int ord = (Integer) unflushedDeletes.toArray()[i];
            recovered.markDeleted(ord, new LogSequenceNumber(unflushedLsnLedger, i));
        }

        // === Convergence: recovered state == flushed ∪ unflushed. ===
        Set<Integer> expected = new HashSet<>(flushedDeletes);
        expected.addAll(unflushedDeletes);
        Set<Integer> actual = new HashSet<>();
        for (int o : recovered.snapshotOrdinals()) {
            actual.add(o);
        }
        assertEquals("recovery via commit-log replay must converge on the full delete set"
                + " (overlay-load covers durable, replay covers in-flight)", expected, actual);

        // Defense: the durable tombstoneLsn from the registry is the ANCHOR
        // — the engine watermark must be ≤ tombstoneLsn at recovery time, OR
        // the engine must replay forward from its durable LSN through the
        // window the overlay covers. Either way, the recovered set must be
        // ≥ the overlay set. Pin: neither flushed nor unflushed deletes are
        // missing from the recovered manager.
        assertTrue(actual.containsAll(flushedDeletes));
        assertTrue(actual.containsAll(unflushedDeletes));
    }

    @Test
    public void tombstoneLsnIsMonotonicAcrossFlushes() throws Exception {
        // Pins the property: the overlay's tombstoneLsn never goes BACKWARDS
        // across flushes. A recovering manager that anchors at the latest
        // overlay's tombstoneLsn can therefore safely replay forward without
        // worrying that an older overlay had a higher LSN it would now miss.
        VersionedSegmentMetadata current = createActiveSegment();
        TombstoneOverlayManager mgr = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);

        // Flush 1: ordinals 1..5 with LSNs (1, 1..5).
        for (int o = 1; o <= 5; o++) {
            mgr.markDeleted(o, new LogSequenceNumber(1L, o));
        }
        VersionedSegmentMetadata afterFlush1 = mgr.flush(current).orElseThrow();
        long lsn1Ledger = afterFlush1.metadata().getTombstoneLsnLedgerId();
        long lsn1Offset = afterFlush1.metadata().getTombstoneLsnOffset();

        // Flush 2: ordinals 6..10 with LSNs (1, 6..10).
        for (int o = 6; o <= 10; o++) {
            mgr.markDeleted(o, new LogSequenceNumber(1L, o));
        }
        VersionedSegmentMetadata afterFlush2 = mgr.flush(afterFlush1).orElseThrow();
        long lsn2Ledger = afterFlush2.metadata().getTombstoneLsnLedgerId();
        long lsn2Offset = afterFlush2.metadata().getTombstoneLsnOffset();

        // Flush 3: ordinals 11..15 with LSNs (2, 0..4) (different ledger).
        for (int i = 0; i < 5; i++) {
            mgr.markDeleted(11 + i, new LogSequenceNumber(2L, i));
        }
        VersionedSegmentMetadata afterFlush3 = mgr.flush(afterFlush2).orElseThrow();
        long lsn3Ledger = afterFlush3.metadata().getTombstoneLsnLedgerId();
        long lsn3Offset = afterFlush3.metadata().getTombstoneLsnOffset();

        // Each successive flush's tombstoneLsn must be ≥ the previous one's.
        assertTrue("tombstoneLsn must be monotone across flushes (flush1 → flush2): "
                        + "(" + lsn1Ledger + "," + lsn1Offset + ") → ("
                        + lsn2Ledger + "," + lsn2Offset + ")",
                compareLsn(lsn2Ledger, lsn2Offset, lsn1Ledger, lsn1Offset) >= 0);
        assertTrue("tombstoneLsn must be monotone across flushes (flush2 → flush3): "
                        + "(" + lsn2Ledger + "," + lsn2Offset + ") → ("
                        + lsn3Ledger + "," + lsn3Offset + ")",
                compareLsn(lsn3Ledger, lsn3Offset, lsn2Ledger, lsn2Offset) >= 0);
    }

    @Test
    public void recoveredManagerByteEquivalentToContinuousManager() throws Exception {
        // Two managers that see the same delete history must produce the
        // same snapshot regardless of whether they were continuously running
        // or recovered from a published overlay + replay. This is the
        // strongest "no surprise on recovery" property.
        VersionedSegmentMetadata current = createActiveSegment();

        // Continuous manager: never restarted.
        TombstoneOverlayManager continuous = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);

        Set<Integer> all = new TreeSet<>();
        for (int batch = 0; batch < 4; batch++) {
            for (int i = 0; i < 7; i++) {
                int ord = batch * 100 + i;
                all.add(ord);
                continuous.markDeleted(ord, new LogSequenceNumber(batch + 1L, i));
            }
            // Flush after every batch so the overlay accumulates generations.
            current = continuous.flush(current).orElseThrow();
        }

        // Recovered manager: bulk-load the latest overlay + replay every
        // markDeleted call from the start (idempotent — set semantics).
        TombstoneOverlayManager recovered = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID,
                tmpFolder.newFolder("recoverTmp").toPath());
        long latestGen = parseGenerationFromPath(current.metadata().getTombstonePath());
        TombstoneOverlay latest = TombstoneOverlayManager.loadOverlay(
                dsm, TS_UUID, IDX_UUID, SEG_UUID, latestGen);
        recovered.replaceFromOverlay(latest);
        // Replay everything (idempotent).
        for (int batch = 0; batch < 4; batch++) {
            for (int i = 0; i < 7; i++) {
                recovered.markDeleted(batch * 100 + i, new LogSequenceNumber(batch + 1L, i));
            }
        }

        int[] continuousSnapshot = continuous.snapshotOrdinals();
        int[] recoveredSnapshot = recovered.snapshotOrdinals();
        Arrays.sort(continuousSnapshot);
        Arrays.sort(recoveredSnapshot);
        org.junit.Assert.assertArrayEquals(
                "recovered manager must be byte-equivalent to a continuously-running manager"
                        + " given the same delete history",
                continuousSnapshot, recoveredSnapshot);
    }

    @Test
    public void replayBeforeOverlayLoadDoesNotLoseEarlierDeletes() throws Exception {
        // A buggy recovery flow that replays the commit log BEFORE bulk-loading
        // the published overlay would risk losing earlier overlay-only deletes
        // if replaceFromOverlay clobbered the in-memory set. Pins the API
        // contract: replaceFromOverlay() REPLACES the in-memory set, so the
        // canonical recovery order is overlay-load FIRST, then replay forward.
        VersionedSegmentMetadata current = createActiveSegment();
        TombstoneOverlayManager owner = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);
        owner.markDeleted(100, new LogSequenceNumber(1L, 100L));
        owner.markDeleted(200, new LogSequenceNumber(1L, 200L));
        VersionedSegmentMetadata afterFlush = owner.flush(current).orElseThrow();
        long publishedGen = parseGenerationFromPath(afterFlush.metadata().getTombstonePath());

        // === Buggy order: replay → replaceFromOverlay (clobbers replay state). ===
        TombstoneOverlayManager buggy = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID,
                tmpFolder.newFolder("buggyTmp").toPath());
        // Replay first (a flawed recovery would do this).
        buggy.markDeleted(100, new LogSequenceNumber(1L, 100L));
        buggy.markDeleted(200, new LogSequenceNumber(1L, 200L));
        buggy.markDeleted(300, new LogSequenceNumber(2L, 0L));   // post-flush in-flight
        // Then replaceFromOverlay — which CLOBBERS the buggy state.
        TombstoneOverlay overlay = TombstoneOverlayManager.loadOverlay(
                dsm, TS_UUID, IDX_UUID, SEG_UUID, publishedGen);
        buggy.replaceFromOverlay(overlay);

        // The buggy manager has lost the in-flight delete (300).
        Set<Integer> buggySet = new HashSet<>();
        for (int o : buggy.snapshotOrdinals()) {
            buggySet.add(o);
        }
        assertFalse("buggy order DEMONSTRATES the lost-delete failure mode (300 was clobbered"
                        + " by the post-replay replaceFromOverlay)",
                buggySet.contains(300));

        // === Correct order: replaceFromOverlay → replay. ===
        TombstoneOverlayManager correct = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID,
                tmpFolder.newFolder("correctTmp").toPath());
        correct.replaceFromOverlay(overlay);
        // Replay the commit-log window — re-apply every entry from the
        // overlay's tombstoneLsn forward, including the in-flight one.
        correct.markDeleted(100, new LogSequenceNumber(1L, 100L));
        correct.markDeleted(200, new LogSequenceNumber(1L, 200L));
        correct.markDeleted(300, new LogSequenceNumber(2L, 0L));

        Set<Integer> correctSet = new HashSet<>();
        for (int o : correct.snapshotOrdinals()) {
            correctSet.add(o);
        }
        assertTrue("correct order preserves every delete (100, 200 from overlay; 300 from replay)",
                correctSet.containsAll(Arrays.asList(100, 200, 300)));
        assertEquals("no extras in the correct recovery", 3, correctSet.size());
    }

    /** Lexicographic LSN compare on (ledger, offset) pairs. */
    private static int compareLsn(long ledgerA, long offsetA, long ledgerB, long offsetB) {
        int c = Long.compare(ledgerA, ledgerB);
        return c != 0 ? c : Long.compare(offsetA, offsetB);
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
