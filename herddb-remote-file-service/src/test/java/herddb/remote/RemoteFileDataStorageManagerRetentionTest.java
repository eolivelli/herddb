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

package herddb.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.core.PageSet;
import herddb.core.PostCheckpointAction;
import herddb.log.LogSequenceNumber;
import herddb.model.Record;
import herddb.storage.IndexStatus;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Exercises the deferred page-deletion logic ("replica-aware retention") in
 * {@link RemoteFileDataStorageManager}.
 *
 * <p>The scenarios mirror the correctness requirements for shared-storage read replicas:
 * a page that became stale at checkpoint LSN {@code C_k} must remain accessible until
 * every replica has advanced past {@code C_k}, OR until a safety time-cap forces cleanup.
 */
public class RemoteFileDataStorageManagerRetentionTest {

    private static final String TS = "ts1";
    private static final String TABLE_UUID = "tbluuid";
    private static final String INDEX_UUID = "idxuuid";
    private static final long MIN_RETENTION_MS = 1000;
    private static final long MAX_RETENTION_MS = 10_000;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;
    private ClockControlledStorage storage;

    /**
     * Models the min-LSN-across-replicas value that the leader would read from ZK.
     * Tests mutate it to simulate replica progress, restart, and disappearance.
     */
    private final ConcurrentHashMap<String, LogSequenceNumber> replicaLsns = new ConcurrentHashMap<>();

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("remote").toPath());
        server.start();
        client = new RemoteFileServiceClient(Arrays.asList("localhost:" + server.getPort()));

        Path metadataDir = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();
        storage = new ClockControlledStorage(metadataDir, tmpDir, 1000, client);
        storage.start();

        Function<String, LogSequenceNumber> supplier = replicaLsns::get;
        storage.setRetentionPolicy(supplier, MIN_RETENTION_MS, MAX_RETENTION_MS);

        storage.initTablespace(TS);
        storage.initTable(TS, TABLE_UUID);
        storage.initIndex(TS, INDEX_UUID);
    }

    @After
    public void tearDown() throws Exception {
        storage.close();
        client.close();
        server.stop();
    }

    // ------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------

    private static PageSet.DataPageMetaData meta() throws Exception {
        VisibleByteArrayOutputStream baos = new VisibleByteArrayOutputStream(16);
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(baos)) {
            out.writeVLong(100);
            out.writeVLong(50);
            out.writeVLong(0);
        }
        try (ExtendedDataInputStream in = new ExtendedDataInputStream(
                new SimpleByteArrayInputStream(baos.toByteArray()))) {
            return PageSet.DataPageMetaData.deserialize(in);
        }
    }

    /** Write a page and return its ID. */
    private void writeDataPage(long pageId) throws Exception {
        List<Record> page = new ArrayList<>();
        page.add(new Record(Bytes.from_string("k" + pageId), Bytes.from_string("v" + pageId)));
        storage.writePage(TS, TABLE_UUID, pageId, page);
    }

    private void writeIndexPage(long pageId) throws Exception {
        byte[] data = ("idx" + pageId).getBytes();
        storage.writeIndexPage(TS, INDEX_UUID, pageId, out -> out.write(data));
    }

    /** Produce a TableStatus with the given active page IDs and LSN. */
    private TableStatus tableStatus(LogSequenceNumber lsn, long... activePageIds) throws Exception {
        Map<Long, PageSet.DataPageMetaData> activePages = new HashMap<>();
        for (long id : activePageIds) {
            activePages.put(id, meta());
        }
        long next = activePageIds.length == 0 ? 1
                : Arrays.stream(activePageIds).max().getAsLong() + 1;
        return new TableStatus(TABLE_UUID, lsn, Bytes.longToByteArray(next), next, activePages);
    }

    private IndexStatus indexStatus(LogSequenceNumber lsn, long... activePageIds) {
        Set<Long> active = new HashSet<>();
        for (long id : activePageIds) {
            active.add(id);
        }
        long next = activePageIds.length == 0 ? 1
                : Arrays.stream(activePageIds).max().getAsLong() + 1;
        return new IndexStatus(INDEX_UUID, lsn, next, active, new byte[0]);
    }

    /** Run all {@link PostCheckpointAction}s — simulates what TableSpaceManager does post-checkpoint. */
    private void runActions(List<PostCheckpointAction> actions) {
        for (PostCheckpointAction a : actions) {
            a.run();
        }
    }

    private boolean pageExists(long pageId) {
        return client.readFile(TS + "/" + TABLE_UUID + "/data/" + pageId + ".page") != null;
    }

    private boolean indexPageExists(long pageId) {
        return client.readFile(TS + "/" + INDEX_UUID + "/index/" + pageId + ".page") != null;
    }

    private LogSequenceNumber lsn(long ledger, long offset) {
        return new LogSequenceNumber(ledger, offset);
    }

    // ------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------

    /**
     * Without retention enabled, pages are deleted immediately (current behaviour,
     * backward compatibility).
     */
    @Test
    public void noRetention_deletionIsImmediate() throws Exception {
        // recreate storage WITHOUT retention policy
        storage.close();
        storage = new ClockControlledStorage(
                folder.newFolder("metadata2").toPath(),
                folder.newFolder("tmp2").toPath(), 1000, client);
        storage.start();
        storage.initTablespace(TS);
        storage.initTable(TS, TABLE_UUID);

        writeDataPage(1);
        writeDataPage(2);

        // checkpoint C1: both pages active
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 1), 1, 2), false));

        // checkpoint C2: page 1 removed
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 2), 2), false));

        assertFalse("without retention, stale page deleted immediately", pageExists(1));
        assertTrue(pageExists(2));
    }

    /**
     * When no replicas are registered, pages are retained for at least {@code maxRetentionMs}
     * before being force-deleted.
     */
    @Test
    public void noReplicas_pagesRetainedUntilMaxAge() throws Exception {
        writeDataPage(1);
        writeDataPage(2);

        // C1: {1,2} active
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 1), 1, 2), false));

        // C2: page 1 becomes stale at LSN (1,2)
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 2), 2), false));

        assertTrue("page 1 must be retained with retention enabled", pageExists(1));
        assertEquals(1, storage.pendingDataDeletionCount(TS, TABLE_UUID));

        // advance past min but not max: still pending (no replica ever advances, nothing to release)
        storage.advanceClockMs(MIN_RETENTION_MS + 100);
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 3), 2), false));
        assertTrue("still retained before max age", pageExists(1));

        // advance past max: force-deleted on next checkpoint
        storage.advanceClockMs(MAX_RETENTION_MS);
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 4), 2), false));
        assertFalse("force-deleted past max age", pageExists(1));
        assertEquals(0, storage.pendingDataDeletionCount(TS, TABLE_UUID));
    }

    /**
     * A replica that remains at the old checkpoint blocks deletion even past the
     * min-retention grace period.
     */
    @Test
    public void replicaBehind_pagesRetained_untilAdvances() throws Exception {
        writeDataPage(1);
        writeDataPage(2);

        // C1: {1,2} active. Replica registers at C1.
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 1), 1, 2), false));
        replicaLsns.put(TS, lsn(1, 1));

        // C2: page 1 stale at (1,2). Replica still at (1,1).
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 2), 2), false));
        assertTrue(pageExists(1));

        // advance well past min retention, replica still behind
        storage.advanceClockMs(MIN_RETENTION_MS * 5);
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 3), 2), false));
        assertTrue("replica still at C1 — page retained", pageExists(1));

        // replica refreshes and catches up to C2
        replicaLsns.put(TS, lsn(1, 2));
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 4), 2), false));
        assertFalse("replica past stale-LSN — page deleted", pageExists(1));
    }

    /**
     * When replica advances past stale-LSN but the min-retention grace period has not
     * elapsed, deletion still waits.
     */
    @Test
    public void minGracePeriodEnforced_evenWhenReplicaCaughtUp() throws Exception {
        writeDataPage(1);
        writeDataPage(2);

        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 1), 1, 2), false));
        // Replica is already ahead — simulate a fresh replica catching up quickly.
        replicaLsns.put(TS, lsn(5, 0));

        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 2), 2), false));

        // No clock advance at all — min grace has not elapsed.
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 3), 2), false));
        assertTrue("min retention not yet elapsed", pageExists(1));
        assertEquals(1, storage.pendingDataDeletionCount(TS, TABLE_UUID));

        storage.advanceClockMs(MIN_RETENTION_MS + 1);
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 4), 2), false));
        assertFalse("min retention elapsed + replica ahead — deleted", pageExists(1));
    }

    /**
     * A stuck replica that never advances is bypassed once max retention is hit.
     */
    @Test
    public void stuckReplica_forceDeleteAfterMaxAge() throws Exception {
        writeDataPage(1);
        writeDataPage(2);

        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 1), 1, 2), false));
        replicaLsns.put(TS, lsn(1, 1));

        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 2), 2), false));

        // replica never advances. Page retained at first...
        storage.advanceClockMs(MIN_RETENTION_MS * 2);
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 3), 2), false));
        assertTrue("within max retention window — retained", pageExists(1));

        // ...but max retention fires eventually.
        storage.advanceClockMs(MAX_RETENTION_MS);
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 4), 2), false));
        assertFalse("max retention elapsed — force-deleted", pageExists(1));
    }

    /**
     * A replica that was present BEFORE the leader's first stale-making checkpoint
     * then restarts (briefly absent, then back) is tracked correctly: pages are
     * retained until the (new) replica advances past the stale-LSN.
     */
    @Test
    public void replicaRestart_beforeLeaderCheckpoint() throws Exception {
        writeDataPage(1);
        writeDataPage(2);

        // Replica present and at C1.
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 1), 1, 2), false));
        replicaLsns.put(TS, lsn(1, 1));

        // Replica goes down briefly (ephemeral znode removed -> no entry).
        replicaLsns.remove(TS);

        // Leader checkpoints C2 while replica is down. Page 1 becomes stale at (1,2).
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 2), 2), false));
        storage.advanceClockMs(MIN_RETENTION_MS + 100);

        // No replica, no minReplicaLsn: retention falls back to max-age cap.
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 3), 2), false));
        assertTrue("still within max age — page retained even with no replica", pageExists(1));

        // Replica comes back up, starts at the latest checkpoint it can find: C2 (or newer).
        // Its LSN is >= the stale-LSN of page 1, so next promote drops the pending entry.
        replicaLsns.put(TS, lsn(1, 2));
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 4), 2), false));
        assertFalse("replica restarted at C2 — page can be deleted", pageExists(1));
    }

    /**
     * Replica shuts down AFTER a leader checkpoint queues pending deletions. The
     * ephemeral znode is removed, so the leader now has no replica to wait for.
     * The leader should still respect max retention before force-deleting (protecting
     * against the replica briefly flapping).
     */
    @Test
    public void replicaRestart_afterLeaderCheckpoint() throws Exception {
        writeDataPage(1);
        writeDataPage(2);

        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 1), 1, 2), false));
        replicaLsns.put(TS, lsn(1, 1));

        // Leader checkpoints, page 1 becomes stale at (1,2).
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 2), 2), false));
        assertTrue(pageExists(1));

        // Replica dies (ephemeral node auto-removed).
        replicaLsns.remove(TS);

        // Without a replica advancing, we rely on the max-age cap.
        storage.advanceClockMs(MIN_RETENTION_MS * 2);
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 3), 2), false));
        assertTrue("before max age — still retained (protects against replica flap)", pageExists(1));

        storage.advanceClockMs(MAX_RETENTION_MS);
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 4), 2), false));
        assertFalse("max age elapsed with no replica — force-deleted", pageExists(1));
    }

    /**
     * Multiple replicas: leader must wait for the SLOWEST (minimum LSN).
     */
    @Test
    public void multipleReplicas_waitsForSlowest() throws Exception {
        writeDataPage(1);
        writeDataPage(2);

        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 1), 1, 2), false));
        // We only publish the MINIMUM LSN across replicas through the supplier,
        // so the test models "min across replicas" directly.
        replicaLsns.put(TS, lsn(1, 1)); // slowest replica at C1

        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 2), 2), false));
        storage.advanceClockMs(MIN_RETENTION_MS + 100);

        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 3), 2), false));
        assertTrue("slow replica still at C1", pageExists(1));

        // Slowest replica advances to C2
        replicaLsns.put(TS, lsn(1, 2));
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 4), 2), false));
        assertFalse("slowest replica caught up — safe to delete", pageExists(1));
    }

    /**
     * Same retention logic applied to secondary-index pages.
     */
    @Test
    public void indexPages_followSameRetentionPolicy() throws Exception {
        writeIndexPage(1);
        writeIndexPage(2);

        runActions(storage.indexCheckpoint(TS, INDEX_UUID,
                indexStatus(lsn(1, 1), 1, 2), false));
        replicaLsns.put(TS, lsn(1, 1));

        runActions(storage.indexCheckpoint(TS, INDEX_UUID,
                indexStatus(lsn(1, 2), 2), false));
        assertTrue("index page 1 retained", indexPageExists(1));
        assertEquals(1, storage.pendingIndexDeletionCount(TS, INDEX_UUID));

        storage.advanceClockMs(MIN_RETENTION_MS + 100);
        replicaLsns.put(TS, lsn(1, 2));
        runActions(storage.indexCheckpoint(TS, INDEX_UUID,
                indexStatus(lsn(1, 3), 2), false));
        assertFalse("replica caught up — index page deleted", indexPageExists(1));
    }

    /**
     * Dropping the table clears pending deletions (pages are removed via bulk delete).
     */
    @Test
    public void dropTable_clearsPendingDeletions() throws Exception {
        writeDataPage(1);
        writeDataPage(2);

        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 1), 1, 2), false));
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 2), 2), false));
        assertEquals(1, storage.pendingDataDeletionCount(TS, TABLE_UUID));

        storage.dropTable(TS, TABLE_UUID);
        assertEquals("drop clears pending", 0, storage.pendingDataDeletionCount(TS, TABLE_UUID));
    }

    /**
     * Pages scheduled for deletion at successive checkpoints are released in order
     * as the replica LSN advances through each.
     */
    @Test
    public void incrementalReplicaProgress_releasesInOrder() throws Exception {
        writeDataPage(1);
        writeDataPage(2);
        writeDataPage(3);

        // C1: {1,2,3}
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 1), 1, 2, 3), false));

        // C2 at lsn(1,2): page 1 stale
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 2), 2, 3), false));
        // C3 at lsn(1,3): page 2 stale
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 3), 3), false));

        assertTrue(pageExists(1));
        assertTrue(pageExists(2));
        assertEquals(2, storage.pendingDataDeletionCount(TS, TABLE_UUID));

        storage.advanceClockMs(MIN_RETENTION_MS + 100);

        // Replica catches up to C2 only: only page 1 should be freed.
        replicaLsns.put(TS, lsn(1, 2));
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 4), 3), false));
        assertFalse("page 1 stale at C2, replica at C2 — safe", pageExists(1));
        assertTrue("page 2 stale at C3, replica only at C2 — retained", pageExists(2));

        // Replica catches up further
        replicaLsns.put(TS, lsn(1, 3));
        runActions(storage.tableCheckpoint(TS, TABLE_UUID,
                tableStatus(lsn(1, 5), 3), false));
        assertFalse("page 2 now safe to delete", pageExists(2));
    }

    // ------------------------------------------------------------
    // Test subclass with a controllable clock
    // ------------------------------------------------------------

    private static class ClockControlledStorage extends RemoteFileDataStorageManager {
        private volatile long clockMs;

        ClockControlledStorage(Path meta, Path tmp, int swap, RemoteFileServiceClient client) {
            super(meta, tmp, swap, client);
            this.clockMs = 1_000_000L; // arbitrary start
        }

        @Override
        long currentTimeMillis() {
            return clockMs;
        }

        void advanceClockMs(long ms) {
            this.clockMs += ms;
        }
    }
}
