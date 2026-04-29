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

package herddb.core.indexes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.file.FileCommitLog;
import herddb.file.FileCommitLogManager;
import herddb.index.vector.RemoteVectorIndexService;
import herddb.index.vector.VectorIndexManager;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.server.ServerConfiguration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Reproduces issue #355: the server drops commit-log ledgers while the
 * IndexingService (IS) tailer is frozen in Phase A compaction.
 *
 * <h2>Code path under test</h2>
 * <pre>
 *   TableSpaceManager.checkpoint()
 *     → computeTailersRetentionFloor()
 *         → VectorIndexManager.getTailersMinLsn()
 *             → RemoteVectorIndexService.getMinProcessedLsn()   (gRPC call)
 *                 returns Optional.of(START_OF_TIME)            (IS frozen/unreachable)
 *         → returns START_OF_TIME
 *     → BookkeeperCommitLog/FileCommitLog.dropOldLedgers(checkpoint, START_OF_TIME)
 *         BUG: START_OF_TIME.ledgerId == -1 &lt; 0, so the
 *              "tailersFloor.ledgerId &gt;= 0" guard is false → no floor applied
 *         → ledgers ARE dropped (wrong)
 * </pre>
 *
 * <h2>Root cause</h2>
 * {@code computeTailersRetentionFloor()} returns {@code START_OF_TIME} in two
 * semantically opposite cases:
 * <ol>
 *   <li>No vector indexes present → "no constraint, drop freely" (intended).</li>
 *   <li>IS present but frozen / gRPC call timed out → "protect all ledgers" (intended
 *       by the JavaDoc, but NOT honoured by {@code dropOldLedgers}).</li>
 * </ol>
 * {@code dropOldLedgers} treats both cases identically: drop freely.  Case 2 is
 * therefore silently treated as case 1, destroying commit-log ledgers the IS still
 * needs to replay once Phase A finishes.
 *
 * <h2>Tests</h2>
 * <ul>
 *   <li>{@link #frozenIsReturnsStartOfTimeFromGetTailersMinLsn()} — documents that
 *       {@code VectorIndexManager.getTailersMinLsn()} faithfully returns
 *       {@code START_OF_TIME} when the mock IS returns {@code (-1,-1)} as its last
 *       processed LSN.  This is the correct behaviour of the VIM layer; the bug is
 *       downstream.</li>
 *   <li>{@link #issue355StartOfTimeFloorFromFrozenIsDoesNotProtectLedgers()} — the
 *       reproducing failing test.  It asserts that {@code dropOldLedgers(checkpoint,
 *       START_OF_TIME)} must NOT drop any ledger.  The assertion currently fails,
 *       confirming the bug.</li>
 * </ul>
 */
public class Issue355FrozenIsTailerLedgerRetentionTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private static final String TS_UUID = "test-tablespace-uuid-355";

    /**
     * Builds a minimal non-unique vector {@link Index}.  VectorIndexManager
     * requires a single FLOATARRAY column; uniqueness must be {@code false}
     * (enforced by the Index builder for vector type).
     */
    private static Index buildVectorIndex() {
        return Index.builder()
                .name("vec_idx")
                .type(Index.TYPE_VECTOR)
                .table("t1")
                .tablespace("ts")
                .column("embedding", ColumnTypes.FLOATARRAY)
                .build();
    }

    /**
     * Creates a {@link VectorIndexManager} whose remote service returns
     * {@code reportedLsn} from {@link RemoteVectorIndexService#getMinProcessedLsn}.
     * Parameters that are unused by {@code getTailersMinLsn()} are passed as
     * {@code null} / zero.
     */
    private static VectorIndexManager buildVim(Optional<LogSequenceNumber> reportedLsn) {
        Index index = buildVectorIndex();
        RemoteVectorIndexService mockIs = new RemoteVectorIndexService() {
            @Override
            public List<Map.Entry<herddb.utils.Bytes, Float>> search(
                    String tablespace, String table, String idx, float[] vector, int topK) {
                return Collections.emptyList();
            }

            @Override
            public IndexStatusInfo getIndexStatus(String tablespace, String table, String idx) {
                // Returns the frozen IS LSN (ledgerId=-1, offset=-1 = START_OF_TIME).
                long l = reportedLsn.isPresent() ? reportedLsn.get().ledgerId : -1L;
                long o = reportedLsn.isPresent() ? reportedLsn.get().offset  : -1L;
                return new IndexStatusInfo(0, 1, l, o, "phase-a-frozen");
            }

            @Override
            public boolean waitForCatchUp(String tablespace, LogSequenceNumber lsn, long timeoutMs) {
                return true;
            }

            @Override
            public Optional<LogSequenceNumber> getMinProcessedLsn(String tablespace) {
                return reportedLsn;
            }

            @Override
            public void close() { }
        };

        return new VectorIndexManager(
                index,
                /* tableManager   */ null,
                /* log            */ null,
                /* dsm            */ null,
                TS_UUID,
                /* transaction    */ 0L,
                /* writeLockTimeout */ 30_000,
                /* readLockTimeout  */ 30_000,
                () -> mockIs,
                NullStatsLogger.INSTANCE);
    }

    private FileCommitLogManager newManager(Path base) {
        return new FileCommitLogManager(base,
                /* maxLogFileSize */ 1L,       // one entry per file → many ledger files
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_BYTES_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_SYNC_TIME_DEFAULT,
                /* requireSync */ false,
                /* enableO_DIRECT */ false,
                ServerConfiguration.PROPERTY_DEFERRED_SYNC_PERIOD_DEFAULT,
                NullStatsLogger.INSTANCE);
    }

    private Path tsFolder(Path base) {
        return base.resolve("ts.txlog");
    }

    private List<Long> listLogLedgers(Path logDir) throws java.io.IOException {
        List<Long> ids = new ArrayList<>();
        try (java.util.stream.Stream<Path> s = Files.list(logDir)) {
            for (Path p : (Iterable<Path>) s::iterator) {
                String name = p.getFileName().toString();
                if (name.endsWith(".txlog")) {
                    ids.add(Long.parseLong(name.replace(".txlog", ""), 16));
                }
            }
        }
        ids.sort(Comparator.naturalOrder());
        return ids;
    }

    private LogSequenceNumber writeEntry(FileCommitLog log) throws Exception {
        return log.log(LogEntryFactory.beginTransaction(0L), true).getLogSequenceNumber();
    }

    // ---------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------

    /**
     * Documents the VectorIndexManager behaviour: when the IS reports
     * {@code (-1,-1)} (= {@code START_OF_TIME}) as its last processed LSN — which
     * happens when the tailer is frozen in Phase A and has not yet advanced past the
     * beginning of the commit log — {@code getTailersMinLsn()} returns
     * {@code Optional.of(START_OF_TIME)}.
     *
     * <p>This behaviour is <em>correct</em> at the VIM level.  The bug (#355) is
     * that the caller ({@code computeTailersRetentionFloor}) passes this value on to
     * {@code dropOldLedgers}, which misinterprets it as "no constraint".
     */
    @Test
    public void frozenIsReturnsStartOfTimeFromGetTailersMinLsn() {
        // IS frozen in Phase A: reports (-1,-1) = START_OF_TIME.
        VectorIndexManager vim = buildVim(Optional.of(LogSequenceNumber.START_OF_TIME));

        Optional<LogSequenceNumber> floor = vim.getTailersMinLsn();

        assertTrue("VectorIndexManager must relay the IS's (-1,-1) as START_OF_TIME",
                floor.isPresent());
        assertEquals("VectorIndexManager floor should be START_OF_TIME when IS reports (-1,-1)",
                LogSequenceNumber.START_OF_TIME, floor.get());
    }

    /**
     * Reproducing failing test for issue #355.
     *
     * <p>Scenario: the IS is present, has been seen by the server (so the
     * {@code peakChannelCount} guard in {@code IndexingServiceClient} does not fire),
     * but its tailer is frozen in Phase A.  The gRPC {@code GetIndexStatus} response
     * carries {@code lastLsnLedger=-1, lastLsnOffset=-1}.
     * {@code IndexingServiceClient.getMinProcessedLsn} therefore returns
     * {@code Optional.of(START_OF_TIME)}.
     * {@code VectorIndexManager.getTailersMinLsn()} relays this unchanged.
     * {@code computeTailersRetentionFloor()} short-circuits and returns
     * {@code START_OF_TIME}.  This value is then passed to
     * {@code dropOldLedgers(checkpoint, START_OF_TIME)}.
     *
     * <p>The correct post-fix behaviour: because the IS is present (a floor was
     * contributed), {@code START_OF_TIME} means "the tailer is at the very beginning —
     * protect <em>all</em> ledgers."  No ledger file should be deleted.
     *
     * <p><strong>This assertion currently FAILS</strong>, reproducing the bug: the
     * guard {@code tailersFloor.ledgerId >= 0} is false for {@code -1}, so no floor
     * is applied and ledgers are silently dropped.
     */
    @Test
    public void issue355StartOfTimeFloorFromFrozenIsDoesNotProtectLedgers() throws Exception {
        // Step 1: VIM with a frozen IS reporting (-1,-1).
        VectorIndexManager vim = buildVim(Optional.of(LogSequenceNumber.START_OF_TIME));
        Optional<LogSequenceNumber> contributed = vim.getTailersMinLsn();
        assertTrue("IS must contribute a floor", contributed.isPresent());
        // computeTailersRetentionFloor() would short-circuit here and return START_OF_TIME.
        assertEquals(LogSequenceNumber.START_OF_TIME, contributed.get());
        LogSequenceNumber tailersFloor = contributed.get(); // = START_OF_TIME = (-1,-1)

        // Step 2: set up a FileCommitLog with several ledgers.
        Path base = folder.newFolder().toPath();
        LogSequenceNumber checkpoint;
        List<Long> before;
        try (FileCommitLogManager manager = newManager(base)) {
            manager.start();
            try (FileCommitLog log = manager.createCommitLog("ts", "ts", "n1")) {
                log.startWriting(1);
                writeEntry(log);
                writeEntry(log);
                checkpoint = writeEntry(log);
                writeEntry(log);
                writeEntry(log);
                before = listLogLedgers(tsFolder(base));
                assertTrue("need multiple ledgers to exercise drop logic; got " + before,
                        before.size() >= 3);

                // Step 3: simulate what TableSpaceManager.checkpoint() does —
                // pass the tailersFloor (START_OF_TIME from frozen IS) to dropOldLedgers.
                log.dropOldLedgers(checkpoint, tailersFloor);
            }

            List<Long> after = listLogLedgers(tsFolder(base));

            // Step 4: assert the correct post-fix behaviour.
            // BUG (issue #355): currently FAILS because START_OF_TIME is treated as
            // "no constraint" → ledgers below checkpoint are deleted even though
            // the IS tailer is frozen and cannot yet read them.
            assertEquals(
                    "No ledger must be dropped when tailersFloor=START_OF_TIME "
                            + "(IS present but frozen in Phase A, issue #355). "
                            + "before=" + before + " after=" + after,
                    before.size(), after.size());
        }
    }
}
