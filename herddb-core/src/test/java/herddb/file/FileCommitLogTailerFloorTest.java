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

package herddb.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.server.ServerConfiguration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Exercises the tailers-floor contract of {@link FileCommitLog#dropOldLedgers}.
 * Rollover is triggered by setting {@code maxLogFileSize} very small so every
 * entry starts a new ledger file.
 */
public class FileCommitLogTailerFloorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private FileCommitLogManager newManager(Path base) {
        return new FileCommitLogManager(base,
                /* maxLogFileSize */ 1L,
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

    private List<Long> listLogLedgers(Path logDirectory) throws java.io.IOException {
        List<Long> ids = new ArrayList<>();
        try (java.util.stream.Stream<Path> stream = Files.list(logDirectory)) {
            for (Path p : (Iterable<Path>) stream::iterator) {
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

    /**
     * Without a tailers floor ({@code null}), deletion is governed solely by the
     * checkpoint LSN — baseline behavior. Ledgers below the checkpoint are deleted;
     * the last file is always retained.
     *
     * <p>Note: before issue #355 was fixed, {@code START_OF_TIME} was used here to
     * mean "no constraint."  After the fix {@code null} is the "no external tailer"
     * signal; {@code START_OF_TIME} now means "IS present but frozen — protect all."
     */
    @Test
    public void nullFloorBehavesLikeNoConstraint() throws Exception {
        Path base = folder.newFolder().toPath();
        LogSequenceNumber checkpoint;
        try (FileCommitLogManager manager = newManager(base)) {
            manager.start();
            try (FileCommitLog log = manager.createCommitLog("ts", "ts", "n1")) {
                log.startWriting(1);
                writeEntry(log);
                writeEntry(log);
                checkpoint = writeEntry(log);
                writeEntry(log);
                writeEntry(log);
                List<Long> before = listLogLedgers(tsFolder(base));
                assertTrue("expected multiple ledgers to be created, got " + before,
                        before.size() >= 3);
                // null = no external tailer constraint (no vector index in this tablespace).
                log.dropOldLedgers(checkpoint, null);
            }
            List<Long> after = listLogLedgers(tsFolder(base));
            for (long id : after) {
                assertTrue("ledger " + id + " below checkpoint " + checkpoint.ledgerId
                                + " should have been dropped",
                        id >= checkpoint.ledgerId || id == after.get(after.size() - 1));
            }
        }
    }

    /**
     * A tailers floor below the checkpoint must pin retention at the floor: ledgers
     * the tailer has not yet processed stay on disk even though the checkpoint LSN
     * alone would allow dropping them.
     */
    @Test
    public void tailersFloorPinsRetentionBelowCheckpoint() throws Exception {
        Path base = folder.newFolder().toPath();
        LogSequenceNumber oldLsn;
        LogSequenceNumber checkpoint;
        try (FileCommitLogManager manager = newManager(base)) {
            manager.start();
            try (FileCommitLog log = manager.createCommitLog("ts", "ts", "n1")) {
                log.startWriting(1);
                oldLsn = writeEntry(log);
                writeEntry(log);
                writeEntry(log);
                checkpoint = writeEntry(log);
                writeEntry(log);
                writeEntry(log);
                log.dropOldLedgers(checkpoint, oldLsn);
            }
            List<Long> after = listLogLedgers(tsFolder(base));
            assertTrue("ledger " + oldLsn.ledgerId + " must be kept (tailer still needs it); got " + after,
                    after.contains(oldLsn.ledgerId));
        }
    }

    /**
     * Issue #355 regression test.
     *
     * <p>When a remote IndexingService (IS) is present but its tailer is frozen
     * in Phase A compaction, {@code VectorIndexManager.getTailersMinLsn()} returns
     * {@code Optional.of(START_OF_TIME)} (either because {@code getMinProcessedLsn}
     * received a gRPC response with {@code lastLsnLedger=-1, lastLsnOffset=-1}, or
     * because the gRPC call timed out while Phase A was consuming all CPU).
     *
     * <p>{@code computeTailersRetentionFloor()} short-circuits to {@code START_OF_TIME}
     * in this case and passes it to {@code dropOldLedgers}.  The intent of that
     * short-circuit is "a tailer we can't safely pin — protect everything."  However
     * the guard in {@code dropOldLedgers} is:
     * <pre>
     *     if (tailersFloor != null {@code &&} tailersFloor.ledgerId &gt;= 0 ...)
     * </pre>
     * {@code START_OF_TIME.ledgerId == -1 &lt; 0}, so the guard is <em>never entered</em>
     * and the log files are dropped as if no floor existed — destroying commit-log
     * ledgers the frozen IS still needs.
     *
     * <p>This test asserts the <em>correct post-fix behaviour</em>: when the tailers
     * floor is {@code START_OF_TIME} (IS present but frozen), <strong>no</strong>
     * ledger file must be deleted.  The test currently <em>fails</em>, thereby
     * reproducing the bug.
     */
    @Test
    public void issue355FrozenIsAtStartOfTimePreventsLedgerDrop() throws Exception {
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
                assertTrue("expected multiple ledgers before drop, got " + before,
                        before.size() >= 3);

                // Simulate the IS being frozen in Phase A: computeTailersRetentionFloor()
                // returns START_OF_TIME. The floor should prevent ANY ledger from being
                // dropped because the IS tailer position is unknown / at the very beginning.
                log.dropOldLedgers(checkpoint, LogSequenceNumber.START_OF_TIME);
            }
            List<Long> after = listLogLedgers(tsFolder(base));
            // BUG (issue #355): currently dropOldLedgers treats START_OF_TIME as
            // "no constraint" and drops ledgers below the checkpoint.
            // After the fix START_OF_TIME must mean "IS present but frozen — keep all".
            assertEquals(
                    "No ledger must be dropped when the IS tailer floor is START_OF_TIME "
                            + "(frozen IS, issue #355); before=" + before + " after=" + after,
                    before.size(), after.size());
        }
    }

    /**
     * Recovery must tolerate older log files that were kept on disk because a
     * tailer pinned them: the recovery loop reads forward from the checkpoint
     * ledger, so the extra older files are silently ignored.
     */
    @Test
    public void recoverySkipsPiledUpOldLedgers() throws Exception {
        Path base = folder.newFolder().toPath();
        LogSequenceNumber oldLsn;
        LogSequenceNumber checkpoint;
        try (FileCommitLogManager manager = newManager(base)) {
            manager.start();
            try (FileCommitLog log = manager.createCommitLog("ts", "ts", "n1")) {
                log.startWriting(1);
                oldLsn = writeEntry(log);
                writeEntry(log);
                checkpoint = writeEntry(log);
                writeEntry(log);
                log.dropOldLedgers(checkpoint, oldLsn);
            }
        }
        try (FileCommitLogManager manager = newManager(base)) {
            manager.start();
            try (FileCommitLog log = manager.createCommitLog("ts", "ts", "n1")) {
                AtomicInteger applied = new AtomicInteger();
                log.recovery(checkpoint, (lsn, entry) -> {
                    applied.incrementAndGet();
                    assertTrue("recovery must not replay entries below checkpoint LSN (got " + lsn + ")",
                            lsn.ledgerId >= checkpoint.ledgerId);
                }, false);
                // Recovery completed without throwing and without replaying any
                // ledger below the checkpoint — the piled old ledgers are silently
                // skipped.
            }
        }
    }
}
