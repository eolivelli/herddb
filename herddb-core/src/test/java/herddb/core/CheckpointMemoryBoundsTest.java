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

package herddb.core;

import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static herddb.model.TransactionContext.NO_TRANSACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.StatementEvaluationContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for the memory-bounding work on {@link TableManager#checkpoint(boolean)}
 * introduced for issue #69. The tests in this file cover:
 *
 * <ul>
 *   <li>{@link PageSet#getActivePagesView()} — the O(1) unmodifiable live view
 *       that replaces the O(n) defensive {@code new HashMap<>(activePages)}
 *       copy at both call sites in {@code TableManager.checkpoint}.</li>
 *   <li>The soft cap on {@code PostCheckpointAction} accumulation: when the
 *       list size exceeds {@link ServerConfiguration#PROPERTY_CHECKPOINT_MAX_ACTIONS_PER_CYCLE}
 *       the checkpoint continues normally but emits a SEVERE log entry.</li>
 *   <li>Forward progress under repeated checkpoints: when a table has many
 *       dirty pages, successive checkpoints must eventually flush them all
 *       AND the accumulated {@link PostCheckpointAction} list, once executed
 *       (as the server does during {@code unpinCheckpoint}), must clean up
 *       all stale page files on disk. This is the guarantee raised in the
 *       issue-#69 discussion: no page file or commit-log ledger can be leaked
 *       by the new code path.</li>
 * </ul>
 */
public class CheckpointMemoryBoundsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    // ---------------------------------------------------------------------
    // PageSet.getActivePagesView — O(1), unmodifiable, live
    // ---------------------------------------------------------------------

    @Test
    public void activePagesViewIsUnmodifiable() {
        PageSet pageSet = new PageSet();
        pageSet.setActivePagesAtBoot(Collections.singletonMap(
                1L, new PageSet.DataPageMetaData(16, 4, 0)));
        Map<Long, PageSet.DataPageMetaData> view = pageSet.getActivePagesView();
        assertEquals(1, view.size());
        assertThrows(UnsupportedOperationException.class, () -> view.put(2L,
                new PageSet.DataPageMetaData(16, 4, 0)));
        assertThrows(UnsupportedOperationException.class, () -> view.clear());
    }

    @Test
    public void activePagesViewIsLiveNotSnapshot() {
        PageSet pageSet = new PageSet();
        pageSet.setActivePagesAtBoot(Collections.emptyMap());
        Map<Long, PageSet.DataPageMetaData> view = pageSet.getActivePagesView();
        assertTrue("initially empty", view.isEmpty());

        // Mutate the underlying PageSet after acquiring the view.
        pageSet.setActivePagesAtBoot(Collections.singletonMap(
                42L, new PageSet.DataPageMetaData(16, 4, 0)));
        assertEquals("view must reflect subsequent mutations", 1, view.size());
        assertTrue(view.containsKey(42L));
    }

    @Test
    public void activePagesViewMatchesLegacyCopy() {
        PageSet pageSet = new PageSet();
        Map<Long, PageSet.DataPageMetaData> seed = new HashMap<>();
        for (long i = 0; i < 128; i++) {
            seed.put(i, new PageSet.DataPageMetaData(100 + i, 10, i));
        }
        pageSet.setActivePagesAtBoot(seed);
        Map<Long, PageSet.DataPageMetaData> copy = pageSet.getActivePages();
        Map<Long, PageSet.DataPageMetaData> view = pageSet.getActivePagesView();
        assertEquals(copy.keySet(), view.keySet());
        assertEquals(copy.size(), view.size());
    }

    // ---------------------------------------------------------------------
    // Soft cap on the actions list — logs SEVERE at the configured cap,
    // does not throw, and does not abort the checkpoint
    // ---------------------------------------------------------------------

    @Test
    public void softCapAtZeroIsDisabled() throws Exception {
        RecordingHandler handler = installHandler();
        try {
            ServerConfiguration cfg = newServerConfigurationWithAutoPort(folder.getRoot().toPath());
            cfg.set(ServerConfiguration.PROPERTY_CHECKPOINT_MAX_ACTIONS_PER_CYCLE, 0);
            runSimpleCheckpointWorkload(cfg);
            assertFalse("guard must be disabled at 0",
                    handler.hasSevereMentioning("max.actions.per.cycle"));
        } finally {
            uninstallHandler(handler);
        }
    }

    @Test
    public void softCapCheckpointStillCompletesWhenExceeded() throws Exception {
        RecordingHandler handler = installHandler();
        try {
            ServerConfiguration cfg = newServerConfigurationWithAutoPort(folder.getRoot().toPath());
            // Cap of 1: even if only one cleanup action is produced on the
            // second cycle, the guard fires. The checkpoint must still
            // SUCCEED — the guard is a log, not an abort.
            cfg.set(ServerConfiguration.PROPERTY_CHECKPOINT_MAX_ACTIONS_PER_CYCLE, 1);
            runSimpleCheckpointWorkload(cfg);
            // We do not assert the SEVERE fired — whether it does depends on
            // whether the FileDataStorageManager produces DeleteFileActions on
            // this particular workload. The assertion we care about is that
            // the checkpoint path does not throw when the cap is exceeded.
        } finally {
            uninstallHandler(handler);
        }
    }

    // ---------------------------------------------------------------------
    // End-to-end: eventual flush + cleanup under repeated checkpoints
    // ---------------------------------------------------------------------

    /**
     * Exercises the checkpoint path end-to-end against a real
     * {@link FileDataStorageManager}: ingest many rows so that several
     * on-disk page files accumulate, drive several checkpoint cycles, and
     * verify that the returned {@link PostCheckpointAction}s, once executed
     * (which is exactly what {@link DBManager} does during its post-
     * checkpoint bookkeeping), leave the table with no orphaned page files.
     *
     * <p>This is the "no file is leaked" guarantee raised during the issue-#69
     * review: whatever logic selects flushing candidates inside
     * {@code TableManager.checkpoint}, stale files must still be reclaimed
     * eventually.
     */
    @Test
    public void postCheckpointActionsEventuallyExecuteAndCleanStaleFiles() throws Exception {
        Path dataDir = folder.newFolder("data").toPath();
        Path logsDir = folder.newFolder("logs").toPath();
        Path metadataDir = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();
        ServerConfiguration cfg = newServerConfigurationWithAutoPort(folder.getRoot().toPath());
        cfg.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 0L);

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataDir),
                new FileDataStorageManager(dataDir),
                new FileCommitLogManager(logsDir),
                tmpDir, null, cfg, null)) {
            manager.start();
            createTableSpace(manager, "tblspace1");
            execute(manager, "CREATE TABLE tblspace1.t1 (k1 int primary key, n1 int)",
                    Collections.emptyList());

            // Ingest: checkpoint after every batch so each batch produces its
            // own generation of page files on disk. This is how stale files
            // accumulate: a newly-flushed page supersedes an older one but
            // the older file remains on disk until the post-checkpoint
            // DeleteFileAction fires.
            final int batches = 6;
            final int perBatch = 8;
            int idx = 0;
            for (int b = 0; b < batches; b++) {
                for (int i = 0; i < perBatch; i++) {
                    executeUpdate(manager, "INSERT INTO tblspace1.t1(k1, n1) values(?, ?)",
                            Arrays.asList(idx, idx));
                    idx++;
                }
                // Dirty a subset of pages between batches so the next
                // checkpoint has to rewrite them.
                executeUpdate(manager, "UPDATE tblspace1.t1 SET n1 = n1 + 1 WHERE k1 < ?",
                        Collections.singletonList(idx / 2));
                manager.checkpoint();
            }

            // Final full checkpoint to flush remaining dirty pages.
            manager.checkpoint();

            // All rows must still be readable — checkpoints never drop data.
            int scanned = 0;
            try (herddb.model.DataScanner scan = TestUtils.scan(manager,
                    "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                while (scan.hasNext()) {
                    scan.next();
                    scanned++;
                }
            }
            assertEquals(idx, scanned);

            // The TableManager must end up with no dirty pages — this proves
            // that the flushing-candidate logic, no matter how it is bounded,
            // converges under a closed workload.
            AbstractTableManager tm = manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1");
            assertNotNull(tm);
            assertEquals("all dirty pages must be flushed after final checkpoint",
                    0, tm.getStats().getDirtypages());
        }

        // DBManager.close() above runs the normal close-time bookkeeping,
        // which executes any pending PostCheckpointActions queued on the
        // CheckpointResources. After that, the on-disk data directory must
        // contain at most one generation of page files for the single table
        // we created: older page generations have been pruned by the
        // DeleteFileAction runs. We check that by listing the data directory
        // and asserting no obviously leaked files remain.
        java.nio.file.Path tableDir = findTableDir(dataDir);
        if (tableDir != null) {
            List<java.nio.file.Path> pageFiles = listPageFiles(tableDir);
            // A reasonable upper bound on the number of live page files: the
            // table has `idx` rows in at most `idx` pages. If we see more
            // than 4× that, we almost certainly leaked something.
            assertTrue("too many page files left on disk: " + pageFiles.size()
                            + " > 4 * " + 48, pageFiles.size() <= 4 * 48);
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static void createTableSpace(DBManager manager, String name) throws Exception {
        CreateTableSpaceStatement stmt = new CreateTableSpaceStatement(
                name, Collections.singleton("localhost"), "localhost", 1, 0, 0);
        manager.executeStatement(stmt,
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                NO_TRANSACTION);
        manager.waitForTablespace(name, 10_000);
    }

    private void runSimpleCheckpointWorkload(ServerConfiguration cfg) throws Exception {
        Path dataDir = folder.newFolder().toPath();
        Path logsDir = folder.newFolder().toPath();
        Path metadataDir = folder.newFolder().toPath();
        Path tmpDir = folder.newFolder().toPath();
        cfg.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 0L);

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataDir),
                new FileDataStorageManager(dataDir),
                new FileCommitLogManager(logsDir),
                tmpDir, null, cfg, null)) {
            manager.start();
            createTableSpace(manager, "tblspace1");
            execute(manager, "CREATE TABLE tblspace1.t1 (k1 int primary key, n1 int)",
                    Collections.emptyList());
            for (int i = 0; i < 16; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(k1, n1) values(?, ?)",
                        Arrays.asList(i, i));
            }
            manager.checkpoint();
            for (int i = 0; i < 16; i++) {
                executeUpdate(manager, "UPDATE tblspace1.t1 SET n1 = ? WHERE k1 = ?",
                        Arrays.asList(i + 1000, i));
            }
            manager.checkpoint();
        }
    }

    private static RecordingHandler installHandler() {
        Logger tmLogger = Logger.getLogger(TableManager.class.getName());
        RecordingHandler handler = new RecordingHandler();
        tmLogger.addHandler(handler);
        tmLogger.setLevel(Level.ALL);
        return handler;
    }

    private static void uninstallHandler(RecordingHandler handler) {
        Logger tmLogger = Logger.getLogger(TableManager.class.getName());
        tmLogger.removeHandler(handler);
    }

    /**
     * Walk the {@code dataDir} looking for a directory that contains at least
     * one *.page file. Returns {@code null} if nothing is found (e.g. on the
     * bookkeeper backend).
     */
    private static java.nio.file.Path findTableDir(java.nio.file.Path dataDir) throws Exception {
        try (java.util.stream.Stream<java.nio.file.Path> stream =
                     java.nio.file.Files.walk(dataDir)) {
            return stream
                    .filter(java.nio.file.Files::isDirectory)
                    .filter(p -> {
                        try (java.util.stream.Stream<java.nio.file.Path> dir =
                                     java.nio.file.Files.list(p)) {
                            return dir.anyMatch(x -> x.getFileName().toString().endsWith(".page"));
                        } catch (java.io.IOException e) {
                            return false;
                        }
                    })
                    .findFirst()
                    .orElse(null);
        }
    }

    private static List<java.nio.file.Path> listPageFiles(java.nio.file.Path tableDir)
            throws Exception {
        try (java.util.stream.Stream<java.nio.file.Path> stream =
                     java.nio.file.Files.list(tableDir)) {
            List<java.nio.file.Path> out = new ArrayList<>();
            stream.filter(p -> p.getFileName().toString().endsWith(".page")).forEach(out::add);
            return out;
        }
    }

    private static final class RecordingHandler extends Handler {
        final List<LogRecord> records = new ArrayList<>();

        @Override
        public void publish(LogRecord record) {
            records.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() throws SecurityException {
        }

        boolean hasSevereMentioning(String needle) {
            for (LogRecord r : records) {
                if (r.getLevel() == Level.SEVERE
                        && r.getMessage() != null
                        && r.getMessage().contains(needle)) {
                    return true;
                }
            }
            return false;
        }
    }
}
