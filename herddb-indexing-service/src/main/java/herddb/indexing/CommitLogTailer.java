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

package herddb.indexing;

import herddb.file.FileCommitLog;
import herddb.file.FileCommitLog.CommitFileReader;
import herddb.file.FileCommitLog.LogEntryWithSequenceNumber;
import herddb.log.LogEntry;
import herddb.log.LogSequenceNumber;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads FileCommitLog {@code .txlog} files from a directory and dispatches entries to a consumer.
 * Continuously polls for new files/entries, skipping anything at or before the watermark LSN.
 * <p>
 * The last (active) file is kept open between poll cycles using a tailable reader,
 * so that newly appended entries are seen without re-reading from the start.
 * When a newer file appears in the directory (indicating the writer has rolled),
 * the active reader is drained and closed.
 *
 * @author enrico.olivelli
 */
public class CommitLogTailer implements Runnable, AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(CommitLogTailer.class.getName());
    private static final String TXLOG_EXTENSION = ".txlog";
    private static final long POLL_INTERVAL_MS = 500;

    private final Path logDirectory;
    private volatile LogSequenceNumber watermark;
    private final EntryConsumer consumer;
    private volatile boolean running = true;

    // Active reader state: kept open on the last (active) file between poll cycles
    private CommitFileReader activeReader;
    private Path activeReaderPath;
    private long entriesProcessed;

    /**
     * Functional interface for consuming log entries.
     */
    @FunctionalInterface
    public interface EntryConsumer {
        void accept(LogSequenceNumber lsn, LogEntry entry);
    }

    public CommitLogTailer(Path logDirectory, LogSequenceNumber startFrom, EntryConsumer consumer) {
        this.logDirectory = logDirectory;
        this.watermark = startFrom;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        LOGGER.info("CommitLogTailer starting, logDir=" + logDirectory + ", watermark=" + watermark);
        long pollCount = 0;
        long totalEntriesProcessed = 0;
        while (running) {
            try {
                pollCount++;
                long entriesBefore = entriesProcessed;
                boolean processedAny = scanAndProcess();
                long entriesThisPoll = entriesProcessed - entriesBefore;
                if (processedAny) {
                    totalEntriesProcessed += entriesThisPoll;
                    LOGGER.info("Poll #" + pollCount + ": processed " + entriesThisPoll
                            + " entries (total=" + totalEntriesProcessed + "), watermark=" + watermark);
                } else {
                    LOGGER.log(Level.FINE, "Poll #{0}: no new entries, watermark={1}",
                            new Object[]{pollCount, watermark});
                    Thread.sleep(POLL_INTERVAL_MS);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOGGER.info("CommitLogTailer interrupted, stopping");
                break;
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error in CommitLogTailer", e);
                closeActiveReader();
                try {
                    Thread.sleep(POLL_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        closeActiveReader();
        LOGGER.info("CommitLogTailer stopped");
    }

    /**
     * Scans the log directory for .txlog files and processes entries beyond the watermark.
     *
     * @return true if at least one entry was processed
     */
    private boolean scanAndProcess() throws IOException {
        List<Path> txlogFiles = listTxlogFiles();
        if (txlogFiles.isEmpty()) {
            LOGGER.log(Level.INFO, "No txlog files found in {0}", logDirectory);
            return false;
        }
        LOGGER.log(Level.FINE, "Found {0} txlog file(s) in {1}", new Object[]{txlogFiles.size(), logDirectory});
        boolean processedAny = false;
        Path lastFile = txlogFiles.get(txlogFiles.size() - 1);

        for (Path txlogFile : txlogFiles) {
            if (!running) {
                break;
            }
            boolean isLastFile = txlogFile.equals(lastFile);
            boolean processed;
            if (isLastFile) {
                processed = processActiveFile(txlogFile);
            } else {
                processed = processCompletedFile(txlogFile);
            }
            if (processed) {
                processedAny = true;
            }
        }
        return processedAny;
    }

    /**
     * Processes a completed (non-active) file. Opens with a standard reader and closes after EOF.
     * If the active reader was on this file (writer rolled since last poll), drains it first.
     */
    private boolean processCompletedFile(Path txlogFile) throws IOException {
        // If the active reader is on this file, the writer has rolled.
        // Drain remaining entries from the tailable reader, then close it.
        if (activeReaderPath != null && activeReaderPath.equals(txlogFile)) {
            boolean processed = drainReader(activeReader);
            closeActiveReader();
            return processed;
        }

        // Skip entire file if watermark is past it
        long fileLedgerId = extractLedgerId(txlogFile);
        if (!watermark.isStartOfTime() && fileLedgerId < watermark.ledgerId) {
            return false;
        }

        boolean processedAny = false;
        try (CommitFileReader reader = CommitFileReader.openForDescribeRawfile(txlogFile)) {
            processedAny = drainReader(reader);
        }
        return processedAny;
    }

    /**
     * Processes the active (last) file. Keeps the reader open between poll cycles
     * so newly appended entries are visible without re-reading from the start.
     */
    private boolean processActiveFile(Path txlogFile) throws IOException {
        // If the active reader is on a different file, close it
        if (activeReaderPath != null && !activeReaderPath.equals(txlogFile)) {
            // Drain remaining entries from the old active file before closing
            drainReader(activeReader);
            closeActiveReader();
        }

        // Skip entire file if watermark is past it
        long fileLedgerId = extractLedgerId(txlogFile);
        if (!watermark.isStartOfTime() && fileLedgerId < watermark.ledgerId) {
            return false;
        }

        // Open a tailable reader if not already open
        if (activeReader == null) {
            LOGGER.info("Opening tailable reader on active file: " + txlogFile.getFileName());
            activeReader = CommitFileReader.openForTailing(txlogFile);
            activeReaderPath = txlogFile;
        }

        return drainReader(activeReader);
    }

    /**
     * Reads all available entries from the reader, dispatching those after the watermark.
     */
    private boolean drainReader(CommitFileReader reader) throws IOException {
        boolean processedAny = false;
        LogEntryWithSequenceNumber entry;
        while (running && (entry = reader.nextEntry()) != null) {
            LogSequenceNumber lsn = entry.logSequenceNumber;
            if (!watermark.isStartOfTime() && !lsn.after(watermark)) {
                continue;
            }
            consumer.accept(lsn, entry.entry);
            watermark = lsn;
            processedAny = true;
            entriesProcessed++;
        }
        return processedAny;
    }

    private long extractLedgerId(Path txlogFile) {
        String fileName = txlogFile.getFileName().toString().replace(TXLOG_EXTENSION, "");
        try {
            return Long.valueOf(fileName, 16);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private void closeActiveReader() {
        if (activeReader != null) {
            try {
                activeReader.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing active reader", e);
            }
            activeReader = null;
            activeReaderPath = null;
        }
    }

    private List<Path> listTxlogFiles() throws IOException {
        List<Path> files = new ArrayList<>();
        if (!Files.isDirectory(logDirectory)) {
            return files;
        }
        // The log directory contains per-tablespace subdirectories named <ledgerId>.txlog,
        // each containing the actual segment files (e.g. 0000000000000001.txlog).
        // We need to scan inside each subdirectory for regular .txlog files.
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(logDirectory, "*" + TXLOG_EXTENSION)) {
            for (Path path : stream) {
                if (Files.isDirectory(path)) {
                    try (DirectoryStream<Path> inner = Files.newDirectoryStream(path, "*" + TXLOG_EXTENSION)) {
                        for (Path segment : inner) {
                            if (Files.isRegularFile(segment)) {
                                files.add(segment);
                            }
                        }
                    }
                } else if (Files.isRegularFile(path)) {
                    files.add(path);
                }
            }
        }
        // Sort by filename (hex ledger IDs) to ensure processing order
        Collections.sort(files, (a, b) -> a.getFileName().toString().compareTo(b.getFileName().toString()));
        return files;
    }

    /**
     * Returns the current watermark (last processed LSN).
     */
    public LogSequenceNumber getWatermark() {
        return watermark;
    }

    /**
     * Returns the total number of entries processed since this tailer was created.
     */
    public long getEntriesProcessed() {
        return entriesProcessed;
    }

    /**
     * Returns whether the tailer is currently running.
     */
    public boolean isRunning() {
        return running;
    }

    @Override
    public void close() {
        running = false;
    }
}
