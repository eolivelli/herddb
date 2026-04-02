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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.log.LogEntry;
import herddb.log.LogEntryType;
import herddb.log.LogSequenceNumber;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataOutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileCommitLogTailerTest {

    private static final byte ENTRY_START = 13;
    private static final byte ENTRY_END = 25;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Writes a txlog segment file with the given entries in the FileCommitLog binary format.
     */
    private void writeTxlogFile(Path file, long ledgerId, long startOffset, int count) throws IOException {
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(file)))) {
            for (int i = 0; i < count; i++) {
                long seqNumber = startOffset + i;
                LogEntry entry = new LogEntry(System.currentTimeMillis(), LogEntryType.INSERT,
                        0, "mytable", Bytes.from_string("key" + i), Bytes.from_string("val" + i));
                out.writeByte(ENTRY_START);
                out.writeLong(seqNumber);
                entry.serialize(out);
                out.writeByte(ENTRY_END);
            }
        }
    }

    /**
     * Reproduces the original bug: the log directory contains a .txlog subdirectory
     * (per-tablespace), and inside it are the actual segment files.
     * Before the fix, the tailer tried to open the directory as a file.
     */
    @Test
    public void testNestedTxlogDirectoryStructure() throws Exception {
        Path logDir = folder.newFolder("txlog").toPath();

        // Create the per-tablespace subdirectory (this is what FileCommitLog creates)
        Path tablespaceDir = logDir.resolve("1e429577d5d94dc287f91de568d1a0c9.txlog");
        Files.createDirectory(tablespaceDir);

        // Write two segment files inside
        writeTxlogFile(tablespaceDir.resolve("0000000000000001.txlog"), 1, 1, 3);
        writeTxlogFile(tablespaceDir.resolve("0000000000000002.txlog"), 2, 1, 2);

        List<LogSequenceNumber> consumed = new ArrayList<>();
        try (FileCommitLogTailer tailer = new FileCommitLogTailer(logDir, null, LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> consumed.add(lsn))) {
            // Run a single scan (not the polling loop)
            Thread t = new Thread(tailer);
            t.start();

            // Wait for entries to be consumed
            long deadline = System.currentTimeMillis() + 5000;
            while (consumed.size() < 5 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            tailer.close();
            t.join(5000);
        }

        assertEquals(5, consumed.size());
        // Entries from the first segment file
        assertEquals(1, consumed.get(0).ledgerId);
        assertEquals(1, consumed.get(0).offset);
        assertEquals(1, consumed.get(1).ledgerId);
        assertEquals(2, consumed.get(1).offset);
        assertEquals(1, consumed.get(2).ledgerId);
        assertEquals(3, consumed.get(2).offset);
        // Entries from the second segment file
        assertEquals(2, consumed.get(3).ledgerId);
        assertEquals(1, consumed.get(3).offset);
        assertEquals(2, consumed.get(4).ledgerId);
        assertEquals(2, consumed.get(4).offset);
    }

    /**
     * Tests that the tailer handles multiple tablespace subdirectories.
     * Segment files from different subdirectories are sorted by filename and
     * processed in order with the global watermark advancing.
     */
    @Test
    public void testMultipleTablespaceDirectories() throws Exception {
        Path logDir = folder.newFolder("txlog-multi").toPath();

        Path ts1 = logDir.resolve("aaaa.txlog");
        Path ts2 = logDir.resolve("bbbb.txlog");
        Files.createDirectory(ts1);
        Files.createDirectory(ts2);

        // Use different ledger IDs so watermark doesn't cause skipping across tablespaces
        writeTxlogFile(ts1.resolve("0000000000000001.txlog"), 1, 1, 2);
        writeTxlogFile(ts2.resolve("0000000000000002.txlog"), 2, 1, 3);

        List<LogSequenceNumber> consumed = new ArrayList<>();
        try (FileCommitLogTailer tailer = new FileCommitLogTailer(logDir, null, LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> consumed.add(lsn))) {
            Thread t = new Thread(tailer);
            t.start();

            long deadline = System.currentTimeMillis() + 5000;
            while (consumed.size() < 5 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            tailer.close();
            t.join(5000);
        }

        assertEquals(5, consumed.size());
    }

    /**
     * Tests that the tailer returns nothing for an empty directory.
     */
    @Test
    public void testEmptyLogDirectory() throws Exception {
        Path logDir = folder.newFolder("txlog-empty").toPath();

        List<LogSequenceNumber> consumed = new ArrayList<>();
        try (FileCommitLogTailer tailer = new FileCommitLogTailer(logDir, null, LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> consumed.add(lsn))) {
            Thread t = new Thread(tailer);
            t.start();

            // Give it a couple of poll cycles
            Thread.sleep(600);
            tailer.close();
            t.join(5000);
        }

        assertTrue(consumed.isEmpty());
    }

    /**
     * Tests that the tailer skips entries at or before the watermark.
     */
    @Test
    public void testWatermarkSkipsOldEntries() throws Exception {
        Path logDir = folder.newFolder("txlog-watermark").toPath();

        Path tablespaceDir = logDir.resolve("abcd1234.txlog");
        Files.createDirectory(tablespaceDir);

        writeTxlogFile(tablespaceDir.resolve("0000000000000001.txlog"), 1, 1, 5);

        // Start from watermark at ledger 1, offset 3 — should skip entries 1,2,3
        LogSequenceNumber watermark = new LogSequenceNumber(1, 3);
        List<LogSequenceNumber> consumed = new ArrayList<>();
        try (FileCommitLogTailer tailer = new FileCommitLogTailer(logDir, null, watermark,
                (lsn, entry) -> consumed.add(lsn))) {
            Thread t = new Thread(tailer);
            t.start();

            long deadline = System.currentTimeMillis() + 5000;
            while (consumed.size() < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            tailer.close();
            t.join(5000);
        }

        assertEquals(2, consumed.size());
        assertEquals(4, consumed.get(0).offset);
        assertEquals(5, consumed.get(1).offset);
    }

    /**
     * Tests that a non-existent log directory does not cause errors.
     */
    @Test
    public void testNonExistentDirectory() throws Exception {
        Path logDir = folder.getRoot().toPath().resolve("does-not-exist");

        List<LogSequenceNumber> consumed = new ArrayList<>();
        try (FileCommitLogTailer tailer = new FileCommitLogTailer(logDir, null, LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> consumed.add(lsn))) {
            Thread t = new Thread(tailer);
            t.start();

            Thread.sleep(600);
            tailer.close();
            t.join(5000);
        }

        assertTrue(consumed.isEmpty());
    }

    /**
     * Appends entries to an existing txlog file (simulating a writer still appending).
     */
    private void appendTxlogEntries(Path file, long ledgerId, long startOffset, int count) throws IOException {
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(file, StandardOpenOption.APPEND)))) {
            for (int i = 0; i < count; i++) {
                long seqNumber = startOffset + i;
                LogEntry entry = new LogEntry(System.currentTimeMillis(), LogEntryType.INSERT,
                        0, "mytable", Bytes.from_string("key" + seqNumber), Bytes.from_string("val" + seqNumber));
                out.writeByte(ENTRY_START);
                out.writeLong(seqNumber);
                entry.serialize(out);
                out.writeByte(ENTRY_END);
            }
        }
    }

    /**
     * Tests that the tailer sees entries appended to the active file without re-reading
     * from the start. The tailable reader stays open and picks up new data on subsequent polls.
     */
    @Test
    public void testTailActiveFileSeesAppendedEntries() throws Exception {
        Path logDir = folder.newFolder("txlog-tail").toPath();

        Path tablespaceDir = logDir.resolve("abcd.txlog");
        Files.createDirectory(tablespaceDir);
        Path segmentFile = tablespaceDir.resolve("0000000000000001.txlog");

        // Write initial entries
        writeTxlogFile(segmentFile, 1, 1, 3);

        List<LogSequenceNumber> consumed = Collections.synchronizedList(new ArrayList<>());
        try (FileCommitLogTailer tailer = new FileCommitLogTailer(logDir, null, LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> consumed.add(lsn))) {
            Thread t = new Thread(tailer);
            t.start();

            // Wait for initial 3 entries
            long deadline = System.currentTimeMillis() + 5000;
            while (consumed.size() < 3 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(3, consumed.size());

            // Append 2 more entries to the same file
            appendTxlogEntries(segmentFile, 1, 4, 2);

            // Wait for the appended entries to be consumed
            deadline = System.currentTimeMillis() + 5000;
            while (consumed.size() < 5 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }

            tailer.close();
            t.join(5000);
        }

        assertEquals(5, consumed.size());
        assertEquals(1, consumed.get(0).offset);
        assertEquals(2, consumed.get(1).offset);
        assertEquals(3, consumed.get(2).offset);
        assertEquals(4, consumed.get(3).offset);
        assertEquals(5, consumed.get(4).offset);
    }

    /**
     * Tests that when the writer rolls to a new segment file, the tailer finishes
     * reading the old file and transitions to the new one.
     */
    @Test
    public void testTailActiveFileRollsToNewSegment() throws Exception {
        Path logDir = folder.newFolder("txlog-roll").toPath();

        Path tablespaceDir = logDir.resolve("abcd.txlog");
        Files.createDirectory(tablespaceDir);
        Path segment1 = tablespaceDir.resolve("0000000000000001.txlog");

        // Write initial entries to segment 1
        writeTxlogFile(segment1, 1, 1, 3);

        List<LogSequenceNumber> consumed = Collections.synchronizedList(new ArrayList<>());
        try (FileCommitLogTailer tailer = new FileCommitLogTailer(logDir, null, LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> consumed.add(lsn))) {
            Thread t = new Thread(tailer);
            t.start();

            // Wait for initial 3 entries
            long deadline = System.currentTimeMillis() + 5000;
            while (consumed.size() < 3 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(3, consumed.size());

            // Simulate writer rolling: create a new segment file
            Path segment2 = tablespaceDir.resolve("0000000000000002.txlog");
            writeTxlogFile(segment2, 2, 1, 2);

            // Wait for the new segment's entries
            deadline = System.currentTimeMillis() + 5000;
            while (consumed.size() < 5 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }

            tailer.close();
            t.join(5000);
        }

        assertEquals(5, consumed.size());
        // First 3 from segment 1
        assertEquals(1, consumed.get(0).ledgerId);
        assertEquals(1, consumed.get(1).ledgerId);
        assertEquals(1, consumed.get(2).ledgerId);
        // Last 2 from segment 2
        assertEquals(2, consumed.get(3).ledgerId);
        assertEquals(2, consumed.get(4).ledgerId);
    }
}
