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

import static herddb.file.FileCommitLog.ENTRY_END;
import static herddb.file.FileCommitLog.ENTRY_START;
import static herddb.utils.TestUtils.NOOP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.file.FileCommitLog.CommitFileReader;
import herddb.file.FileCommitLog.LogEntryWithSequenceNumber;
import herddb.log.CommitLog;
import herddb.log.CommitLogResult;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.server.ServerConfiguration;
import herddb.utils.TestUtils;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic Tests on FileCommitLog
 *
 * @author enrico.olivelli
 */
public class FileCommitLogTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testLog() throws Exception {
        try (FileCommitLogManager manager = new FileCommitLogManager(folder.newFolder().toPath())) {
            manager.start();
            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.startWriting(1);
                for (int i = 0; i < 10_000; i++) {
                    log.log(LogEntryFactory.beginTransaction(0), false);
                    writeCount++;
                }
            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");
        }
    }

    @Test
    public void testDiskFullLogMissingFooter() throws Exception {
        try (FileCommitLogManager manager = new FileCommitLogManager(folder.newFolder().toPath(),
                ServerConfiguration.PROPERTY_MAX_LOG_FILE_SIZE_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_BYTES_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_SYNC_TIME_DEFAULT,
                ServerConfiguration.PROPERTY_REQUIRE_FSYNC_DEFAULT,
                false, // do not use O_DIRECT, we are creating a broken file, O_DIRECT will add padding at unpredictable points
                ServerConfiguration.PROPERTY_DEFERRED_SYNC_PERIOD_DEFAULT,
                NullStatsLogger.INSTANCE)) {
            manager.start();
            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.startWriting(1);
                for (int i = 0; i < 100; i++) {
                    log.log(LogEntryFactory.beginTransaction(0), true).getLogSequenceNumber();
                    writeCount++;
                }
                FileCommitLog fileCommitLog = (FileCommitLog) log;

                // simulate end of disk
                byte[] dummyEntry = LogEntryFactory.beginTransaction(0).serialize();
                // header
                fileCommitLog.getWriter().out.write(ENTRY_START);
                fileCommitLog.getWriter().out.writeLong(0);
                // entry
                fileCommitLog.getWriter().out.write(dummyEntry);
                // missing entry footer
                fileCommitLog.getWriter().out.flush();

            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");

            // must be able to read twice
            AtomicInteger readCount2 = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount2.incrementAndGet();
                    }
                }, true);
            }
            assertEquals(writeCount, readCount.get());
        }
    }

    @Test
    public void testDiskFullLogBrokenEntry() throws Exception {
        try (FileCommitLogManager manager = new FileCommitLogManager(folder.newFolder().toPath(),
                ServerConfiguration.PROPERTY_MAX_LOG_FILE_SIZE_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_BYTES_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_SYNC_TIME_DEFAULT,
                ServerConfiguration.PROPERTY_REQUIRE_FSYNC_DEFAULT,
                false, // do not use O_DIRECT, we are creating a broken file, O_DIRECT will add padding at unpredictable points
                ServerConfiguration.PROPERTY_DEFERRED_SYNC_PERIOD_DEFAULT,
                NullStatsLogger.INSTANCE)) {
            manager.start();
            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.startWriting(1);
                for (int i = 0; i < 100; i++) {
                    log.log(LogEntryFactory.beginTransaction(0), true).getLogSequenceNumber();
                    writeCount++;
                }
                FileCommitLog fileCommitLog = (FileCommitLog) log;

                // simulate end of disk
                byte[] dummyEntry = LogEntryFactory.beginTransaction(0).serialize();
                // header
                fileCommitLog.getWriter().out.write(ENTRY_START);
                fileCommitLog.getWriter().out.writeLong(0);
                // just half entry
                fileCommitLog.getWriter().out.write(dummyEntry, 0, dummyEntry.length / 2);
                // missing entry footer
                fileCommitLog.getWriter().out.flush();

            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");

            // must be able to read twice
            AtomicInteger readCount2 = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount2.incrementAndGet();
                    }
                }, true);
            }
            assertEquals(writeCount, readCount.get());
        }
    }

    @Test
    public void testLogsynch() throws Exception {
        try (FileCommitLogManager manager = new FileCommitLogManager(folder.newFolder().toPath())) {
            manager.start();
            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.startWriting(1);
                for (int i = 0; i < 100; i++) {
                    log.log(LogEntryFactory.beginTransaction(0), true);
                    writeCount++;
                }
            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");
        }
    }

    @Test
    public void testLogMultiFiles() throws Exception {
        TestStatsProvider testStatsProvider = new TestStatsProvider();
        TestStatsProvider.TestStatsLogger statsLogger = testStatsProvider.getStatsLogger("test");

        try (FileCommitLogManager manager = new FileCommitLogManager(
                folder.newFolder().toPath(),
                1024 * 2, // 2K Bbyte files,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_BYTES_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_SYNC_TIME_DEFAULT,
                false,
                false, /* O_DIRECT */
                ServerConfiguration.PROPERTY_DEFERRED_SYNC_PERIOD_DEFAULT,
                statsLogger)) {
            manager.start();

            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (FileCommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.startWriting(1);
                for (int i = 0; i < 10_000; i++) {
                    log.log(LogEntryFactory.beginTransaction(0), false);
                    writeCount++;
                }
                TestUtils.waitForCondition(() -> {
                    int qsize = log.getQueueSize();
                    return qsize == 0;
                }, TestUtils.NOOP, 100);
            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");

            // this number really depends on disk format
            // this test in the future will be updated when we change the format
            assertEquals(145L, statsLogger.scope("aa").getCounter("newfiles").get().longValue());
        }
    }


    @Test
    public void testLogMultiFiles_O_DIRECT() throws Exception {
        TestStatsProvider testStatsProvider = new TestStatsProvider();
        TestStatsProvider.TestStatsLogger statsLogger = testStatsProvider.getStatsLogger("test");

        try (FileCommitLogManager manager = new FileCommitLogManager(
                folder.newFolder().toPath(),
                1024 * 2, // 2K Bbyte files,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_BYTES_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_SYNC_TIME_DEFAULT,
                false,
                true, /* O_DIRECT */
                ServerConfiguration.PROPERTY_DEFERRED_SYNC_PERIOD_DEFAULT,
                statsLogger)) {
            manager.start();

            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (FileCommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.startWriting(1);
                for (int i = 0; i < 10_000; i++) {
                    log.log(LogEntryFactory.beginTransaction(0), false);
                    writeCount++;
                }
                TestUtils.waitForCondition(() -> {
                    int qsize = log.getQueueSize();
                    return qsize == 0;
                }, TestUtils.NOOP, 100);
            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");

            // this number really depends on disk format
            // this test in the future will be updated when we change the format
            assertEquals(145L, statsLogger.scope("aa").getCounter("newfiles").get().longValue());
        }
    }

    @Test
    public void testMaxSyncTime() throws Exception {
        TestStatsProvider testStatsProvider = new TestStatsProvider();
        TestStatsProvider.TestStatsLogger statsLogger = testStatsProvider.getStatsLogger("test");

        try (FileCommitLogManager manager = new FileCommitLogManager(
                folder.newFolder().toPath(),
                ServerConfiguration.PROPERTY_MAX_LOG_FILE_SIZE_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_BYTES_DEFAULT,
                1, // 1ms
                true /* require fsync */,
                false, /* O_DIRECT */
                ServerConfiguration.PROPERTY_DEFERRED_SYNC_PERIOD_DEFAULT,
                statsLogger)) {
            manager.start();

            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (FileCommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.startWriting(1);
                log.log(LogEntryFactory.beginTransaction(0), true).getLogSequenceNumber();
                writeCount = 1;
            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");
        }
    }

    @Test
    public void testMaxBatchSize() throws Exception {
        TestStatsProvider testStatsProvider = new TestStatsProvider();
        TestStatsProvider.TestStatsLogger statsLogger = testStatsProvider.getStatsLogger("test");

        try (FileCommitLogManager manager = new FileCommitLogManager(
                folder.newFolder().toPath(),
                ServerConfiguration.PROPERTY_MAX_LOG_FILE_SIZE_DEFAULT,
                2, // flush only when we have 2 entries in the queue
                Integer.MAX_VALUE, // no flush by size
                Integer.MAX_VALUE, // no flush by time
                true /* require fsync */,
                false, /* O_DIRECT */
                ServerConfiguration.PROPERTY_DEFERRED_SYNC_PERIOD_DEFAULT,
                statsLogger)) {
            manager.start();

            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (FileCommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.startWriting(1);
                CopyOnWriteArrayList<LogSequenceNumber> completed = new CopyOnWriteArrayList<>();

                CommitLogResult future = log.log(LogEntryFactory.beginTransaction(0), true);
                future.logSequenceNumber.thenAccept(completed::add);
                assertFalse(future.logSequenceNumber.isDone());

                CommitLogResult future2 = log.log(LogEntryFactory.beginTransaction(0), true);
                future2.logSequenceNumber.thenAccept(completed::add);

                future.logSequenceNumber.get(10, TimeUnit.SECONDS);
                future2.logSequenceNumber.get(10, TimeUnit.SECONDS);

                TestUtils.waitForCondition(() -> {
                    return completed.size() == 2;
                }, NOOP, 100);

                writeCount = completed.size();

                assertTrue(completed.get(1).after(completed.get(0)));
            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");
        }
    }

    @Test
    public void testMaxBatchSizeBytes() throws Exception {
        TestStatsProvider testStatsProvider = new TestStatsProvider();
        TestStatsProvider.TestStatsLogger statsLogger = testStatsProvider.getStatsLogger("test");

        try (FileCommitLogManager manager = new FileCommitLogManager(
                folder.newFolder().toPath(),
                ServerConfiguration.PROPERTY_MAX_LOG_FILE_SIZE_DEFAULT,
                Integer.MAX_VALUE, // no flush by batch size
                LogEntryFactory.beginTransaction(0).serialize().length * 2 - 1, // flush after 2 writes
                Integer.MAX_VALUE, // no flush by time
                true /* require fsync */,
                false, /* O_DIRECT */
                ServerConfiguration.PROPERTY_DEFERRED_SYNC_PERIOD_DEFAULT,
                statsLogger)) {
            manager.start();

            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (FileCommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.startWriting(1);
                CopyOnWriteArrayList<LogSequenceNumber> completed = new CopyOnWriteArrayList<>();

                CommitLogResult future = log.log(LogEntryFactory.beginTransaction(0), true);
                future.logSequenceNumber.thenAccept(completed::add);
                assertFalse(future.logSequenceNumber.isDone());

                CommitLogResult future2 = log.log(LogEntryFactory.beginTransaction(0), true);
                future2.logSequenceNumber.thenAccept(completed::add);

                future.logSequenceNumber.get(10, TimeUnit.SECONDS);
                future2.logSequenceNumber.get(10, TimeUnit.SECONDS);

                TestUtils.waitForCondition(() -> {
                    return completed.size() == 2;
                }, NOOP, 100);

                writeCount = completed.size();

                assertTrue(completed.get(1).after(completed.get(0)));
            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");
        }
    }

    @Test
    public void testDeferredSync() throws Exception {
        TestStatsProvider testStatsProvider = new TestStatsProvider();
        TestStatsProvider.TestStatsLogger statsLogger = testStatsProvider.getStatsLogger("test");
        try (FileCommitLogManager manager = new FileCommitLogManager(
                folder.newFolder().toPath(),
                ServerConfiguration.PROPERTY_MAX_LOG_FILE_SIZE_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_BYTES_DEFAULT,
                1,
                false /* require fsync */,
                false, /* O_DIRECT */
                1, // each second
                statsLogger)) {

            manager.start();

            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (FileCommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.startWriting(1);
                for (int i = 0; i < 10_000; i++) {
                    log.log(LogEntryFactory.beginTransaction(0), false);
                    writeCount++;
                }
                Counter deferredSyncs = statsLogger.scope("aa").getCounter("deferredSyncs");

                TestUtils.waitForCondition(() -> {
                    int qsize = log.getQueueSize();
                    Long _deferredSyncs = deferredSyncs.get();
                    return qsize == 0 && _deferredSyncs != null && _deferredSyncs > 0;
                }, TestUtils.NOOP, 100);
            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid")) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");
        }
    }

    /**
     * Helper: writes one complete entry (ENTRY_START + seqNumber + serialized LogEntry + ENTRY_END).
     */
    private static void writeCompleteEntry(OutputStream out, long seqNumber, LogEntry entry) throws Exception {
        byte[] serialized = entry.serialize();
        out.write(ENTRY_START);
        // write seqNumber as 8 big-endian bytes
        for (int i = 56; i >= 0; i -= 8) {
            out.write((int) (seqNumber >>> i) & 0xFF);
        }
        out.write(serialized);
        out.write(ENTRY_END);
    }

    /**
     * Verify that TailableFileInputStream mark/reset works correctly through
     * the ExtendedDataInputStream/DataInputStream wrapper chain used by CommitFileReader.
     */
    @Test
    public void testTailableReaderMarkResetThroughWrapper() throws Exception {
        Path txlogFile = folder.newFile("0000000000000010.txlog").toPath();

        LogEntry entry1 = LogEntryFactory.beginTransaction(0);

        // Write one complete entry
        try (OutputStream out = Files.newOutputStream(txlogFile, StandardOpenOption.WRITE)) {
            writeCompleteEntry(out, 1, entry1);
            out.flush();
        }

        try (CommitFileReader reader = CommitFileReader.openForTailing(txlogFile)) {
            // Read the one complete entry
            LogEntryWithSequenceNumber result1 = reader.nextEntry();
            assertNotNull(result1);
            assertEquals(1, result1.logSequenceNumber.offset);

            // EOF — no more entries
            LogEntryWithSequenceNumber result2 = reader.nextEntry();
            assertNull(result2);

            // Append a second complete entry
            LogEntry entry2 = LogEntryFactory.beginTransaction(1);
            try (OutputStream out = Files.newOutputStream(txlogFile, StandardOpenOption.APPEND)) {
                writeCompleteEntry(out, 2, entry2);
                out.flush();
            }

            // Reader should see the newly appended entry
            LogEntryWithSequenceNumber result3 = reader.nextEntry();
            assertNotNull("tailable reader should see appended entry", result3);
            assertEquals(2, result3.logSequenceNumber.offset);
        }
    }

    /**
     * When tailing, a partial entry (missing footer) should not corrupt the reader.
     * The next call to nextEntry() after the writer completes the entry should succeed.
     */
    @Test
    public void testTailableReaderRewindsOnPartialEntry() throws Exception {
        Path txlogFile = folder.newFile("0000000000000001.txlog").toPath();

        LogEntry entry1 = LogEntryFactory.beginTransaction(0);
        LogEntry entry2 = LogEntryFactory.beginTransaction(1);
        byte[] entry2Bytes = entry2.serialize();

        // Write one complete entry and a partial second entry (header + half payload, no footer)
        try (OutputStream out = Files.newOutputStream(txlogFile, StandardOpenOption.WRITE)) {
            writeCompleteEntry(out, 1, entry1);
            // partial entry: ENTRY_START + seqNumber + half of the serialized entry
            out.write(ENTRY_START);
            for (int i = 56; i >= 0; i -= 8) {
                out.write((int) (2L >>> i) & 0xFF);
            }
            out.write(entry2Bytes, 0, entry2Bytes.length / 2);
            out.flush();
        }

        try (CommitFileReader reader = CommitFileReader.openForTailing(txlogFile)) {
            // First entry reads fine
            LogEntryWithSequenceNumber result1 = reader.nextEntry();
            assertNotNull(result1);
            assertEquals(1, result1.logSequenceNumber.offset);

            // Second entry is incomplete — should return null (not throw)
            LogEntryWithSequenceNumber result2 = reader.nextEntry();
            assertNull(result2);

            // Now the writer finishes the entry — append the rest of the payload + footer
            try (OutputStream out = Files.newOutputStream(txlogFile, StandardOpenOption.APPEND)) {
                out.write(entry2Bytes, entry2Bytes.length / 2, entry2Bytes.length - entry2Bytes.length / 2);
                out.write(ENTRY_END);
                out.flush();
            }

            // The tailable reader should now successfully read the complete entry
            LogEntryWithSequenceNumber result3 = reader.nextEntry();
            assertNotNull("tailable reader should re-read the entry after rewind", result3);
            assertEquals(2, result3.logSequenceNumber.offset);
        }
    }

    /**
     * When tailing, if the file has only the ENTRY_START byte of a new entry (no payload at all),
     * the reader should rewind and retry successfully once data appears.
     */
    @Test
    public void testTailableReaderRewindsOnEntryStartOnly() throws Exception {
        Path txlogFile = folder.newFile("0000000000000002.txlog").toPath();

        LogEntry entry1 = LogEntryFactory.beginTransaction(0);

        // Write one complete entry followed by just ENTRY_START
        try (OutputStream out = Files.newOutputStream(txlogFile, StandardOpenOption.WRITE)) {
            writeCompleteEntry(out, 1, entry1);
            out.write(ENTRY_START);
            out.flush();
        }

        try (CommitFileReader reader = CommitFileReader.openForTailing(txlogFile)) {
            // First entry reads fine
            LogEntryWithSequenceNumber result1 = reader.nextEntry();
            assertNotNull(result1);
            assertEquals(1, result1.logSequenceNumber.offset);

            // Only ENTRY_START byte written — should return null
            LogEntryWithSequenceNumber result2 = reader.nextEntry();
            assertNull(result2);

            // Now the writer finishes writing the full second entry
            LogEntry entry2 = LogEntryFactory.beginTransaction(1);
            try (OutputStream out = Files.newOutputStream(txlogFile, StandardOpenOption.APPEND)) {
                byte[] entry2Bytes = entry2.serialize();
                // write the remaining bytes: seqNumber + payload + footer
                for (int i = 56; i >= 0; i -= 8) {
                    out.write((int) (2L >>> i) & 0xFF);
                }
                out.write(entry2Bytes);
                out.write(ENTRY_END);
                out.flush();
            }

            // Reader should now read the complete entry
            LogEntryWithSequenceNumber result3 = reader.nextEntry();
            assertNotNull("tailable reader should re-read entry after rewind", result3);
            assertEquals(2, result3.logSequenceNumber.offset);
        }
    }

    /**
     * Non-tailable reader (openForDescribeRawfile) should still skip truncated entries
     * without throwing, preserving the original behavior.
     */
    @Test
    public void testNonTailableReaderSkipsTruncatedEntry() throws Exception {
        Path txlogFile = folder.newFile("0000000000000003.txlog").toPath();

        LogEntry entry1 = LogEntryFactory.beginTransaction(0);
        byte[] entry2Bytes = LogEntryFactory.beginTransaction(1).serialize();

        // Write one complete entry and a truncated second entry
        try (OutputStream out = Files.newOutputStream(txlogFile, StandardOpenOption.WRITE)) {
            writeCompleteEntry(out, 1, entry1);
            out.write(ENTRY_START);
            for (int i = 56; i >= 0; i -= 8) {
                out.write((int) (2L >>> i) & 0xFF);
            }
            out.write(entry2Bytes, 0, entry2Bytes.length / 2);
            out.flush();
        }

        try (CommitFileReader reader = CommitFileReader.openForDescribeRawfile(txlogFile)) {
            // First entry reads fine
            LogEntryWithSequenceNumber result1 = reader.nextEntry();
            assertNotNull(result1);
            assertEquals(1, result1.logSequenceNumber.offset);

            // Truncated entry — should return null (original skip behavior)
            LogEntryWithSequenceNumber result2 = reader.nextEntry();
            assertNull(result2);

            // No more entries
            LogEntryWithSequenceNumber result3 = reader.nextEntry();
            assertNull(result3);
        }
    }
}
