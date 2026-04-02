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
import static org.junit.Assert.fail;
import herddb.indexing.IndexingServiceEngine.IndexStatusInfo;
import herddb.log.LogEntry;
import herddb.log.LogEntryType;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.TableSpace;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataOutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Reproduces and verifies the fix for the bug where {@code waitForCatchUp} blocks
 * forever after a commit log ledger rollover. When the commit log rolls to a new
 * ledger, {@code getLastSequenceNumber()} returns {@code (newLedgerId, -1)} — a phantom
 * LSN with no corresponding entry. The tailer processes all real entries but can never
 * reach this phantom LSN, causing {@code waitForCatchUp} to loop indefinitely.
 */
public class WaitForCatchUpAfterLedgerRolloverTest {

    private static final byte ENTRY_START = 13;
    private static final byte ENTRY_END = 25;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Writes a txlog segment file with the given entries in FileCommitLog binary format.
     */
    private void writeTxlogFile(Path file, long ledgerId, long startOffset, int count) throws IOException {
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(file)))) {
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
     * Demonstrates the bug: waitForCatchUp with a phantom LSN (newLedgerId, -1)
     * blocks forever because the tailer can never reach a LSN in a ledger that
     * has no entries.
     *
     * Then demonstrates the fix: using getLastWrittenSequenceNumber() as the target
     * instead of getLastSequenceNumber() gives a reachable LSN and waitForCatchUp
     * completes immediately.
     */
    @Test
    public void testWaitForCatchUpWithPhantomLsnBlocksForever() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        // Register a tablespace with a known UUID
        String tsUUID = "tablespace1";
        MemoryMetadataStorageManager metadataManager = new MemoryMetadataStorageManager();
        metadataManager.start();
        metadataManager.registerTableSpace(TableSpace.builder()
                .name(TableSpace.DEFAULT).uuid(tsUUID)
                .leader("local").replica("local").build());

        // Create per-tablespace subdirectory (mimics FileCommitLog directory structure)
        Path tablespaceDir = logDir.resolve(tsUUID + ".txlog");
        Files.createDirectory(tablespaceDir);

        // Write 5 entries to ledger 1
        writeTxlogFile(tablespaceDir.resolve("0000000000000001.txlog"), 1, 1, 5);

        // Create an EMPTY ledger 2 file — simulates the state after openNewLedger()
        // created the file but no entries have been written yet
        Files.createFile(tablespaceDir.resolve("0000000000000002.txlog"));

        try (EmbeddedIndexingService eis = new EmbeddedIndexingService(logDir, dataDir)) {
            eis.setMetadataStorageManager(metadataManager);
            eis.start();

            // Wait for tailer to consume all 5 entries from ledger 1
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                IndexStatusInfo status = eis.getEngine().getIndexStatus("tablespace1", "", "");
                if (status.getLastLsnLedger() == 1 && status.getLastLsnOffset() == 5) {
                    break;
                }
                Thread.sleep(50);
            }

            IndexStatusInfo status = eis.getEngine().getIndexStatus("tablespace1", "", "");
            assertEquals("Tailer should have processed up to ledger 1",
                    1, status.getLastLsnLedger());
            assertEquals("Tailer should have processed up to offset 5",
                    5, status.getLastLsnOffset());

            // The phantom LSN that getLastSequenceNumber() would return after ledger rollover
            LogSequenceNumber phantomLsn = new LogSequenceNumber(2, -1);

            // Demonstrate the bug: waitForCatchUp with phantom LSN should NOT complete
            // because the tailer is at (1, 5) and can never reach (2, -1) — different ledger
            try (IndexingServiceClient client = eis.createClient()) {
                CompletableFuture<Void> catchUpFuture = CompletableFuture.runAsync(() -> {
                    try {
                        client.waitForCatchUp("tablespace1", phantomLsn);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

                // This should time out — the bug means it would never complete
                try {
                    catchUpFuture.get(3, TimeUnit.SECONDS);
                    fail("waitForCatchUp with phantom LSN (2, -1) should have timed out, "
                            + "but it completed. This means the tailer unexpectedly advanced past ledger 2.");
                } catch (TimeoutException expected) {
                    // Expected: waitForCatchUp is stuck because phantom LSN is unreachable
                    catchUpFuture.cancel(true);
                }

                // Demonstrate the fix: the correct target LSN is the last REAL entry (1, 5)
                // This is what getLastWrittenSequenceNumber() would return instead of (2, -1)
                LogSequenceNumber correctLsn = new LogSequenceNumber(1, 5);

                CompletableFuture<Void> fixedFuture = CompletableFuture.runAsync(() -> {
                    try {
                        client.waitForCatchUp("tablespace1", correctLsn);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

                // This should complete immediately — the tailer is already at (1, 5)
                fixedFuture.get(5, TimeUnit.SECONDS);
            }
        }
    }
}
