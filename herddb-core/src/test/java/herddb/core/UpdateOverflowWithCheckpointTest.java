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
import static herddb.core.TestUtils.scan;
import static herddb.core.TestUtils.scanKeepReadLocks;
import static herddb.model.TransactionContext.NO_TRANSACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import herddb.utils.DataAccessor;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Targeted test for the DataPage.put() overflow race condition.
 * <p>
 * Uses very small pages to maximize the chance that an UPDATE causes
 * a page overflow (the record is too big for the current page after
 * the value changes size). This exercises the code path where
 * DataPage.put() must restore the old record on failure.
 * <p>
 * Runs concurrent reads and checkpoints to trigger the race window
 * where keyToPage points to a page whose record was transiently lost.
 */
public class UpdateOverflowWithCheckpointTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Stress test: concurrent updates with varying value sizes on tiny pages
     * plus frequent checkpoints and reads. Uses auto-transactions to ensure
     * proper record-level locking (matching the pattern of the original
     * MultipleConcurrentUpdatesTest). This maximizes page overflow events.
     */
    @Test
    public void testUpdateOverflowWithConcurrentReadsAndCheckpoints() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmpDir").toPath();

        ServerConfiguration config = newServerConfigurationWithAutoPort();
        // Very small pages to force frequent overflow during updates
        config.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 2 * 1024);
        // Small memory budget to force page evictions
        config.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, 64 * 1024);
        config.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, 512 * 1024);
        // Frequent checkpoints via periodic thread
        config.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 1000);

        int numRecords = 200;
        int numOperations = 1000;
        int numThreads = 10;

        ConcurrentHashMap<String, String> expectedValues = new ConcurrentHashMap<>();
        AtomicReference<Throwable> error = new AtomicReference<>();

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, config, null)) {

            manager.start();
            CreateTableSpaceStatement st = new CreateTableSpaceStatement("tblspace1",
                    Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            // Use a string column with varying sizes to trigger overflow
            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, v1 string)",
                    Collections.emptyList());

            // Insert initial records with small values
            long tx = TestUtils.beginTransaction(manager, "tblspace1");
            for (int i = 0; i < numRecords; i++) {
                String key = "k_" + String.format("%04d", i);
                String value = "small_" + i;
                executeUpdate(manager, "INSERT INTO tblspace1.t1(k1,v1) values(?,?)",
                        Arrays.asList(key, value), new TransactionContext(tx));
                expectedValues.put(key, value);
            }
            TestUtils.commitTransaction(manager, "tblspace1", tx);
            manager.checkpoint();

            // Run concurrent updates (varying sizes) and reads
            ExecutorService pool = Executors.newFixedThreadPool(numThreads);

            // Worker threads: mix of updates and reads
            List<Future<?>> workers = new java.util.ArrayList<>();
            for (int i = 0; i < numOperations; i++) {
                workers.add(pool.submit(() -> {
                    if (error.get() != null) {
                        return;
                    }
                    try {
                        ThreadLocalRandom rng = ThreadLocalRandom.current();
                        int idx = rng.nextInt(numRecords);
                        String key = "k_" + String.format("%04d", idx);

                        // Remove from expected to claim exclusive access
                        String prev = expectedValues.remove(key);
                        if (prev == null) {
                            return; // another thread is working on this key
                        }

                        try {
                            if (rng.nextBoolean()) {
                                // UPDATE with varying value size to trigger page overflow.
                                // Use auto-transaction for proper record-level locking.
                                int valueSize = rng.nextInt(10, 500);
                                StringBuilder sb = new StringBuilder(valueSize);
                                for (int c = 0; c < valueSize; c++) {
                                    sb.append((char) ('a' + rng.nextInt(26)));
                                }
                                String newValue = sb.toString();

                                long utx = TestUtils.beginTransaction(manager, "tblspace1");
                                executeUpdate(manager, "UPDATE tblspace1.t1 set v1=? where k1=?",
                                        Arrays.asList(newValue, key), new TransactionContext(utx));
                                TestUtils.commitTransaction(manager, "tblspace1", utx);
                                expectedValues.put(key, newValue);
                            } else {
                                // READ — this is the operation that fails if DataPage.put lost the old record
                                try (DataScanner scanner = scanKeepReadLocks(manager,
                                        "SELECT v1 FROM tblspace1.t1 where k1=?",
                                        Arrays.asList(key), NO_TRANSACTION)) {
                                    List<DataAccessor> rows = scanner.consume();
                                    assertEquals("Record must be found for key " + key, 1, rows.size());
                                    assertNotNull("Value must not be null for key " + key,
                                            rows.get(0).get("v1"));
                                }
                                expectedValues.put(key, prev);
                            }
                        } catch (Exception e) {
                            // Put back on error so other threads can proceed
                            expectedValues.put(key, prev);
                            throw e;
                        }
                    } catch (Exception e) {
                        error.compareAndSet(null, e);
                    }
                }));
            }

            // Wait for all workers
            for (Future<?> f : workers) {
                f.get(120, TimeUnit.SECONDS);
            }

            pool.shutdown();
            pool.awaitTermination(30, TimeUnit.SECONDS);

            // Check for errors
            if (error.get() != null) {
                throw new AssertionError("Concurrent operation failed", error.get());
            }

            // Final consistency check: every record must be readable
            for (int i = 0; i < numRecords; i++) {
                String key = "k_" + String.format("%04d", i);
                try (DataScanner scanner = scan(manager,
                        "SELECT v1 FROM tblspace1.t1 where k1=?",
                        Arrays.asList(key))) {
                    List<DataAccessor> rows = scanner.consume();
                    assertEquals("Record must exist for key " + key, 1, rows.size());
                }
            }
        }
    }

    /**
     * Targeted test: force page overflow during update by growing value
     * from tiny to large, then verify the record is still readable.
     * No concurrency — purely validates the DataPage.put restore path
     * through the SQL layer.
     */
    @Test
    public void testUpdateGrowingValueTriggerOverflow() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmpDir").toPath();

        ServerConfiguration config = newServerConfigurationWithAutoPort();
        // Tiny pages: ~1KB
        config.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 1024);
        config.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, 32 * 1024);
        config.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, 256 * 1024);
        config.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 0); // manual checkpoints

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, config, null)) {

            manager.start();
            CreateTableSpaceStatement st = new CreateTableSpaceStatement("tblspace1",
                    Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, v1 string)",
                    Collections.emptyList());

            // Fill a page with small records
            int numRecords = 10;
            for (int i = 0; i < numRecords; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(k1,v1) values(?,?)",
                        Arrays.asList("key_" + i, "tiny"), NO_TRANSACTION);
            }

            manager.checkpoint();

            // Now update each record with a much larger value.
            // This should trigger DataPage.put overflow for in-place updates
            // on the mutable page, forcing the record to move to a new page.
            String largeValue = new String(new char[500]).replace('\0', 'X');
            for (int i = 0; i < numRecords; i++) {
                String key = "key_" + i;
                executeUpdate(manager, "UPDATE tblspace1.t1 set v1=? where k1=?",
                        Arrays.asList(largeValue, key), NO_TRANSACTION);

                // Immediately verify the record is readable
                try (DataScanner scanner = scan(manager,
                        "SELECT v1 FROM tblspace1.t1 where k1=?",
                        Arrays.asList(key))) {
                    List<DataAccessor> rows = scanner.consume();
                    assertEquals("Record must exist after update for " + key, 1, rows.size());
                    assertEquals("Value must be the large value", largeValue, rows.get(0).get("v1").toString());
                }
            }

            manager.checkpoint();

            // Verify all records after checkpoint
            for (int i = 0; i < numRecords; i++) {
                String key = "key_" + i;
                try (DataScanner scanner = scan(manager,
                        "SELECT v1 FROM tblspace1.t1 where k1=?",
                        Arrays.asList(key))) {
                    List<DataAccessor> rows = scanner.consume();
                    assertEquals(1, rows.size());
                    assertEquals(largeValue, rows.get(0).get("v1").toString());
                }
            }
        }
    }
}
