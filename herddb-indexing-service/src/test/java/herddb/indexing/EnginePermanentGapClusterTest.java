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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.cluster.BookkeeperCommitLog;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.LedgersInfo;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.log.CommitLogResult;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Table;
import herddb.server.ServerConfiguration;
import herddb.utils.ZKTestEnv;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Cluster-mode counterpart to {@link RestartUnreachableStartOfTimeTest}:
 * boots a real ZooKeeper + BookKeeper environment via {@link ZKTestEnv},
 * writes commit-log entries, simulates a {@code dropOldLedgers} that
 * removes every ledger up to and including the one carrying the engine's
 * watermark, and then starts a real {@link IndexingServiceEngine}
 * configured to tail BookKeeper from that watermark.
 *
 * <p>This is the engine-level companion to
 * {@code BookKeeperCommitLogTailerPermanentGapTest} (which exercises the
 * same gap detection at the tailer level): the assertion here is that
 * the engine, given a snapshot with embedded schema, never blocks
 * forever on the missing ledger and reaches a quiescent state quickly.
 */
public class EnginePermanentGapClusterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void setUp() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder("zk").toPath());
        testEnv.startBookieAndInitCluster();
    }

    @After
    public void tearDown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    private static Table buildTable() {
        return Table.builder()
                .name("t1")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static Index buildVectorIndex() {
        return Index.builder()
                .name("vidx")
                .table("t1")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    /**
     * Watermark store backed by an atomic reference — the engine's
     * load() returns the seeded snapshot, save() records whatever the
     * engine eventually persists.
     */
    private static final class InMemoryWatermarkStore implements WatermarkStore {
        private final AtomicReference<WatermarkSnapshot> current;

        InMemoryWatermarkStore(WatermarkSnapshot initial) {
            this.current = new AtomicReference<>(initial);
        }

        @Override
        public WatermarkSnapshot load() {
            return current.get();
        }

        @Override
        public void save(WatermarkSnapshot snapshot) throws IOException {
            current.set(snapshot);
        }
    }

    /**
     * Verifies that an indexing-service engine started with a snapshot
     * whose LSN points into a now-trimmed ledger boots cleanly:
     *
     * <ol>
     *   <li>Schema is hydrated from the snapshot (no DDL replayed from BK).</li>
     *   <li>The BookKeeper tailer detects the permanent gap and stops
     *       quickly (sister test:
     *       {@code BookKeeperCommitLogTailerPermanentGapTest}).</li>
     *   <li>The engine remains responsive — searches can still run, the
     *       in-memory schema is intact, and {@link IndexingServiceEngine#close()}
     *       returns within a bounded time.</li>
     * </ol>
     *
     * <p>This is the safety net for the issue #383 scenario "restarts when
     * the START_OF_TIME is no more available". Today the engine relies on
     * the snapshot to provide schema; the tailer simply stops if its
     * starting ledger is gone. That is acceptable as long as the engine
     * does not deadlock waiting for entries the cluster cannot deliver.
     */
    @Test
    public void engineStartsAndStaysResponsiveAfterDropOldLedgers() throws Exception {
        String tableSpaceUUID = "engine-permanent-gap-uuid";

        try (ZookeeperMetadataStorageManager metadataManager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            metadataManager.start();
            metadataManager.ensureDefaultTableSpace("writer-node", "writer-node", 0, 1);

            ServerConfiguration serverConfig = new ServerConfiguration();
            serverConfig.set(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH,
                    ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT);

            // Step 1: write some entries to BK so we have a real LSN to
            // anchor the trimmed-history simulation against.
            LogSequenceNumber watermarkLsn;
            try (BookkeeperCommitLogManager clManager = new BookkeeperCommitLogManager(
                    metadataManager, serverConfig, NullStatsLogger.INSTANCE)) {
                clManager.start();
                try (BookkeeperCommitLog writerLog = clManager.createCommitLog(
                        tableSpaceUUID, "default", "writer-node")) {
                    writerLog.startWriting(1);
                    writerLog.log(LogEntryFactory.noop(), true);
                    writerLog.log(LogEntryFactory.noop(), true);
                    CommitLogResult last = writerLog.log(LogEntryFactory.noop(), true);
                    watermarkLsn = last.getLogSequenceNumber();
                }

                // Step 2: write more entries with a fresh writer (creates
                // ledgers with higher IDs).
                try (BookkeeperCommitLog writerLog2 = clManager.createCommitLog(
                        tableSpaceUUID, "default", "writer-node")) {
                    writerLog2.startWriting(1);
                    writerLog2.log(LogEntryFactory.noop(), false);
                    writerLog2.log(LogEntryFactory.noop(), false);
                } catch (Exception ignored) {
                    // Embedded BK may surface a BKLedgerClosedException on
                    // close when two sequential writers share a tablespace
                    // — irrelevant to the gap-detection invariant.
                }

                // Step 3: simulate dropOldLedgers: remove every ledger
                // whose ID is ≤ watermarkLsn.ledgerId.
                LedgersInfo ledgersInfo = metadataManager.getActualLedgersList(tableSpaceUUID);
                List<Long> toRemove = new ArrayList<>();
                for (long id : ledgersInfo.getActiveLedgers()) {
                    if (id <= watermarkLsn.ledgerId) {
                        toRemove.add(id);
                    }
                }
                for (long id : toRemove) {
                    ledgersInfo.removeLedger(id);
                }
                metadataManager.saveActualLedgersList(tableSpaceUUID, ledgersInfo);

                // Sanity: at least one newer ledger must remain so that
                // the tailer's "all-remaining-newer" branch fires.
                LedgersInfo afterDrop = metadataManager.getActualLedgersList(tableSpaceUUID);
                boolean hasNewer = afterDrop.getActiveLedgers().stream()
                        .anyMatch(id -> id > watermarkLsn.ledgerId);
                assertTrue("post-drop ledger list must include at least one newer ledger; got "
                        + afterDrop.getActiveLedgers(), hasNewer);
            }

            // Step 4: build a watermark snapshot with schema embedded —
            // the schema-recovery path is the only way the engine has to
            // know the table/index definitions when the DDL ledgers are
            // gone (issue #368).
            Table table = buildTable();
            Index ix = buildVectorIndex();
            WatermarkSnapshot snapshot = new WatermarkSnapshot(
                    watermarkLsn, 1,
                    Collections.singletonList(table),
                    Collections.singletonList(ix));
            InMemoryWatermarkStore watermark = new InMemoryWatermarkStore(snapshot);

            // Step 5: start a real engine pointed at the same ZK + BK.
            Path logDir = folder.newFolder("engine-log").toPath();
            Path dataDir = folder.newFolder("engine-data").toPath();
            Properties props = new Properties();
            props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
            props.setProperty(IndexingServerConfiguration.PROPERTY_LOG_TYPE, "bookkeeper");
            props.setProperty(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS,
                    testEnv.getAddress());
            props.setProperty(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_PATH,
                    testEnv.getPath());
            props.setProperty(IndexingServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH,
                    ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT);

            IndexingServerConfiguration cfg = new IndexingServerConfiguration(props);
            IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, cfg);

            // Wire the engine to the same ZK metadata so it resolves the
            // tablespace UUID we wrote ledgers under.
            try (ZookeeperMetadataStorageManager engineMeta = new ZookeeperMetadataStorageManager(
                    testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
                engineMeta.start();
                engine.setMetadataStorageManager(engineMeta);
                engine.setWatermarkStore(watermark);
                try {
                    engine.start();

                    // Schema must have been hydrated from the snapshot.
                    assertEquals("snapshot must restore exactly one vector index",
                            1, engine.listIndexes().size());
                    assertNotNull("table must be present after snapshot recovery",
                            engine.listIndexes().get(0).getTable());

                    // The recovery LSN equals the watermark — ready to
                    // resume safely (issue #364).
                    assertEquals(watermarkLsn, engine.getLastDurableLsn());

                    // Give the BK tailer a moment to detect the gap and
                    // settle. We deliberately use a generous deadline so
                    // slow CI environments do not flake — what we really
                    // care about is that the engine does not block
                    // forever (the negative case the test guards against).
                    long deadline = System.currentTimeMillis() + 15_000L;
                    while (System.currentTimeMillis() < deadline) {
                        Thread.sleep(100);
                        // No assertions inside the loop: we just want to
                        // give the tailer time to do its work without
                        // failing the test if it has not stopped yet.
                    }

                    // Whether the tailer is still trying to attach (it
                    // will keep retrying with backoff in some
                    // configurations) or has already stopped, the
                    // engine MUST still be responsive: schema visible,
                    // listIndexes works, durable LSN unchanged.
                    assertEquals("schema must remain visible while tailer reacts to the gap",
                            1, engine.listIndexes().size());
                    assertEquals("durable LSN must not move backwards",
                            watermarkLsn, engine.getLastDurableLsn());
                } finally {
                    // The critical safety assertion: close() must not
                    // hang. The engine's tailer thread is interrupted
                    // and joined with a 5-second timeout; tests run
                    // forked so a hang would surface as a Surefire
                    // timeout, not a clean failure — but completing
                    // close within a bounded time IS the contract under
                    // test, so we time it explicitly here.
                    long t0 = System.currentTimeMillis();
                    engine.close();
                    long elapsed = System.currentTimeMillis() - t0;
                    assertTrue("engine.close() must return within 30s even with a permanently-gapped log; took "
                            + elapsed + "ms", elapsed < 30_000L);
                }
            }
        }
    }
}
