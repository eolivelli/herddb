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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end regression for issue #364.
 *
 * <p>Drives a real {@link IndexingServiceEngine} (with a real
 * {@link PersistentVectorStore} + checkpoint pipeline) behind a real
 * gRPC server, then queries it with a real {@link IndexingServiceClient}.
 * The test reproduces the BIGANN 1B failure mode: the in-memory tailer
 * advances past a checkpoint that failed to save, while the persisted
 * watermark stays anchored at the previous durable LSN. The retention
 * floor {@link IndexingServiceClient#getMinProcessedLsn(String) returned
 * to the server} MUST be the persisted LSN — not the unsafe in-memory
 * value.
 *
 * <p>This test is the wire-level companion to
 * {@link IndexingServiceEngineDurableLsnTest} (engine-only) and
 * {@link IndexingServiceClientDurableLsnTest} (client-only).
 */
public class Issue364DurableLsnEndToEndTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private int savedMinLive;
    private long savedDeferral;

    @Before
    public void saveGateState() {
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        savedDeferral = PersistentVectorStore.maxCheckpointDeferralMs;
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreGateState() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
        PersistentVectorStore.maxCheckpointDeferralMs = savedDeferral;
    }

    /**
     * In-memory watermark store whose {@code save()} can be flipped to
     * fail mid-test, simulating an S3 / file-server outage that prevents
     * the IS from publishing its recovery LSN.
     */
    private static final class FailableWatermarkStore implements WatermarkStore {
        private final AtomicReference<WatermarkSnapshot> saved =
                new AtomicReference<>(WatermarkSnapshot.START_OF_TIME);
        private volatile boolean failOnNextSave;

        void setFailOnNextSave(boolean fail) {
            this.failOnNextSave = fail;
        }

        @Override
        public synchronized WatermarkSnapshot load() {
            return saved.get();
        }

        @Override
        public synchronized void save(WatermarkSnapshot snapshot) throws IOException {
            if (failOnNextSave) {
                throw new IOException("simulated watermark save failure");
            }
            saved.set(snapshot);
        }
    }

    private static MemoryMetadataStorageManager createTestMetadata() throws Exception {
        MemoryMetadataStorageManager m = new MemoryMetadataStorageManager();
        m.start();
        m.ensureDefaultTableSpace("local", "local", 0, 1);
        return m;
    }

    private Table createTable() {
        return Table.builder()
                .name("vectable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Reproduces the issue #364 failure mode end-to-end:
     * <ol>
     *   <li>Start an IS with a {@link FailableWatermarkStore}.</li>
     *   <li>Apply a batch and run a successful checkpoint — durable LSN A.</li>
     *   <li>Apply more entries (tailer LSN advances past A).</li>
     *   <li>Trigger a checkpoint with the watermark save failing — durable
     *       LSN stays at A while tailer LSN is past A.</li>
     *   <li>A {@link IndexingServiceClient} querying this IS must report
     *       LSN A as the retention floor — never the unsafe tailer LSN.
     *       This is the exact value the server's
     *       {@code BookkeeperCommitLog.dropOldLedgers} pins against, so
     *       the broken behaviour would let the server drop log ledgers
     *       the IS would still need to replay on recovery.</li>
     * </ol>
     */
    @Test
    public void retentionFloorTracksDurableLsnNotTailerLsn() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());
        FailableWatermarkStore watermark = new FailableWatermarkStore();
        engine.setWatermarkStore(watermark);

        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(
                128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName,
                                       dataDirectory, indexProperties) -> {
            PersistentVectorStore pvs = new PersistentVectorStore(
                    indexName, tableName, "tstblspace", vectorColumnName,
                    dataDirectory, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE,
                    VectorSimilarityFunction.EUCLIDEAN);
            try {
                pvs.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return pvs;
        });

        // Bring up an in-process gRPC server in front of the engine, then a
        // real IndexingServiceClient (the same code the herddb server uses).
        IndexingServer grpcServer = new IndexingServer("localhost", 0, engine, config);
        try {
            grpcServer.start();
            engine.start();

            Table table = createTable();
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            Index index = Index.builder()
                    .name("vidx")
                    .table("vectable")
                    .type(Index.TYPE_VECTOR)
                    .column("vec", ColumnTypes.FLOATARRAY)
                    .build();
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(index, null));

            Random rng = new Random(303);
            int dim = 16;
            long base = 100;

            // Phase 1: ingest + successful checkpoint → durable LSN A.
            LogSequenceNumber lsnA = null;
            for (int i = 0; i < 256; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "k" + i,
                        "vec", randomVector(rng, dim));
                LogEntry insert =
                        LogEntryFactory.insert(table, record.key, record.value, null);
                lsnA = new LogSequenceNumber(1, base + i);
                engine.applySingleEntryForTest(lsnA, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(lsnA);
            engine.forceCheckpointAndSaveWatermark();
            assertEquals("phase 1 sets durable to lsnA",
                    lsnA, engine.getLastDurableLsn());

            // Phase 2: more inserts; tailer advances past lsnA without a
            // successful checkpoint (we'll fail the next save).
            LogSequenceNumber lsnB = null;
            for (int i = 0; i < 200; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "k" + (1000 + i),
                        "vec", randomVector(rng, dim));
                LogEntry insert =
                        LogEntryFactory.insert(table, record.key, record.value, null);
                lsnB = new LogSequenceNumber(2, i + 1);
                engine.applySingleEntryForTest(lsnB, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(lsnB);

            watermark.setFailOnNextSave(true);
            engine.forceCheckpointAndSaveWatermark();

            // Engine-side invariant: tailer past lsnA, durable still at lsnA.
            assertEquals("durable LSN must stay at lsnA after the failed save",
                    lsnA, engine.getLastDurableLsn());
            assertEquals("tailer LSN advanced to lsnB",
                    lsnB, engine.getLastProcessedLsn());
            assertNotEquals("the test scenario requires distinct tailer/durable LSNs",
                    engine.getLastProcessedLsn(), engine.getLastDurableLsn());

            // Wire-side invariant: the value the server uses for retention
            // is the durable LSN, never the tailer LSN. This is what
            // protects ledger lsnA-required entries from being dropped while
            // the IS catches up its own watermark.
            String addr = "localhost:" + grpcServer.getPort();
            try (IndexingServiceClient client = IndexingServiceClient.fromAddresses(
                    Arrays.asList(addr), 5)) {
                Optional<LogSequenceNumber> floor =
                        client.getMinProcessedLsn("local");
                assertTrue("retention floor must be present", floor.isPresent());
                assertEquals(
                        "retention floor MUST be the durable LSN (lsnA), "
                                + "not the tailer LSN (lsnB) — issue #364",
                        lsnA, floor.get());
                assertNotNull("durable lsnA must not be null", lsnA);
            }

            // Recovery: next save succeeds → durable catches up → retention
            // floor moves forward as expected.
            watermark.setFailOnNextSave(false);
            engine.forceCheckpointAndSaveWatermark();
            assertEquals("durable LSN catches up after a successful save",
                    lsnB, engine.getLastDurableLsn());

            try (IndexingServiceClient client = IndexingServiceClient.fromAddresses(
                    Arrays.asList(addr), 5)) {
                Optional<LogSequenceNumber> floor =
                        client.getMinProcessedLsn("local");
                assertTrue(floor.isPresent());
                assertEquals(
                        "retention floor advances to lsnB once the watermark is durable",
                        lsnB, floor.get());
            }
        } finally {
            grpcServer.stop();
            engine.close();
        }
    }
}
