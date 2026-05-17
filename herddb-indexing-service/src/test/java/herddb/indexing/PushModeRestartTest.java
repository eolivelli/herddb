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
import static org.junit.Assert.fail;
import herddb.codec.RecordSerializer;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import io.netty.buffer.ByteBuf;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Restart / recovery coverage for push mode. In push mode there is no commit
 * log to replay, but the indexing service still reloads its checkpointed
 * segments and restores the table/index schema from the persisted watermark
 * snapshot, and the {@link PushCommitLogTailer} resumes from the durable LSN.
 * It also resolves a stable tablespace UUID without a HerdDB server.
 */
public class PushModeRestartTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(120);

    private static Table vectorTable() {
        return Table.builder()
                .name("vectable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static Index vectorIndex() {
        return Index.builder()
                .name("vidx")
                .table("vectable")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    private static float[] vec(int seed, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = seed + i;
        }
        return v;
    }

    /**
     * Starts a push-mode service with an <em>empty</em> metadata store (no
     * HerdDB server registered the tablespace), so the engine must resolve the
     * tablespace UUID by deterministic derivation.
     */
    private EmbeddedIndexingService startPushService(Path logDir, Path dataDir, String storageType)
            throws Exception {
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, storageType);
        props.setProperty(IndexingServerConfiguration.PROPERTY_LOG_TYPE,
                IndexingServerConfiguration.PROPERTY_LOG_TYPE_PUSH);
        // Keep the persistent store path simple and deterministic for a tiny
        // test dataset — recovery of the watermark/schema/segments is what is
        // under test, not FusedPQ.
        props.setProperty(IndexingServerConfiguration.PROPERTY_VECTOR_FUSED_PQ, "false");
        EmbeddedIndexingService svc = new EmbeddedIndexingService(
                logDir, dataDir, new IndexingServerConfiguration(props));
        MemoryMetadataStorageManager meta = new MemoryMetadataStorageManager();
        meta.start();
        // Deliberately NO ensureDefaultTableSpace(): push mode must work
        // without a server having registered the tablespace.
        svc.setMetadataStorageManager(meta);
        svc.start();
        return svc;
    }

    private static void push(IndexingPushClient client, long ledger, long firstOffset,
                             List<LogEntry> entries) {
        List<LogSequenceNumber> lsns = new ArrayList<>();
        List<ByteBuf> bufs = new ArrayList<>();
        for (int i = 0; i < entries.size(); i++) {
            lsns.add(new LogSequenceNumber(ledger, firstOffset + i));
            bufs.add(entries.get(i).serializeAsByteBuf());
        }
        try {
            client.pushEntries(lsns, bufs);
        } finally {
            for (ByteBuf b : bufs) {
                b.release();
            }
        }
    }

    private static long vectorCount(IndexingPushClient client) {
        return client.getIndexStatus("default", "vectable", "vidx").getVectorCount();
    }

    private static void awaitVectorCount(IndexingPushClient client, long expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long last = -1;
        while (System.currentTimeMillis() < deadline) {
            last = vectorCount(client);
            if (last >= expected) {
                return;
            }
            Thread.sleep(50);
        }
        fail("indexed vector count did not reach " + expected + " (last=" + last + ")");
    }

    @Test
    public void pushModeDerivesAStableTableSpaceUuidAcrossRestarts() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        String firstUuid;
        EmbeddedIndexingService svc = startPushService(logDir, dataDir, "memory");
        try {
            firstUuid = svc.getEngine().getTableSpaceUUID();
            assertNotNull("a tablespace UUID must be resolved without a server", firstUuid);
            assertTrue("derived UUID must be non-empty", !firstUuid.isEmpty());
        } finally {
            svc.close();
        }
        // A fresh engine with a fresh empty metadata store must derive the
        // exact same UUID, so it addresses the same storage namespace.
        svc = startPushService(logDir, dataDir, "memory");
        try {
            assertEquals("tablespace UUID must be stable across restarts",
                    firstUuid, svc.getEngine().getTableSpaceUUID());
        } finally {
            svc.close();
        }
    }

    @Test
    public void pushTailerResumesFromThePersistedWatermarkSnapshot() throws Exception {
        // Inject a watermark snapshot that already carries the schema and a
        // non-START_OF_TIME LSN — the engine restores the schema, the push
        // tailer resumes from that LSN, and re-pushed stale entries are
        // skipped while genuinely new ones are applied.
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Table table = vectorTable();
        LogSequenceNumber durable = new LogSequenceNumber(5, 100);
        WatermarkSnapshot snapshot = new WatermarkSnapshot(durable, 1, 0L,
                Collections.singletonList(table),
                Collections.singletonList(vectorIndex()));

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_LOG_TYPE,
                IndexingServerConfiguration.PROPERTY_LOG_TYPE_PUSH);
        EmbeddedIndexingService svc = new EmbeddedIndexingService(
                logDir, dataDir, new IndexingServerConfiguration(props));
        MemoryMetadataStorageManager meta = new MemoryMetadataStorageManager();
        meta.start();
        svc.setMetadataStorageManager(meta);
        svc.setWatermarkStore(new InMemoryWatermarkStore(snapshot));
        svc.start();
        IndexingPushClient client = new IndexingPushClient(svc.getAddress());
        try {
            // The push tailer resumed at the durable LSN, not START_OF_TIME.
            assertEquals(durable, svc.getEngine().getPushTailer().getWatermark());
            // Schema was restored from the snapshot — the index exists.
            assertEquals(0L, vectorCount(client));

            // A re-pushed entry at/before the watermark is skipped.
            Record stale = RecordSerializer.makeRecord(table, "pk", "stale", "vec", vec(1, 8));
            push(client, 5, 100, Collections.singletonList(
                    LogEntryFactory.insert(table, stale.key, stale.value, null)));
            Thread.sleep(500);
            assertEquals("re-pushed stale entry must be skipped", 0L, vectorCount(client));

            // A genuinely new entry (LSN > watermark) is applied.
            Record fresh = RecordSerializer.makeRecord(table, "pk", "fresh", "vec", vec(2, 8));
            push(client, 5, 101, Collections.singletonList(
                    LogEntryFactory.insert(table, fresh.key, fresh.value, null)));
            awaitVectorCount(client, 1, 20_000);
        } finally {
            client.close();
            svc.close();
        }
    }

    @Test
    public void pushModeReloadsCheckpointedSegmentsAfterRestart() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();
        Table table = vectorTable();
        final int dim = 8;
        final int rows = 40;

        String tableSpaceUuid;
        LogSequenceNumber durable;

        // --- first run: push, checkpoint, capture the durable watermark ---
        EmbeddedIndexingService svc = startPushService(logDir, dataDir, "file");
        IndexingPushClient client = new IndexingPushClient(svc.getAddress());
        try {
            tableSpaceUuid = svc.getEngine().getTableSpaceUUID();
            push(client, 1, 1, Arrays.asList(
                    LogEntryFactory.createTable(table, null),
                    LogEntryFactory.createIndex(vectorIndex(), null)));
            List<LogEntry> inserts = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                Record r = RecordSerializer.makeRecord(table, "pk", "key" + i, "vec", vec(i, dim));
                inserts.add(LogEntryFactory.insert(table, r.key, r.value, null));
            }
            push(client, 1, 3, inserts);
            awaitVectorCount(client, rows, 60_000);

            // Force a checkpoint: segments + watermark + schema become durable.
            svc.getEngine().forceCheckpointAndSaveWatermark();
            durable = svc.getEngine().getLastDurableLsn();
            assertEquals("durable watermark must be the last pushed LSN",
                    new LogSequenceNumber(1, 2 + rows), durable);
        } finally {
            client.close();
            svc.close();
        }

        // --- restart: same directories, fresh empty metadata store ---
        svc = startPushService(logDir, dataDir, "file");
        client = new IndexingPushClient(svc.getAddress());
        try {
            assertEquals("tablespace UUID must be stable across restarts",
                    tableSpaceUuid, svc.getEngine().getTableSpaceUUID());
            // Checkpointed segments are reloaded with no commit-log replay.
            awaitVectorCount(client, rows, 60_000);
            // The push tailer resumed from the durable watermark.
            assertEquals(durable, svc.getEngine().getPushTailer().getWatermark());

            // A re-pushed stale entry (LSN <= watermark) is skipped.
            Record stale = RecordSerializer.makeRecord(table, "pk", "key0", "vec", vec(0, dim));
            push(client, 1, 2 + rows, Collections.singletonList(
                    LogEntryFactory.insert(table, stale.key, stale.value, null)));
            Thread.sleep(500);
            assertEquals("re-pushed stale entry must not change the count",
                    (long) rows, vectorCount(client));

            // A new entry past the watermark is applied on top of the
            // recovered state.
            Record fresh = RecordSerializer.makeRecord(table, "pk", "afterRestart", "vec", vec(99, dim));
            push(client, 1, 3 + rows, Collections.singletonList(
                    LogEntryFactory.insert(table, fresh.key, fresh.value, null)));
            awaitVectorCount(client, rows + 1, 60_000);
        } finally {
            client.close();
            svc.close();
        }
    }

    /** In-memory {@link WatermarkStore} preloaded with one snapshot. */
    private static final class InMemoryWatermarkStore implements WatermarkStore {

        private WatermarkSnapshot snapshot;

        InMemoryWatermarkStore(WatermarkSnapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public WatermarkSnapshot load() {
            return snapshot;
        }

        @Override
        public void save(WatermarkSnapshot snapshot) {
            this.snapshot = snapshot;
        }
    }
}
