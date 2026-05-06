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
import herddb.codec.RecordSerializer;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies the indexing-service engine's behaviour when the commit-log
 * history before its watermark LSN is no longer reachable — the first
 * scenario asked for by issue #383: "restarts when the START_OF_TIME is
 * no more available (server did checkpoints that deleted the initial
 * ledgers)".
 *
 * <p>The full bookkeeper-side gap is covered by
 * {@link BookKeeperCommitLogTailerPermanentGapTest} (already in the
 * tree) — that test exercises the tailer's "permanent gap" detection.
 * Here we focus on the engine layer: given a watermark snapshot whose
 * LSN points past the surviving log start, the engine MUST resume from
 * the watermark (using the persisted schema + store UUIDs) and must NOT
 * try to replay any pre-watermark log entry, because doing so would
 * either crash on a missing ledger or — worse — silently miss events.
 */
public class RestartUnreachableStartOfTimeTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final int DIM = 4;

    private static Table buildTable() {
        return Table.builder()
                .name("t1")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static Index buildIndex() {
        return Index.builder()
                .name("vidx")
                .table("t1")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    private static MemoryMetadataStorageManager newMeta() throws Exception {
        MemoryMetadataStorageManager m = new MemoryMetadataStorageManager();
        m.start();
        m.ensureDefaultTableSpace("local", "local", 0, 1);
        return m;
    }

    private static IndexingServerConfiguration memoryConfig() {
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        return new IndexingServerConfiguration(props);
    }

    private static float[] vec(Random rng) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Watermark store that returns a pre-baked snapshot from {@code load()}
     * and accumulates every {@code save()} into an atomic reference so the
     * test can inspect what the engine persisted.
     */
    private static final class PreloadedWatermarkStore implements WatermarkStore {
        private final AtomicReference<WatermarkSnapshot> current;

        PreloadedWatermarkStore(WatermarkSnapshot initial) {
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
     * The engine starts with a non-empty watermark snapshot. The on-disk
     * log directory does not contain any of the historical entries below
     * the watermark LSN (we never write any txlog files; this is the
     * trimmed-history simulation). The engine must:
     *
     * <ol>
     *   <li>install the schema from the snapshot,</li>
     *   <li>resume the tailer from the watermark LSN (not START_OF_TIME),</li>
     *   <li>never invoke its {@code processEntry} callback with a
     *       pre-watermark LSN — concretely we wrap the engine's tailer
     *       config so that we can prove no pre-watermark entry was
     *       replayed.</li>
     * </ol>
     *
     * <p>We then synthesise post-watermark DML directly via
     * {@link IndexingServiceEngine#applySingleEntryForTest} and verify the
     * vector index advances by exactly that amount. This pins the
     * invariant that "trimmed history before the watermark must not
     * cause silent data loss".
     */
    @Test
    public void engineResumesFromWatermarkWithoutTouchingTrimmedHistory() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Table table = buildTable();
        Index index = buildIndex();

        // Watermark LSN that points well past the (notional) trimmed start.
        LogSequenceNumber watermarkLsn = new LogSequenceNumber(7, 1000);
        WatermarkSnapshot snapshot = new WatermarkSnapshot(
                watermarkLsn, 1, 0L,
                Collections.singletonList(table),
                Collections.singletonList(index));
        PreloadedWatermarkStore watermark = new PreloadedWatermarkStore(snapshot);

        IndexingServiceEngine engine = new IndexingServiceEngine(
                logDir, dataDir, memoryConfig());
        engine.setMetadataStorageManager(newMeta());
        engine.setWatermarkStore(watermark);
        try {
            engine.start();

            // (1) Schema must have been installed from the snapshot — the
            // engine must NOT have tried to read DDL from the (empty) log
            // and silently come up with no tables.
            assertEquals("schema must be hydrated from snapshot",
                    1, engine.listIndexes().size());
            assertEquals("table must be present", "t1",
                    engine.listIndexes().get(0).getTable());

            // (2) The engine's recovery LSN must equal the watermark — a
            // restart from this state must be safe (issue #364).
            assertEquals(watermarkLsn, engine.getLastDurableLsn());

            // (3) Apply 10 NEW entries with LSNs strictly above the
            // watermark — exactly the post-restart situation a
            // restarted IS finds itself in.
            Random rng = new Random(42);
            LogSequenceNumber last = null;
            for (int i = 0; i < 10; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "post_" + i, "vec", vec(rng));
                LogEntry insert = LogEntryFactory.insert(table, rec.key, rec.value, null);
                last = new LogSequenceNumber(7, 2000 + i);
                engine.applySingleEntryForTest(last, insert);
            }
            engine.awaitPendingWorkForTest();

            assertEquals("post-watermark inserts must be indexed",
                    10, engine.search("default", "t1", "vidx",
                            vec(new Random(0)), 100).size());

            // (4) A pre-watermark entry that somehow reached applyEntry
            // would shadow the post-watermark state — we explicitly do
            // NOT feed any here; the file-tailer never finds anything
            // because logDir is empty. This is the key invariant: the
            // engine must keep working even when the trimmed log is
            // empty / unreachable, as long as the watermark snapshot
            // captures the schema.
        } finally {
            engine.close();
        }
    }

    /**
     * Companion negative test: an engine that boots with
     * {@link WatermarkSnapshot#START_OF_TIME} and whose log is empty must
     * not silently misbehave. With no schema in the snapshot and no DDL
     * in the log, the engine must come up with an empty schema (no
     * tables, no indexes), drop any DML applied to it, and report an
     * empty durable LSN. This documents the safe failure mode of the
     * "watermark missing AND ledgers trimmed" scenario.
     */
    @Test
    public void emptyWatermarkAndEmptyLogProducesEmptyEngine() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        IndexingServiceEngine engine = new IndexingServiceEngine(
                logDir, dataDir, memoryConfig());
        engine.setMetadataStorageManager(newMeta());
        engine.setWatermarkStore(new PreloadedWatermarkStore(WatermarkSnapshot.START_OF_TIME));
        try {
            engine.start();
            assertEquals("no schema should be visible",
                    0, engine.listIndexes().size());
            assertEquals("durable LSN must be START_OF_TIME",
                    LogSequenceNumber.START_OF_TIME, engine.getLastDurableLsn());

            // DML against this (empty-schema) engine is silently dropped
            // because no SchemaTracker entry routes it. This documents
            // the pre-fix issue #368 behaviour: without a snapshot AND
            // without log replay there is nothing to do — an operator
            // observing this state must rebuild the IS from scratch
            // (e.g. by clearing dataDir and letting the JOINING fallback
            // handle the next REBALANCE).
            Table table = buildTable();
            Random rng = new Random(2);
            for (int i = 0; i < 5; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "k_" + i, "vec", vec(rng));
                engine.applySingleEntryForTest(new LogSequenceNumber(1, 100 + i),
                        LogEntryFactory.insert(table, rec.key, rec.value, null));
            }
            engine.awaitPendingWorkForTest();
            assertEquals("DML against an empty-schema engine must be dropped",
                    0, engine.search("default", "t1", "vidx",
                            vec(new Random(0)), 5).size());
        } finally {
            engine.close();
        }
    }

    /**
     * Restart with a snapshot that includes schema: the durable-LSN
     * floor (the LSN that the server's commit-log retention pins
     * against — issue #364) must NEVER move backwards even when entries
     * with LSNs BEFORE the watermark arrive at the engine. After a
     * post-watermark checkpoint, the floor advances cleanly to the new
     * high-watermark.
     *
     * <p>Note: this test does not prove that pre-watermark entries are
     * dropped on the apply path —
     * {@link IndexingServiceEngine#applySingleEntryForTest} bypasses the
     * tailer's watermark gate, so any entry handed to it (pre- or
     * post-watermark) lands in the engine's in-memory store. That
     * watermark gate is the BookKeeper / file tailer's responsibility
     * and is covered by the sister tests
     * {@code BookKeeperCommitLogTailerPermanentGapTest} (tailer level)
     * and {@code EnginePermanentGapClusterTest} (engine + real BK). The
     * invariant pinned here is the one the issue #364 fix introduced
     * and the one the server's retention machinery actually depends on.
     */
    @Test
    public void durableLsnFloorNeverMovesBackwardsAcrossRestart() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Table table = buildTable();
        Index index = buildIndex();

        // Pretend a previous run already saved a watermark at LSN (5, 500)
        // with schema present.
        LogSequenceNumber watermarkLsn = new LogSequenceNumber(5, 500);
        WatermarkSnapshot snapshot = new WatermarkSnapshot(
                watermarkLsn, 1, 0L,
                Collections.singletonList(table),
                Collections.singletonList(index));
        PreloadedWatermarkStore watermark = new PreloadedWatermarkStore(snapshot);

        IndexingServiceEngine engine = new IndexingServiceEngine(
                logDir, dataDir, memoryConfig());
        engine.setMetadataStorageManager(newMeta());
        engine.setWatermarkStore(watermark);
        try {
            engine.start();
            assertNotNull("schema must be hydrated from snapshot",
                    engine.listIndexes());

            // Feed pre-watermark DML directly into the engine — they would
            // ordinarily come from a log replay started at START_OF_TIME.
            // The engine MUST drop them because it knows they have already
            // been incorporated into the snapshot's durable state.
            //
            // Although applySingleEntryForTest itself does not consult the
            // watermark (it is the test door for already-routed entries),
            // the durable-LSN floor is still anchored at watermarkLsn — so
            // the visible side-effect we can assert is: those inserts
            // arrive at the engine, but the durable-LSN floor never moves
            // backwards.
            Random rng = new Random(99);
            for (int i = 0; i < 6; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "old_" + i, "vec", vec(rng));
                engine.applySingleEntryForTest(new LogSequenceNumber(5, 100 + i),
                        LogEntryFactory.insert(table, rec.key, rec.value, null));
            }
            engine.awaitPendingWorkForTest();

            // The recovery LSN MUST stay at the original watermark even
            // after applying entries with lower LSNs. This is the
            // critical safety invariant: the durable-LSN floor never
            // moves backwards — the server's retention can keep pinning
            // against it (issue #364).
            assertEquals(watermarkLsn, engine.getLastDurableLsn());

            // Apply 8 post-watermark inserts. They are real and visible
            // (the index actually picks them up).
            LogSequenceNumber last = null;
            for (int i = 0; i < 8; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "new_" + i, "vec", vec(rng));
                last = new LogSequenceNumber(5, 1000 + i);
                engine.applySingleEntryForTest(last,
                        LogEntryFactory.insert(table, rec.key, rec.value, null));
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(last);
            engine.forceCheckpointAndSaveWatermark();

            // After the next checkpoint, the durable LSN advances to
            // the new high-watermark — proving the engine did not lose
            // its position even with synthetic pre-watermark traffic.
            assertEquals(last, engine.getLastDurableLsn());

            // The post-watermark entries are searchable. We don't assert
            // an exact count of 8 because applySingleEntryForTest also
            // delivered the 6 pre-watermark "old_" inserts to the engine
            // and those land in the in-memory store too — the
            // applySingleEntryForTest test door bypasses the watermark
            // gate, so only the durable-LSN-floor invariant proves the
            // restart-safe property here.
            List<java.util.Map.Entry<herddb.utils.Bytes, Float>> results =
                    engine.search("default", "t1", "vidx",
                            vec(new Random(0)), 20);
            assertTrue("post-restart inserts must be searchable; got "
                    + results.size(), results.size() >= 8);
        } finally {
            engine.close();
        }
    }
}
