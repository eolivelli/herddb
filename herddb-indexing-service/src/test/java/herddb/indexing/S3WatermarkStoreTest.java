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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Table;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class S3WatermarkStoreTest {

    /** In-memory fake of the remote file service for unit tests. */
    static class FakeRemoteFileIO implements S3WatermarkStore.RemoteFileIO {
        final Map<String, byte[]> store = new HashMap<>();
        volatile boolean failNextWrite = false;
        volatile boolean failNextRead = false;
        volatile int writeCount = 0;

        @Override
        public void writeFile(String path, byte[] content) throws IOException {
            if (failNextWrite) {
                failNextWrite = false;
                throw new IOException("injected write failure for " + path);
            }
            store.put(path, content.clone());
            writeCount++;
        }

        @Override
        public byte[] readFile(String path) throws IOException {
            if (failNextRead) {
                failNextRead = false;
                throw new IOException("injected read failure for " + path);
            }
            byte[] data = store.get(path);
            return data == null ? null : data.clone();
        }
    }

    // ---------- helpers -------------------------------------------------------

    private static Table buildTable(String name) {
        return Table.builder()
                .tablespace("default")
                .name(name)
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static Index buildVectorIndex(String name, String table) {
        return Index.builder()
                .name(name)
                .table(table)
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    // ---------- existing tests ------------------------------------------------

    @Test
    public void loadReturnsStartOfTimeWhenAbsent() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);
        assertEquals(WatermarkSnapshot.START_OF_TIME, store.load());
    }

    @Test
    public void saveAndLoadRoundTrip() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);

        WatermarkSnapshot saved = new WatermarkSnapshot(new LogSequenceNumber(42L, 7L), 4);
        store.save(saved);

        WatermarkSnapshot loaded = store.load();
        assertEquals(saved.lsn.ledgerId, loaded.lsn.ledgerId);
        assertEquals(saved.lsn.offset, loaded.lsn.offset);
        assertEquals(saved.numInstances, loaded.numInstances);
    }

    @Test
    public void pathIncludesTablespaceAndInstance() {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid-abc", 3);
        assertEquals("ts-uuid-abc/_indexing/3/watermark.lsn", store.getPath());
    }

    @Test
    public void monotonicSaveBlocksRegression() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);

        store.save(new WatermarkSnapshot(new LogSequenceNumber(5L, 100L), 2));
        byte[] persisted = io.store.get(store.getPath());

        // Attempt to save an older LSN — should be a no-op.
        store.save(new WatermarkSnapshot(new LogSequenceNumber(5L, 50L), 4));
        assertArrayEquals("older save must not overwrite",
                persisted, io.store.get(store.getPath()));

        // Verify the stored snapshot is still the newer one.
        WatermarkSnapshot loaded = store.load();
        assertEquals(5L, loaded.lsn.ledgerId);
        assertEquals(100L, loaded.lsn.offset);
        assertEquals(2, loaded.numInstances);

        // A newer save goes through.
        store.save(new WatermarkSnapshot(new LogSequenceNumber(5L, 200L), 4));
        loaded = store.load();
        assertEquals(200L, loaded.lsn.offset);
        assertEquals(4, loaded.numInstances);
    }

    @Test
    public void equalLsnSaveIsIdempotent() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);

        WatermarkSnapshot snap = new WatermarkSnapshot(new LogSequenceNumber(1L, 1L), 1);
        store.save(snap);
        store.save(snap);
        // Both saves happened — monotonicity allows equal LSN.
        assertEquals(2, io.writeCount);
        assertEquals(snap.lsn.offset, store.load().lsn.offset);
    }

    @Test
    public void corruptObjectIsDetected() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);

        store.save(new WatermarkSnapshot(new LogSequenceNumber(7L, 99L), 1));
        byte[] data = io.store.get(store.getPath());
        // Flip a byte in the payload (not the XXHash footer) — integrity check must catch.
        data[0] ^= 0x55;
        io.store.put(store.getPath(), data);

        assertThrows(S3WatermarkStore.CorruptWatermarkException.class, store::load);
    }

    @Test
    public void truncatedObjectIsDetected() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        io.store.put("ts-uuid/_indexing/0/watermark.lsn", new byte[]{1, 2, 3});
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);
        assertThrows(S3WatermarkStore.CorruptWatermarkException.class, store::load);
    }

    @Test
    public void saveAfterCorruptionOverwrites() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);
        // Seed a corrupt object.
        io.store.put(store.getPath(), new byte[]{9, 9, 9});
        // Writing over it must succeed (corrupt current value does not block saves).
        store.save(new WatermarkSnapshot(new LogSequenceNumber(3L, 3L), 1));
        assertEquals(3L, store.load().lsn.offset);
    }

    @Test
    public void readFailurePropagatesAsIOException() {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        io.failNextRead = true;
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);
        IOException ex = assertThrows(IOException.class, store::load);
        assertNotNull(ex.getMessage());
    }

    @Test
    public void writeFailurePropagatesAsIOException() {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        io.failNextWrite = true;
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);
        IOException ex = assertThrows(IOException.class,
                () -> store.save(new WatermarkSnapshot(new LogSequenceNumber(1L, 1L), 1)));
        assertTrue(ex.getMessage().contains("watermark"));
        // No object was written.
        assertNull(io.store.get(store.getPath()));
    }

    // ---------- schema round-trip tests (issue #368) --------------------------

    /**
     * Verifies that a {@link WatermarkSnapshot} carrying schema (table and
     * vector-index definitions) survives a save-load cycle through
     * {@link S3WatermarkStore} with all fields intact, and that the
     * XXHash64 integrity check covers the schema bytes.
     */
    @Test
    public void saveAndLoadRoundTripWithSchema() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 1);

        Table t = buildTable("tbl1");
        Index ix = buildVectorIndex("vidx1", "tbl1");

        WatermarkSnapshot saved = new WatermarkSnapshot(
                new LogSequenceNumber(5L, 300L), 2,
                Arrays.asList(t), Arrays.asList(ix));
        store.save(saved);

        WatermarkSnapshot loaded = store.load();
        assertEquals("lsn.ledgerId", 5L, loaded.lsn.ledgerId);
        assertEquals("lsn.offset", 300L, loaded.lsn.offset);
        assertEquals("numInstances", 2, loaded.numInstances);
        assertTrue("snapshot must carry schema", loaded.hasSchema());
        assertEquals("one table", 1, loaded.tables.size());
        assertEquals("table name", "tbl1", loaded.tables.get(0).name);
        assertEquals("one vector index", 1, loaded.vectorIndexes.size());
        assertEquals("index name", "vidx1", loaded.vectorIndexes.get(0).name);
    }

    /**
     * Verifies that multiple tables and indexes are round-tripped faithfully
     * through {@link S3WatermarkStore}.
     */
    @Test
    public void saveAndLoadRoundTripWithMultipleTablesAndIndexes() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts", 0);

        Table t1 = buildTable("ta");
        Table t2 = buildTable("tb");
        Index ix1 = buildVectorIndex("i1", "ta");
        Index ix2 = buildVectorIndex("i2", "tb");

        WatermarkSnapshot saved = new WatermarkSnapshot(
                new LogSequenceNumber(3L, 77L), 4,
                Arrays.asList(t1, t2), Arrays.asList(ix1, ix2));
        store.save(saved);

        WatermarkSnapshot loaded = store.load();
        assertEquals("two tables", 2, loaded.tables.size());
        assertEquals("two indexes", 2, loaded.vectorIndexes.size());
        assertEquals("table 1", "ta", loaded.tables.get(0).name);
        assertEquals("table 2", "tb", loaded.tables.get(1).name);
        assertEquals("index 1", "i1", loaded.vectorIndexes.get(0).name);
        assertEquals("index 2", "i2", loaded.vectorIndexes.get(1).name);
    }

    /**
     * Verifies that the hash check catches corruption in schema bytes:
     * flipping a bit in the middle of the schema payload must trigger
     * {@link S3WatermarkStore.CorruptWatermarkException}.
     */
    @Test
    public void schemaBytesAreCoveredByHashCheck() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts", 0);

        Table t = buildTable("mytable");
        Index ix = buildVectorIndex("myidx", "mytable");
        store.save(new WatermarkSnapshot(
                new LogSequenceNumber(1L, 1L), 1,
                Arrays.asList(t), Arrays.asList(ix)));

        byte[] data = io.store.get(store.getPath());
        // Corrupt the middle of the payload (well past the fixed header),
        // which falls in the schema region.
        int middle = data.length / 2;
        data[middle] ^= 0xFF;
        io.store.put(store.getPath(), data);

        assertThrows("schema corruption must be detected by hash",
                S3WatermarkStore.CorruptWatermarkException.class, store::load);
    }

    /**
     * Verifies that the monotonicity check works correctly when the stored
     * object carries schema: a newer snapshot (higher LSN) with different
     * schema must overwrite the old one.
     */
    @Test
    public void monotonicSaveWorksWithSchema() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts", 0);

        Table t1 = buildTable("first");
        Index ix1 = buildVectorIndex("idx1", "first");
        store.save(new WatermarkSnapshot(
                new LogSequenceNumber(1L, 100L), 1,
                Collections.singletonList(t1), Collections.singletonList(ix1)));

        // Attempt to regress — must be rejected.
        Table t2 = buildTable("second");
        store.save(new WatermarkSnapshot(
                new LogSequenceNumber(1L, 50L), 2,
                Collections.singletonList(t2), Collections.emptyList()));

        WatermarkSnapshot loaded = store.load();
        assertEquals("must still see the first table", "first", loaded.tables.get(0).name);
        assertEquals(100L, loaded.lsn.offset);

        // Advance LSN — must go through.
        Table t3 = buildTable("third");
        store.save(new WatermarkSnapshot(
                new LogSequenceNumber(1L, 200L), 1,
                Collections.singletonList(t3), Collections.emptyList()));
        loaded = store.load();
        assertEquals("advanced table name", "third", loaded.tables.get(0).name);
        assertEquals(200L, loaded.lsn.offset);
    }
}
