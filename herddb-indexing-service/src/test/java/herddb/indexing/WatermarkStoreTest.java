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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Table;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WatermarkStoreTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

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

    // ---------- existing tests (unchanged) ------------------------------------

    @Test
    public void testLoadReturnsStartOfTimeWhenNoFile() throws IOException {
        LocalWatermarkStore store = new LocalWatermarkStore(folder.newFolder("empty").toPath());
        WatermarkSnapshot snapshot = store.load();
        assertEquals(WatermarkSnapshot.START_OF_TIME, snapshot);
    }

    @Test
    public void testSaveAndLoad() throws IOException {
        Path dir = folder.newFolder("data").toPath();
        LocalWatermarkStore store = new LocalWatermarkStore(dir);

        WatermarkSnapshot saved = new WatermarkSnapshot(new LogSequenceNumber(5, 42), 4);
        store.save(saved);

        WatermarkSnapshot loaded = store.load();
        assertEquals(saved.lsn.ledgerId, loaded.lsn.ledgerId);
        assertEquals(saved.lsn.offset, loaded.lsn.offset);
        assertEquals(saved.numInstances, loaded.numInstances);
    }

    @Test
    public void testOverwrite() throws IOException {
        Path dir = folder.newFolder("overwrite").toPath();
        LocalWatermarkStore store = new LocalWatermarkStore(dir);

        store.save(new WatermarkSnapshot(new LogSequenceNumber(1, 10), 2));
        store.save(new WatermarkSnapshot(new LogSequenceNumber(2, 20), 4));

        WatermarkSnapshot loaded = store.load();
        assertEquals(2, loaded.lsn.ledgerId);
        assertEquals(20, loaded.lsn.offset);
        assertEquals(4, loaded.numInstances);
    }

    // ---------- schema round-trip tests (issue #368) --------------------------

    /**
     * Verifies that a {@link WatermarkSnapshot} carrying schema (table and
     * vector-index definitions) survives a save-load cycle through
     * {@link LocalWatermarkStore} with all fields intact.
     */
    @Test
    public void testSaveAndLoadWithSchema() throws IOException {
        Path dir = folder.newFolder("schema").toPath();
        LocalWatermarkStore store = new LocalWatermarkStore(dir);

        Table t = buildTable("mytable");
        Index ix = buildVectorIndex("myidx", "mytable");
        WatermarkSnapshot saved = new WatermarkSnapshot(
                new LogSequenceNumber(7, 100), 2,
                Arrays.asList(t), Arrays.asList(ix));

        store.save(saved);
        WatermarkSnapshot loaded = store.load();

        assertEquals("lsn.ledgerId", 7, loaded.lsn.ledgerId);
        assertEquals("lsn.offset", 100, loaded.lsn.offset);
        assertEquals("numInstances", 2, loaded.numInstances);
        assertTrue("loaded snapshot must carry schema", loaded.hasSchema());
        assertEquals("one table", 1, loaded.tables.size());
        assertEquals("table name", "mytable", loaded.tables.get(0).name);
        assertEquals("one vector index", 1, loaded.vectorIndexes.size());
        assertEquals("index name", "myidx", loaded.vectorIndexes.get(0).name);
        assertEquals("index table", "mytable", loaded.vectorIndexes.get(0).table);
    }

    /**
     * Verifies that an empty-schema snapshot (the legacy 2-arg constructor)
     * round-trips correctly: after saving and loading, {@link WatermarkSnapshot#hasSchema()}
     * returns {@code false} and the tables / vectorIndexes lists are empty.
     */
    @Test
    public void testSaveAndLoadWithEmptySchema() throws IOException {
        Path dir = folder.newFolder("empty-schema").toPath();
        LocalWatermarkStore store = new LocalWatermarkStore(dir);

        WatermarkSnapshot saved = new WatermarkSnapshot(new LogSequenceNumber(3, 55), 1);
        store.save(saved);

        WatermarkSnapshot loaded = store.load();
        assertEquals("lsn.ledgerId", 3, loaded.lsn.ledgerId);
        assertEquals("lsn.offset", 55, loaded.lsn.offset);
        assertEquals("numInstances", 1, loaded.numInstances);
        assertFalse("empty-schema snapshot must not report hasSchema()", loaded.hasSchema());
        assertTrue("tables list must be empty", loaded.tables.isEmpty());
        assertTrue("vectorIndexes list must be empty", loaded.vectorIndexes.isEmpty());
    }

    /**
     * Verifies that multiple tables and indexes are round-tripped faithfully
     * through {@link LocalWatermarkStore}.
     */
    @Test
    public void testSaveAndLoadWithMultipleTablesAndIndexes() throws IOException {
        Path dir = folder.newFolder("multi").toPath();
        LocalWatermarkStore store = new LocalWatermarkStore(dir);

        Table t1 = buildTable("tab1");
        Table t2 = buildTable("tab2");
        Index ix1 = buildVectorIndex("idx1", "tab1");
        Index ix2 = buildVectorIndex("idx2", "tab2");

        WatermarkSnapshot saved = new WatermarkSnapshot(
                new LogSequenceNumber(10, 999), 4,
                Arrays.asList(t1, t2), Arrays.asList(ix1, ix2));
        store.save(saved);

        WatermarkSnapshot loaded = store.load();
        assertEquals("two tables", 2, loaded.tables.size());
        assertEquals("two vector indexes", 2, loaded.vectorIndexes.size());
        assertEquals("numInstances", 4, loaded.numInstances);
        assertEquals("table 1 name", "tab1", loaded.tables.get(0).name);
        assertEquals("table 2 name", "tab2", loaded.tables.get(1).name);
        assertEquals("index 1 name", "idx1", loaded.vectorIndexes.get(0).name);
        assertEquals("index 2 name", "idx2", loaded.vectorIndexes.get(1).name);
    }

    /**
     * Verifies that a corrupt watermark file with an absurdly large tableCount
     * causes an {@link IOException} rather than an {@link OutOfMemoryError}
     * (bounds-check regression for issue #368).
     */
    @Test
    public void testCorruptTableCountThrowsIOException() throws Exception {
        Path dir = folder.newFolder("corrupt-count").toPath();
        java.nio.file.Files.createDirectories(dir);
        Path wf = dir.resolve("watermark.dat");

        // Manually craft a watermark file that has a valid header but an
        // absurdly large tableCount, which must be caught before an OOM.
        try (java.io.DataOutputStream dos =
                new java.io.DataOutputStream(java.nio.file.Files.newOutputStream(wf))) {
            dos.writeByte(1);                    // version
            dos.writeLong(1L);                   // ledgerId
            dos.writeLong(100L);                 // offset
            dos.writeInt(2);                     // numInstances
            dos.writeInt(Integer.MAX_VALUE);     // corrupt tableCount
        }

        LocalWatermarkStore store = new LocalWatermarkStore(dir);
        assertThrows("corrupt tableCount must raise IOException, not OOM",
                IOException.class, store::load);
    }

    /**
     * Verifies that a corrupt watermark file with a negative per-blob length
     * causes an {@link IOException} rather than {@link NegativeArraySizeException}.
     */
    @Test
    public void testCorruptBlobLenThrowsIOException() throws Exception {
        Path dir = folder.newFolder("corrupt-len").toPath();
        java.nio.file.Files.createDirectories(dir);
        Path wf = dir.resolve("watermark.dat");

        try (java.io.DataOutputStream dos =
                new java.io.DataOutputStream(java.nio.file.Files.newOutputStream(wf))) {
            dos.writeByte(1);       // version
            dos.writeLong(1L);      // ledgerId
            dos.writeLong(100L);    // offset
            dos.writeInt(1);        // numInstances
            dos.writeInt(1);        // tableCount = 1
            dos.writeInt(-7);       // corrupt blob length (negative)
        }

        LocalWatermarkStore store = new LocalWatermarkStore(dir);
        assertThrows("negative blob length must raise IOException",
                IOException.class, store::load);
    }

    /**
     * Verifies that schema is preserved across an overwrite: saving a snapshot
     * with schema, then a snapshot without schema (empty), results in a loaded
     * snapshot that has no schema.
     */
    @Test
    public void testOverwriteSchemaWithEmptySchema() throws IOException {
        Path dir = folder.newFolder("overwrite-schema").toPath();
        LocalWatermarkStore store = new LocalWatermarkStore(dir);

        // Save with schema
        Table t = buildTable("t");
        Index ix = buildVectorIndex("i", "t");
        store.save(new WatermarkSnapshot(
                new LogSequenceNumber(1, 1), 1,
                Collections.singletonList(t), Collections.singletonList(ix)));

        // Overwrite with empty schema
        store.save(new WatermarkSnapshot(new LogSequenceNumber(2, 2), 1));
        WatermarkSnapshot loaded = store.load();
        assertFalse("overwritten snapshot must have no schema", loaded.hasSchema());
        assertEquals("lsn advanced", 2, loaded.lsn.ledgerId);
    }
}
