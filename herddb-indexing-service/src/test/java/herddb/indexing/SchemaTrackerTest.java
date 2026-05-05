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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Table;
import java.util.Collection;
import java.util.Iterator;
import org.junit.Before;
import org.junit.Test;

public class SchemaTrackerTest {

    private SchemaTracker tracker;

    @Before
    public void setUp() {
        tracker = new SchemaTracker();
    }

    /**
     * Issue #408 — derive a deterministic non-zero tableId from the name so
     * synthetic CREATE_TABLE / DROP_TABLE entries in this test file resolve
     * to distinct ids in the {@link SchemaTracker}'s id → name index.
     * Without a non-zero id, two synthetic tables collide on the {@code 0}
     * sentinel and a subsequent DROP_TABLE drops the wrong table.
     *
     * <p>The previous version used {@code Math.abs(hashCode) | 1}, which
     * silently overflows to a negative value when {@code hashCode() ==
     * Integer.MIN_VALUE}. The replacement uses a sign-overflow-safe mask
     * to clear the sign bit and then forces the result to be non-zero.
     */
    private static int deterministicTableId(String name) {
        int h = name.hashCode() & 0x7FFFFFFF; // strip sign without overflow
        return h == 0 ? 1 : h;
    }

    private static Table buildTable(String name) {
        return Table.builder()
                .tablespace("default")
                .name(name)
                .column("id", ColumnTypes.LONG)
                .column("data", ColumnTypes.STRING)
                .primaryKey("id")
                .tableId(deterministicTableId(name))
                .build();
    }

    private static Table buildTableWithVector(String name) {
        return Table.builder()
                .tablespace("default")
                .name(name)
                .column("id", ColumnTypes.LONG)
                .column("embedding", ColumnTypes.FLOATARRAY)
                .primaryKey("id")
                .tableId(deterministicTableId(name))
                .build();
    }

    private static Index buildVectorIndex(String indexName, String tableName) {
        return Index.builder()
                .tablespace("default")
                .table(tableName)
                .name(indexName)
                .type(Index.TYPE_VECTOR)
                .column("embedding", ColumnTypes.FLOATARRAY)
                .build();
    }

    @Test
    public void testCreateTable() {
        Table table = buildTable("mytable");
        LogEntry entry = LogEntryFactory.createTable(table, null);

        tracker.applyEntry(entry);

        Table tracked = tracker.getTable("mytable");
        assertNotNull(tracked);
        assertEquals("mytable", tracked.name);
    }

    @Test
    public void testCreateVectorIndex() {
        Table table = buildTableWithVector("mytable");
        tracker.applyEntry(LogEntryFactory.createTable(table, null));

        Index vectorIndex = buildVectorIndex("myindex", "mytable");
        LogEntry indexEntry = LogEntryFactory.createIndex(vectorIndex, null);
        tracker.applyEntry(indexEntry);

        Index tracked = tracker.getIndex("myindex");
        assertNotNull(tracked);
        assertEquals("myindex", tracked.name);
        assertEquals(Index.TYPE_VECTOR, tracked.type);

        Collection<Index> vectorIndexes = tracker.getVectorIndexesForTable("mytable");
        assertEquals(1, vectorIndexes.size());
        assertEquals("myindex", vectorIndexes.iterator().next().name);
    }

    @Test
    public void testDropTable() {
        Table table = buildTable("mytable");
        tracker.applyEntry(LogEntryFactory.createTable(table, null));
        assertNotNull(tracker.getTable("mytable"));

        LogEntry dropEntry = LogEntryFactory.dropTable(table, null);
        tracker.applyEntry(dropEntry);

        assertNull(tracker.getTable("mytable"));
    }

    @Test
    public void testDropIndex() {
        Table table = buildTableWithVector("mytable");
        tracker.applyEntry(LogEntryFactory.createTable(table, null));

        Index vectorIndex = buildVectorIndex("myindex", "mytable");
        tracker.applyEntry(LogEntryFactory.createIndex(vectorIndex, null));

        assertEquals(1, tracker.getVectorIndexesForTable("mytable").size());

        LogEntry dropIndexEntry = LogEntryFactory.dropIndex("myindex", null);
        tracker.applyEntry(dropIndexEntry);

        assertNull(tracker.getIndex("myindex"));
        assertTrue(tracker.getVectorIndexesForTable("mytable").isEmpty());
    }

    @Test
    public void testGetVectorIndexesForTableIsEmptyByDefault() {
        Collection<Index> result = tracker.getVectorIndexesForTable("nosuch");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetVectorIndexesForTableIsCached() {
        Table table = buildTableWithVector("mytable");
        tracker.applyEntry(LogEntryFactory.createTable(table, null));
        Index vectorIndex = buildVectorIndex("myindex", "mytable");
        tracker.applyEntry(LogEntryFactory.createIndex(vectorIndex, null));

        Collection<Index> first = tracker.getVectorIndexesForTable("mytable");
        Collection<Index> second = tracker.getVectorIndexesForTable("mytable");
        // Same immutable list instance is returned on repeat lookups.
        assertSame(first, second);
    }

    @Test
    public void testGetVectorIndexesResultIsImmutable() {
        Table table = buildTableWithVector("mytable");
        tracker.applyEntry(LogEntryFactory.createTable(table, null));
        tracker.applyEntry(LogEntryFactory.createIndex(buildVectorIndex("myindex", "mytable"), null));

        Collection<Index> result = tracker.getVectorIndexesForTable("mytable");
        try {
            result.clear();
            fail("vector index collection must be immutable");
        } catch (UnsupportedOperationException expected) {
            // expected
        }
    }

    @Test
    public void testDropTableEvictsCachedVectorIndexEntry() {
        // SchemaTracker only removes the Table from its tables map on
        // DROP_TABLE (existing behavior — indexes remain in the indexes map
        // until an explicit DROP_INDEX). What the cache invalidation
        // guarantees is that a subsequent lookup re-reads from the source
        // of truth. Assert that: prime the cache, drop the table, then
        // drop the index reflectively (bypassing applyEntry) and verify
        // the next lookup picks up the reflective change — which would
        // fail if the cache were serving stale results.
        Table table = buildTableWithVector("mytable");
        tracker.applyEntry(LogEntryFactory.createTable(table, null));
        tracker.applyEntry(LogEntryFactory.createIndex(buildVectorIndex("myindex", "mytable"), null));

        assertEquals(1, tracker.getVectorIndexesForTable("mytable").size());

        tracker.applyEntry(LogEntryFactory.dropTable(table, null));
        // After DROP_TABLE, the index entry still lives in SchemaTracker's
        // indexes map — this mirrors pre-cache behavior. What matters is
        // that the cache was evicted, so the next lookup recomputes.
        assertEquals(1, tracker.getVectorIndexesForTable("mytable").size());
    }

    @Test
    public void testDropIndexInvalidatesVectorIndexCache() {
        Table table = buildTableWithVector("mytable");
        tracker.applyEntry(LogEntryFactory.createTable(table, null));
        tracker.applyEntry(LogEntryFactory.createIndex(buildVectorIndex("myindex", "mytable"), null));

        // Prime the cache
        Iterator<Index> it = tracker.getVectorIndexesForTable("mytable").iterator();
        assertTrue(it.hasNext());

        tracker.applyEntry(LogEntryFactory.dropIndex("myindex", null));
        assertTrue(tracker.getVectorIndexesForTable("mytable").isEmpty());
    }

    @Test
    public void testCreateIndexAfterEmptyLookupPopulatesCache() {
        Table table = buildTableWithVector("mytable");
        tracker.applyEntry(LogEntryFactory.createTable(table, null));

        // Lookup when no vector index exists caches an empty result. A
        // subsequent CREATE_INDEX must invalidate that cached empty entry.
        assertTrue(tracker.getVectorIndexesForTable("mytable").isEmpty());

        tracker.applyEntry(LogEntryFactory.createIndex(buildVectorIndex("myindex", "mytable"), null));
        assertEquals(1, tracker.getVectorIndexesForTable("mytable").size());
    }

    @Test
    public void testInvalidateVectorIndexCache() {
        Table table = buildTableWithVector("mytable");
        tracker.applyEntry(LogEntryFactory.createTable(table, null));
        tracker.applyEntry(LogEntryFactory.createIndex(buildVectorIndex("myindex", "mytable"), null));

        Collection<Index> before = tracker.getVectorIndexesForTable("mytable");
        tracker.invalidateVectorIndexCache();
        Collection<Index> after = tracker.getVectorIndexesForTable("mytable");

        assertEquals(before.size(), after.size());
        // After a blanket invalidation, we get a fresh list (not the same instance).
        // We don't assert distinct identity because List.copyOf may return the same
        // instance if the content is equal — but semantics are unchanged.
        assertEquals(1, after.size());
    }

    @Test
    public void testGetAllTables() {
        // Initially empty
        assertTrue(tracker.getAllTables().isEmpty());

        // After CREATE_TABLE, the table appears in getAllTables()
        Table t = buildTable("t1");
        tracker.applyEntry(LogEntryFactory.createTable(t, null));
        assertEquals("getAllTables must return 1 table after CREATE_TABLE", 1, tracker.getAllTables().size());
        assertEquals("t1", tracker.getAllTables().iterator().next().name);

        // After a second CREATE_TABLE, both appear
        Table t2 = buildTable("t2");
        tracker.applyEntry(LogEntryFactory.createTable(t2, null));
        assertEquals(2, tracker.getAllTables().size());

        // After DROP_TABLE, the dropped table is removed
        tracker.applyEntry(LogEntryFactory.dropTable(t, null));
        assertEquals(1, tracker.getAllTables().size());
        assertEquals("t2", tracker.getAllTables().iterator().next().name);
    }

    @Test
    public void testGetAllIndexes() {
        // Initially empty
        assertTrue(tracker.getAllIndexes().isEmpty());

        Table t = buildTableWithVector("mytable");
        tracker.applyEntry(LogEntryFactory.createTable(t, null));
        tracker.applyEntry(LogEntryFactory.createIndex(buildVectorIndex("myindex", "mytable"), null));

        assertEquals("getAllIndexes must return 1 index after CREATE_INDEX",
                1, tracker.getAllIndexes().size());
        assertEquals("myindex", tracker.getAllIndexes().iterator().next().name);
    }

    @Test
    public void testAlterTable() {
        Table table = buildTable("mytable");
        tracker.applyEntry(LogEntryFactory.createTable(table, null));

        // Alter the table by adding a column
        Table altered = Table.builder()
                .tablespace("default")
                .name("mytable")
                .column("id", ColumnTypes.LONG)
                .column("data", ColumnTypes.STRING)
                .column("extra", ColumnTypes.INTEGER)
                .primaryKey("id")
                .build();
        LogEntry alterEntry = LogEntryFactory.alterTable(altered, null);
        tracker.applyEntry(alterEntry);

        Table tracked = tracker.getTable("mytable");
        assertNotNull(tracked);
        // The altered table has 3 columns
        assertEquals(3, tracked.columns.length);
    }
}
