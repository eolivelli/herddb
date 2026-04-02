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
import static org.junit.Assert.assertTrue;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Table;
import java.util.Collection;
import org.junit.Before;
import org.junit.Test;

public class SchemaTrackerTest {

    private SchemaTracker tracker;

    @Before
    public void setUp() {
        tracker = new SchemaTracker();
    }

    private static Table buildTable(String name) {
        return Table.builder()
                .tablespace("default")
                .name(name)
                .column("id", ColumnTypes.LONG)
                .column("data", ColumnTypes.STRING)
                .primaryKey("id")
                .build();
    }

    private static Table buildTableWithVector(String name) {
        return Table.builder()
                .tablespace("default")
                .name(name)
                .column("id", ColumnTypes.LONG)
                .column("embedding", ColumnTypes.FLOATARRAY)
                .primaryKey("id")
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

        LogEntry dropEntry = LogEntryFactory.dropTable("mytable", null);
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
