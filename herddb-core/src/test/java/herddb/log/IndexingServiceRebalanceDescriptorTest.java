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

package herddb.log;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import herddb.index.vector.VectorIndexManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Table;
import herddb.utils.Bytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 * Round-trip tests for the {@link IndexingServiceRebalanceDescriptor} payload
 * and the {@link LogEntryType#INDEXING_SERVICE_REBALANCE} {@link LogEntry}
 * envelope.
 *
 * @author enrico.olivelli
 */
public class IndexingServiceRebalanceDescriptorTest {

    private Table makeTable(String name) {
        return Table.builder()
                .name(name)
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private Index makeVectorIndex(String name, String table, int numShards) {
        return Index.builder()
                .name(name)
                .table(table)
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_NUM_SHARDS, String.valueOf(numShards))
                .build();
    }

    @Test
    public void roundTripEmpty() throws Exception {
        IndexingServiceRebalanceDescriptor d = new IndexingServiceRebalanceDescriptor(
                42L, 1, Collections.emptyList(), Collections.emptyList());
        IndexingServiceRebalanceDescriptor restored =
                IndexingServiceRebalanceDescriptor.deserialize(d.serialize());
        assertEquals(42L, restored.epoch);
        assertEquals(1, restored.defaultNumInstances);
        assertEquals(0, restored.tables.size());
        assertEquals(0, restored.vectorIndexes.size());
    }

    @Test
    public void roundTripWithTablesAndIndexes() throws Exception {
        Table t1 = makeTable("t1");
        Table t2 = makeTable("t2");
        Index ix1 = makeVectorIndex("vidx_a", "t1", 4);
        Index ix2 = makeVectorIndex("vidx_b", "t1", 8);
        IndexingServiceRebalanceDescriptor d = new IndexingServiceRebalanceDescriptor(
                System.currentTimeMillis(), 4,
                Arrays.asList(t1, t2), Arrays.asList(ix1, ix2));
        IndexingServiceRebalanceDescriptor restored =
                IndexingServiceRebalanceDescriptor.deserialize(d.serialize());

        assertEquals(d.epoch, restored.epoch);
        assertEquals(4, restored.defaultNumInstances);
        assertEquals(2, restored.tables.size());
        assertEquals("t1", restored.tables.get(0).name);
        assertEquals("t2", restored.tables.get(1).name);
        assertEquals(2, restored.vectorIndexes.size());
        assertEquals("4", restored.vectorIndexes.get(0).properties.get(VectorIndexManager.PROP_NUM_SHARDS));
        assertEquals("8", restored.vectorIndexes.get(1).properties.get(VectorIndexManager.PROP_NUM_SHARDS));
    }

    @Test
    public void zeroNumInstancesRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new IndexingServiceRebalanceDescriptor(1, 0,
                        Collections.emptyList(), Collections.emptyList()));
    }

    @Test
    public void logEntryRoundTrip() throws Exception {
        IndexingServiceRebalanceDescriptor d = new IndexingServiceRebalanceDescriptor(
                7L, 8,
                Collections.singletonList(makeTable("t1")),
                Collections.singletonList(makeVectorIndex("v", "t1", 4)));
        LogEntry entry = LogEntryFactory.indexingServiceRebalance(d);
        assertEquals(LogEntryType.INDEXING_SERVICE_REBALANCE, entry.type);
        assertEquals(0L, entry.transactionId);

        LogEntry restoredEntry = LogEntry.deserialize(entry.serialize());
        assertEquals(LogEntryType.INDEXING_SERVICE_REBALANCE, restoredEntry.type);
        assertEquals(0L, restoredEntry.transactionId);
        assertArrayEquals(entry.value.to_array(), restoredEntry.value.to_array());

        IndexingServiceRebalanceDescriptor restoredDescriptor =
                IndexingServiceRebalanceDescriptor.deserialize(restoredEntry.value.to_array());
        assertEquals(7L, restoredDescriptor.epoch);
        assertEquals(8, restoredDescriptor.defaultNumInstances);
        assertEquals("4", restoredDescriptor.vectorIndexes.get(0).properties
                .get(VectorIndexManager.PROP_NUM_SHARDS));
    }

    /**
     * Regression guard: existing log-entry types must still round-trip
     * unchanged after the new INDEXING_SERVICE_REBALANCE branch was added to
     * the switch statements.
     */
    @Test
    public void otherEntryTypesUnchangedRegression() throws Exception {
        Table t = makeTable("t1");
        LogEntry insert = new LogEntry(123L, LogEntryType.INSERT, 0L, 7,
                Bytes.from_string("k1"), Bytes.from_string("v1"));
        LogEntry restored = LogEntry.deserialize(insert.serialize());
        assertEquals(LogEntryType.INSERT, restored.type);
        assertEquals(7, restored.tableId);
        assertEquals(Bytes.from_string("k1"), restored.key);
        assertEquals(Bytes.from_string("v1"), restored.value);

        LogEntry createIndex = LogEntryFactory.createIndex(
                makeVectorIndex("v", "t1", 4), null);
        LogEntry restoredCreate = LogEntry.deserialize(createIndex.serialize());
        assertEquals(LogEntryType.CREATE_INDEX, restoredCreate.type);
        assertArrayEquals(createIndex.value.to_array(), restoredCreate.value.to_array());

        // Round-trip a list to ensure the new factory does not interfere with iteration
        List<LogEntry> entries = new ArrayList<>();
        entries.add(LogEntryFactory.beginTransaction(99L));
        entries.add(LogEntryFactory.commitTransaction(99L));
        entries.add(LogEntryFactory.noop());
        for (LogEntry e : entries) {
            LogEntry r = LogEntry.deserialize(e.serialize());
            assertEquals(e.type, r.type);
            assertEquals(e.transactionId, r.transactionId);
        }
    }
}
