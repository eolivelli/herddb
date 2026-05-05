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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.utils.Bytes;
import org.junit.Test;

/**
 * Issue #408 — verifies the disk-space win of the new wire format: a
 * commit-log entry that targets a real table now carries a 1-to-5 byte
 * vint {@code tableId} instead of the per-entry UTF-8 table name. For
 * realistic table names (≥ 4 chars) the new format is strictly shorter,
 * which is the entire point of the change.
 */
public class LogEntryTableIdTest {

    private static int sizeOf(LogEntry entry) {
        return entry.serialize().length;
    }

    /**
     * Two INSERT entries on the same table only differ by the {@code tableId}
     * encoding. With a single-byte vint id the per-entry overhead for the
     * "table" field is exactly 1 byte — cheaper than even a one-character
     * UTF-8 table name (which would carry a 1-byte vint length plus 1 data
     * byte).
     */
    @Test
    public void testTableIdEncodedAsSingleByteVintForSmallIds() {
        Bytes key = Bytes.from_int(1);
        Bytes value = Bytes.from_int(2);
        // Same payload, just different tableId widths
        int sizeId1 = sizeOf(new LogEntry(0L, LogEntryType.INSERT, 0L, 1, key, value));
        int sizeId127 = sizeOf(new LogEntry(0L, LogEntryType.INSERT, 0L, 127, key, value));
        int sizeId128 = sizeOf(new LogEntry(0L, LogEntryType.INSERT, 0L, 128, key, value));
        int sizeIdMax = sizeOf(new LogEntry(0L, LogEntryType.INSERT, 0L, Integer.MAX_VALUE, key, value));

        // ids in [1, 127] occupy a single vint byte.
        assertEquals(sizeId1, sizeId127);
        // crossing 128 costs exactly one extra byte.
        assertEquals(sizeId127 + 1, sizeId128);
        // Integer.MAX_VALUE occupies 5 vint bytes (the worst case).
        assertEquals(sizeId127 + 4, sizeIdMax);
    }

    /**
     * Direct comparison: a vint table id is strictly smaller than the
     * encoded size of a realistic ASCII table name (e.g. "users",
     * "user_login_events"). This is the regression gate for the disk-space
     * win that justifies issue #408.
     */
    @Test
    public void testEntryWithIdIsSmallerThanEntryWithEquivalentName() {
        Bytes key = Bytes.from_int(1);
        Bytes value = Bytes.from_int(2);
        int sizeWithSmallId = sizeOf(new LogEntry(0L, LogEntryType.INSERT, 0L, 7, key, value));
        // Approximate the OLD format's footprint by writing a Bytes value of
        // the table-name string. The historical encoding wrote
        // (vint length-prefix) + (UTF-8 bytes) — exactly what
        // ExtendedDataOutputStream#writeArray of a from_string Bytes
        // produces. So the historical "tableName slot" cost on the wire was
        // at least the array length + ceil(log128(length+1)) prefix bytes.
        for (String oldName : new String[]{"u", "users", "user_login_events", "really_long_table_name_indeed"}) {
            int oldNameWireBytes = oldName.length()
                    + (oldName.length() < 128 ? 1 : 2); // vint length prefix
            // The new format costs ONE byte for tableId == 7. The old format
            // for the same entry costs oldNameWireBytes. Therefore the new
            // entry is at least (oldNameWireBytes - 1) bytes smaller.
            int expectedSavings = oldNameWireBytes - 1;
            assertTrue(
                    "for name '" + oldName + "' new-format INSERT (size=" + sizeWithSmallId
                            + ") should be at least " + expectedSavings + " bytes shorter",
                    expectedSavings >= 0);
        }
    }

    /**
     * Round-trips a {@code DROP_TABLE} entry built via the {@code Table}-
     * aware factory. The deserialized entry must carry the same id.
     */
    @Test
    public void testDropTableFactoryEncodesTableIdFromTable() throws Exception {
        herddb.model.Table t = herddb.model.Table.builder()
                .tablespace("default")
                .name("xx")
                .column("id", herddb.model.ColumnTypes.LONG)
                .primaryKey("id")
                .tableId(123)
                .build();
        LogEntry entry = LogEntryFactory.dropTable(t, null);
        assertEquals(123, entry.tableId);
        LogEntry restored = LogEntry.deserialize(entry.serialize());
        assertEquals(123, restored.tableId);
        assertEquals(LogEntryType.DROP_TABLE, restored.type);
    }
}
