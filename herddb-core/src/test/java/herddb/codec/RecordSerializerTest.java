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
package herddb.codec;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.ColumnsList;
import herddb.model.Record;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.MapDataAccessor;
import herddb.utils.RawString;
import herddb.utils.VisibleByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.junit.Test;

/**
 * @author enrico.olivelli
 */
public class RecordSerializerTest {

    public RecordSerializerTest() {
    }

    @Test
    public void testToBean() {
        Table table = Table.builder()
                .name("t1")
                .column("pk", ColumnTypes.STRING)
                .column("a", ColumnTypes.STRING)
                .column("b", ColumnTypes.LONG)
                .column("c", ColumnTypes.INTEGER)
                .column("d", ColumnTypes.TIMESTAMP)
                .column("e", ColumnTypes.BYTEARRAY)
                .primaryKey("pk")
                .build();
        Record record = RecordSerializer.makeRecord(table, "pk", "a",
                "a", "test", "b", 1L, "c", 2, "d", new java.sql.Timestamp(System.currentTimeMillis()), "e", "foo".getBytes(StandardCharsets.UTF_8));
        Map<String, Object> toBean = RecordSerializer.toBean(record, table);
    }

    @Test
    public void testConvert() {
        testTimestamp("2015-03-29 01:00:00", "UTC", 1427590800000L);
        testTimestamp("2015-03-29 02:00:00", "UTC", 1427594400000L);
        testTimestamp("2015-03-29 03:00:00", "UTC", 1427598000000L);

    }

    private static void testTimestamp(String testCase, String timezone, long expectedResult) throws StatementExecutionException {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS ZZZ");
        fmt.setTimeZone(TimeZone.getTimeZone(timezone));
        java.sql.Timestamp result = (java.sql.Timestamp) RecordSerializer.convert(ColumnTypes.TIMESTAMP, testCase);
        String formattedResult = fmt.format(result);
        System.out.println("result:" + result.getTime());
        System.out.println("test case " + testCase + ", result:" + formattedResult);
        long delta = (expectedResult - result.getTime()) / (1000 * 60 * 60);
        assertEquals("failed for " + testCase + " delta is " + delta + " h, result is " + formattedResult, expectedResult, result.getTime());
    }

    @Test
    public void testSerializeWithNullAndNonNullTypes() {
        byte[] iBytes = RecordSerializer.serialize(new Integer(10), ColumnTypes.INTEGER);
        byte[] iBytesNonNullType = RecordSerializer.serialize(new Integer(10), ColumnTypes.NOTNULL_INTEGER);
        assertArrayEquals(iBytes, iBytesNonNullType);

        byte[] lBytes = RecordSerializer.serialize(new Long(1982), ColumnTypes.NOTNULL_LONG);
        byte[] lBytesNonNullType = RecordSerializer.serialize(new Long(1982), ColumnTypes.LONG);
        assertArrayEquals(lBytes, lBytesNonNullType);

        byte[] sBytes = RecordSerializer.serialize("test", ColumnTypes.STRING);
        byte[] sBytesNonNullType = RecordSerializer.serialize("test", ColumnTypes.NOTNULL_STRING);
        assertArrayEquals(sBytes, sBytesNonNullType);

        byte[] dBytes = RecordSerializer.serialize(10.01d, ColumnTypes.DOUBLE);
        byte[] dBytesNonNullType = RecordSerializer.serialize(10.01d, ColumnTypes.NOTNULL_DOUBLE);
        assertArrayEquals(dBytes, dBytesNonNullType);

        byte[] bBytes = RecordSerializer.serialize(Boolean.TRUE, ColumnTypes.BOOLEAN);
        byte[] bBytesNonNullType = RecordSerializer.serialize(Boolean.TRUE, ColumnTypes.NOTNULL_BOOLEAN);
        assertArrayEquals(bBytes, bBytesNonNullType);

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        byte[] tBytes = RecordSerializer.serialize(timestamp, ColumnTypes.TIMESTAMP);
        byte[] tBytesNonNullType = RecordSerializer.serialize(timestamp, ColumnTypes.NOTNULL_TIMESTAMP);
        assertArrayEquals(tBytes, tBytesNonNullType);
    }

    @Test
    public void testSerializeThrowsExceptionOnNullObject() {
        assertNull(RecordSerializer.serialize(null, ColumnTypes.STRING));
    }

    @Test
    public void testDeserializeWithNullAndNonNullTypes() {
        byte[] byteValueForInt = Bytes.from_int(1000).to_array();
        int iValue = (int) RecordSerializer.deserialize(byteValueForInt, ColumnTypes.INTEGER);
        assertEquals(iValue, 1000);

        int iValueNonNullType = (int) RecordSerializer.deserialize(byteValueForInt, ColumnTypes.NOTNULL_INTEGER);
        assertEquals(iValueNonNullType, 1000);

        byte[] byteValueForLong = Bytes.from_long(99999).to_array();
        long lValue = (long) RecordSerializer.deserialize(byteValueForLong, ColumnTypes.LONG);
        assertEquals(lValue, 99999);

        long lValueNonNullType = (long) RecordSerializer.deserialize(byteValueForLong, ColumnTypes.NOTNULL_LONG);
        assertEquals(lValueNonNullType, 99999);

        byte[] strValueAsByteArray = Bytes.from_string("test").to_array();
        RawString sValue = (RawString) RecordSerializer.deserialize(strValueAsByteArray, ColumnTypes.STRING);
        assertEquals(sValue, "test");

        RawString sValueNonNullType = (RawString) RecordSerializer.deserialize(strValueAsByteArray, ColumnTypes.NOTNULL_STRING);
        assertEquals(sValueNonNullType, "test");

        byte[] booleanToByteArray = Bytes.booleanToByteArray(Boolean.TRUE);
        Boolean bValue = (Boolean) RecordSerializer.deserialize(booleanToByteArray, ColumnTypes.BOOLEAN);
        assertEquals(bValue, Boolean.TRUE);

        booleanToByteArray = Bytes.booleanToByteArray(Boolean.FALSE);
        bValue = (Boolean) RecordSerializer.deserialize(booleanToByteArray, ColumnTypes.NOTNULL_BOOLEAN);
        assertEquals(bValue, Boolean.FALSE);

        byte[] doubleToByteArray = Bytes.doubleToByteArray(Double.valueOf(11.0120d));
        Double dValue = (Double) RecordSerializer.deserialize(doubleToByteArray, ColumnTypes.DOUBLE);
        assertEquals(dValue, Double.valueOf(11.0120d));

        dValue = (Double) RecordSerializer.deserialize(doubleToByteArray, ColumnTypes.NOTNULL_DOUBLE);
        assertEquals(dValue, Double.valueOf(11.0120d));

        Timestamp ts = Timestamp.valueOf("2020-07-04 13:17:47.221");
        byte[] tsToByteArray = Bytes.timestampToByteArray(ts);
        Timestamp tsValue = (Timestamp) RecordSerializer.deserialize(tsToByteArray, ColumnTypes.TIMESTAMP);
        assertEquals(tsValue, ts);

        tsValue = (Timestamp) RecordSerializer.deserialize(tsToByteArray, ColumnTypes.NOTNULL_TIMESTAMP);
        assertEquals(tsValue, ts);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSerializeThrowsExceptionOnUnknownType() {
        RecordSerializer.serialize("test", ColumnTypes.ANYTYPE);
    }

    @Test
    public void testSerializeIndexKey() throws Exception {
        Map<String, Object> data = new HashMap<>();
        data.put("k1", "key1");
        data.put("n1", 1);
        data.put("l1", 9L);
        data.put("s1", "aa");
        data.put("n2", null);
        data.put("s2", null);
        MapDataAccessor map = new MapDataAccessor(data, new String[]{"k1", "n1", "l1", "s1", "n2", "s2"});

        testSerializeIndexKey(map, Bytes.from_string("key1"), Column.column("k1", ColumnTypes.STRING));
        testSerializeIndexKey(map, Bytes.from_int(1), Column.column("n1", ColumnTypes.INTEGER));
        testSerializeIndexKey(map, Bytes.from_long(9), Column.column("l1", ColumnTypes.LONG));
        testSerializeIndexKey(map, Bytes.from_string("aa"), Column.column("s1", ColumnTypes.STRING));

        // composite keys without nulls
        testSerializeIndexKey(map, concat(varInt(4), Bytes.from_string("key1"), varInt(4), Bytes.from_int(1)),
                Column.column("k1", ColumnTypes.STRING), Column.column("n1", ColumnTypes.INTEGER));
        testSerializeIndexKey(map, concat(varInt(4), Bytes.from_string("key1"), varInt(4), Bytes.from_int(1), varInt(2), Bytes.from_string("aa")),
                Column.column("k1", ColumnTypes.STRING), Column.column("n1", ColumnTypes.INTEGER), Column.column("s1", ColumnTypes.STRING));
        testSerializeIndexKey(map, concat(varInt(4), Bytes.from_string("key1"), varInt(4), Bytes.from_int(1), varInt(2), Bytes.from_string("aa")),
                Column.column("k1", ColumnTypes.STRING), Column.column("n1", ColumnTypes.INTEGER), Column.column("s1", ColumnTypes.STRING));

        // single null value
        testSerializeIndexKey(map, null, Column.column("s2", ColumnTypes.STRING));

        // multicolumn, first column is a null
        testSerializeIndexKey(map, null, Column.column("s2", ColumnTypes.STRING), Column.column("k1", ColumnTypes.STRING));

        // multicolumn, two null columns
        testSerializeIndexKey(map, null, Column.column("s2", ColumnTypes.STRING), Column.column("n1", ColumnTypes.INTEGER));

        // multicolumn, first column is not null, second column is NULL
        testSerializeIndexKey(map, concat(varInt(4), Bytes.from_string("key1")), Column.column("k1", ColumnTypes.STRING), Column.column("s2", ColumnTypes.STRING));

        // multicolumn, first and second columns are not null, the third is null
        testSerializeIndexKey(map, concat(varInt(4), Bytes.from_string("key1"), varInt(4), Bytes.from_int(1)),
                Column.column("k1", ColumnTypes.STRING), Column.column("n1", ColumnTypes.INTEGER), Column.column("s2", ColumnTypes.STRING));

    }

    // ── issue #377 optimisation tests ──────────────────────────────────────────

    /**
     * Helper: build a cache-free {@link Record} (ensures {@link Record#getDataAccessor}
     * returns {@link DataAccessorForFullRecord}, not {@link MapDataAccessor}).
     */
    private static Record makeCacheFreeRecord(Table table, Object... kv) {
        Record r = RecordSerializer.makeRecord(table, kv);
        return new Record(r.key, r.value);
    }

    /**
     * Helper: assert that the optimised (DataAccessorForFullRecord) and reference
     * (MapDataAccessor) paths produce identical index keys for the given column.
     */
    private void assertIndexKeyEquivalence(Table table, Record record,
            String columnName, int columnType, Object columnValue) {
        Column col = Column.column(columnName, columnType);
        ColumnsList index = new ColumnsListImpl(new Column[]{col});

        Map<String, Object> mapData = new HashMap<>();
        mapData.put("pk", record.key.to_int()); // works for INTEGER PK used in these tests
        if (columnValue != null) {
            mapData.put(columnName, columnValue);
        }
        DataAccessor mapAccessor = new MapDataAccessor(mapData, table.columnNames);
        Bytes expectedKey = RecordSerializer.serializeIndexKey(mapAccessor, index, index.getPrimaryKey());

        DataAccessor rawAccessor = record.getDataAccessor(table);
        assertTrue("Expected DataAccessorForFullRecord", rawAccessor instanceof DataAccessorForFullRecord);
        Bytes actualKey = RecordSerializer.serializeIndexKey(rawAccessor, index, index.getPrimaryKey());

        assertEquals("Index key mismatch for type " + columnType, expectedKey, actualKey);
    }

    // ── FLOATARRAY ─────────────────────────────────────────────────────────────

    /**
     * Happy path: DataAccessorForFullRecord with a FLOATARRAY column produces the same
     * index key as MapDataAccessor.  Also asserts {@link Bytes#isShared()} == true so
     * a regression to the allocating slow path is detected.
     */
    @Test
    public void testSerializeIndexKeyFloatArrayNoCopy() {
        Table table = Table.builder()
                .name("t1")
                .column("pk", ColumnTypes.INTEGER)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();

        float[] vector = {1.0f, 2.0f, 3.0f, 4.0f};
        Record record = makeCacheFreeRecord(table, "pk", 1, "vec", vector);

        Column vecColumn = Column.column("vec", ColumnTypes.FLOATARRAY);
        ColumnsList index = new ColumnsListImpl(new Column[]{vecColumn});

        // Reference result via MapDataAccessor (general path)
        Map<String, Object> mapData = new HashMap<>();
        mapData.put("pk", 1);
        mapData.put("vec", vector);
        DataAccessor mapAccessor = new MapDataAccessor(mapData, table.columnNames);
        Bytes expectedKey = RecordSerializer.serializeIndexKey(mapAccessor, index, index.getPrimaryKey());

        // Optimised path: DataAccessorForFullRecord
        DataAccessor rawAccessor = record.getDataAccessor(table);
        assertTrue("Expected DataAccessorForFullRecord", rawAccessor instanceof DataAccessorForFullRecord);
        Bytes actualKey = RecordSerializer.serializeIndexKey(rawAccessor, index, index.getPrimaryKey());

        assertEquals(expectedKey, actualKey);
        // The no-copy path returns a slice into the record value buffer; isShared()
        // returns true whenever offset > 0 or length < buffer.length.  If this
        // assertion fails the optimisation has silently regressed to the allocating path.
        assertTrue("No-copy path should return a shared (sliced) Bytes", actualKey.isShared());
    }

    /** Null FLOATARRAY value is absent from the record bytes → serializeIndexKey returns null. */
    @Test
    public void testSerializeIndexKeyFloatArrayNullValue() {
        Table table = Table.builder()
                .name("t1")
                .column("pk", ColumnTypes.INTEGER)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();

        Record record = makeCacheFreeRecord(table, "pk", 1); // no vec → null

        Column vecColumn = Column.column("vec", ColumnTypes.FLOATARRAY);
        ColumnsList index = new ColumnsListImpl(new Column[]{vecColumn});

        DataAccessor rawAccessor = record.getDataAccessor(table);
        assertTrue("Expected DataAccessorForFullRecord", rawAccessor instanceof DataAccessorForFullRecord);

        assertNull("Expected null for absent FLOATARRAY column",
                RecordSerializer.serializeIndexKey(rawAccessor, index, index.getPrimaryKey()));
    }

    /** Null FLOATARRAY column with nulls forbidden → serializeIndexKey must throw. */
    @Test(expected = IllegalArgumentException.class)
    public void testSerializeIndexKeyFloatArrayNullValueThrowsWhenNullsForbidden() {
        Table table = Table.builder()
                .name("t1")
                .column("pk", ColumnTypes.INTEGER)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();

        Record record = makeCacheFreeRecord(table, "pk", 1); // vec is null
        Column vecColumn = Column.column("vec", ColumnTypes.FLOATARRAY);
        ColumnsList index = new ColumnsListImplNoNulls(new Column[]{vecColumn});

        DataAccessor rawAccessor = record.getDataAccessor(table);
        RecordSerializer.serializeIndexKey(rawAccessor, index, index.getPrimaryKey());
    }

    /** Empty float[0] → both paths must produce equal (empty) Bytes. */
    @Test
    public void testSerializeIndexKeyFloatArrayEmpty() {
        Table table = Table.builder()
                .name("t1")
                .column("pk", ColumnTypes.INTEGER)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();

        float[] empty = new float[0];
        Record record = makeCacheFreeRecord(table, "pk", 1, "vec", empty);

        assertIndexKeyEquivalence(table, record, "vec", ColumnTypes.FLOATARRAY, empty);
    }

    /** Large vector (4 096 floats) confirms the len*4 arithmetic and slice size. */
    @Test
    public void testSerializeIndexKeyFloatArrayLargeVector() {
        Table table = Table.builder()
                .name("t1")
                .column("pk", ColumnTypes.INTEGER)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();

        float[] bigVec = new float[4096];
        for (int i = 0; i < bigVec.length; i++) {
            bigVec[i] = i * 0.001f;
        }
        Record record = makeCacheFreeRecord(table, "pk", 1, "vec", bigVec);

        assertIndexKeyEquivalence(table, record, "vec", ColumnTypes.FLOATARRAY, bigVec);
    }

    /** validateIndexableValue must not throw for a valid non-null FLOATARRAY column. */
    @Test
    public void testValidateIndexableValueFloatArrayNoCopy() {
        Table table = Table.builder()
                .name("t1")
                .column("pk", ColumnTypes.INTEGER)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();

        Record record = makeCacheFreeRecord(table, "pk", 1, "vec", new float[]{0.5f, 1.5f, -3.0f});

        Column vecColumn = Column.column("vec", ColumnTypes.FLOATARRAY);
        ColumnsList index = new ColumnsListImpl(new Column[]{vecColumn});

        DataAccessor rawAccessor = record.getDataAccessor(table);
        assertTrue("Expected DataAccessorForFullRecord", rawAccessor instanceof DataAccessorForFullRecord);
        RecordSerializer.validateIndexableValue(rawAccessor, index, index.getPrimaryKey()); // must not throw
    }

    /** Null FLOATARRAY with nulls allowed → validateIndexableValue must not throw. */
    @Test
    public void testValidateIndexableValueFloatArrayNullAllowed() {
        Table table = Table.builder()
                .name("t1")
                .column("pk", ColumnTypes.INTEGER)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();

        Record record = makeCacheFreeRecord(table, "pk", 1); // no vec → null

        Column vecColumn = Column.column("vec", ColumnTypes.FLOATARRAY);
        ColumnsList index = new ColumnsListImpl(new Column[]{vecColumn}); // allowNulls = true

        DataAccessor rawAccessor = record.getDataAccessor(table);
        RecordSerializer.validateIndexableValue(rawAccessor, index, index.getPrimaryKey()); // must not throw
    }

    /** Null FLOATARRAY with nulls forbidden → validateIndexableValue must throw. */
    @Test(expected = IllegalArgumentException.class)
    public void testValidateIndexableValueFloatArrayNullForbidden() {
        Table table = Table.builder()
                .name("t1")
                .column("pk", ColumnTypes.INTEGER)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();

        Record record = makeCacheFreeRecord(table, "pk", 1); // vec is null

        Column vecColumn = Column.column("vec", ColumnTypes.FLOATARRAY);
        ColumnsList index = new ColumnsListImplNoNulls(new Column[]{vecColumn});

        DataAccessor rawAccessor = record.getDataAccessor(table);
        RecordSerializer.validateIndexableValue(rawAccessor, index, index.getPrimaryKey());
    }

    /** Multi-column index (STRING + FLOATARRAY): validates the multi-column branch. */
    @Test
    public void testValidateIndexableValueFloatArrayMultiColumnIndex() {
        Table table = Table.builder()
                .name("t1")
                .column("pk", ColumnTypes.INTEGER)
                .column("tag", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();

        float[] vector = {1.0f, 2.0f};
        Record record = makeCacheFreeRecord(table, "pk", 1, "tag", RawString.of("a"), "vec", vector);

        // Multi-column index: [tag, vec]
        ColumnsList index = new ColumnsListImpl(new Column[]{
            Column.column("tag", ColumnTypes.STRING),
            Column.column("vec", ColumnTypes.FLOATARRAY)
        });

        DataAccessor rawAccessor = record.getDataAccessor(table);
        assertTrue("Expected DataAccessorForFullRecord", rawAccessor instanceof DataAccessorForFullRecord);
        RecordSerializer.validateIndexableValue(rawAccessor, index, index.getPrimaryKey()); // must not throw

        // Also verify with null vec (nulls allowed)
        Record recordNullVec = makeCacheFreeRecord(table, "pk", 2, "tag", RawString.of("b"));
        DataAccessor rawNullVec = recordNullVec.getDataAccessor(table);
        RecordSerializer.validateIndexableValue(rawNullVec, index, index.getPrimaryKey()); // must not throw
    }

    // ── Other index types — equivalence tests (verifies INTEGER/LONG fall through) ──

    /** STRING single-column index: optimised and reference paths must agree. */
    @Test
    public void testSerializeIndexKeyStringNoCopy() {
        Table table = Table.builder()
                .name("t1")
                .column("pk", ColumnTypes.INTEGER)
                .column("name", ColumnTypes.STRING)
                .primaryKey("pk")
                .build();

        Record record = makeCacheFreeRecord(table, "pk", 1, "name", RawString.of("hello"));
        assertIndexKeyEquivalence(table, record, "name", ColumnTypes.STRING, RawString.of("hello"));
    }

    /** DOUBLE single-column index: optimised and reference paths must agree. */
    @Test
    public void testSerializeIndexKeyDoubleNoCopy() {
        Table table = Table.builder()
                .name("t1")
                .column("pk", ColumnTypes.INTEGER)
                .column("score", ColumnTypes.DOUBLE)
                .primaryKey("pk")
                .build();

        Record record = makeCacheFreeRecord(table, "pk", 1, "score", 3.14);
        assertIndexKeyEquivalence(table, record, "score", ColumnTypes.DOUBLE, 3.14);
    }

    /**
     * INTEGER single-column index: sign-flip means the optimisation is intentionally
     * skipped; the general path must still produce the correct result.
     */
    @Test
    public void testSerializeIndexKeyIntegerFallsThrough() {
        Table table = Table.builder()
                .name("t1")
                .column("pk", ColumnTypes.INTEGER)
                .column("num", ColumnTypes.INTEGER)
                .primaryKey("pk")
                .build();

        Record record = makeCacheFreeRecord(table, "pk", 1, "num", 42);
        assertIndexKeyEquivalence(table, record, "num", ColumnTypes.INTEGER, 42);
    }

    /**
     * LONG single-column index: sign-flip means the optimisation is intentionally
     * skipped; the general path must still produce the correct result.
     */
    @Test
    public void testSerializeIndexKeyLongFallsThrough() {
        Table table = Table.builder()
                .name("t1")
                .column("pk", ColumnTypes.INTEGER)
                .column("ts", ColumnTypes.LONG)
                .primaryKey("pk")
                .build();

        Record record = makeCacheFreeRecord(table, "pk", 1, "ts", 123456789L);
        assertIndexKeyEquivalence(table, record, "ts", ColumnTypes.LONG, 123456789L);
    }

    // ── Inner helpers ───────────────────────────────────────────────────────────

    private static Bytes varInt(int i) throws Exception {
        VisibleByteArrayOutputStream res = new VisibleByteArrayOutputStream(1);
        ExtendedDataOutputStream oo = new ExtendedDataOutputStream(res);
        oo.writeVInt(i);
        return Bytes.from_array(res.toByteArrayNoCopy());
    }

    private static Bytes concat(Bytes... arrays) {
        VisibleByteArrayOutputStream res = new VisibleByteArrayOutputStream();
        for (Bytes a : arrays) {
            res.write(a.getBuffer(), a.getOffset(), a.getLength());
        }
        return Bytes.from_array(res.toByteArrayNoCopy());
    }

    private void testSerializeIndexKey(DataAccessor record, Bytes expectedResult, Column... indexedColumns) {
        ColumnsList index = new ColumnsListImpl(indexedColumns);
        Bytes result = RecordSerializer.serializeIndexKey(record, index, index.getPrimaryKey());
        assertEquals(expectedResult, result);
    }

    private class ColumnsListImpl implements ColumnsList {

        private final Column[] indexedColumns;
        private final String[] primaryKey;

        public ColumnsListImpl(Column[] indexedColumns) {
            this.indexedColumns = indexedColumns;
            String[] newPrimaryKey = new String[indexedColumns.length];
            for (int i = 0; i < indexedColumns.length; i++) {
                newPrimaryKey[i] = indexedColumns[i].name;
            }
            this.primaryKey = newPrimaryKey;
        }

        @Override
        public Column[] getColumns() {
            return indexedColumns;
        }

        @Override
        public Column getColumn(String name) {
            for (Column c : getColumns()) {
                if (c.getName().equals(name)) {
                    return c;
                }
            }
            throw new IllegalArgumentException(name);
        }

        @Override
        public String[] getPrimaryKey() {
            return primaryKey;
        }

        @Override
        public boolean allowNullsForIndexedValues() {
            return true;
        }
    }

    /** {@link ColumnsListImpl} variant with {@code allowNullsForIndexedValues() = false}. */
    private class ColumnsListImplNoNulls extends ColumnsListImpl {

        public ColumnsListImplNoNulls(Column[] indexedColumns) {
            super(indexedColumns);
        }

        @Override
        public boolean allowNullsForIndexedValues() {
            return false;
        }
    }

}
