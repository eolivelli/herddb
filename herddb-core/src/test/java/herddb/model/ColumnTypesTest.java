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

package herddb.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.sql.Types;
import org.junit.Test;

/**
 * Unit tests for {@link ColumnTypes} utility methods.
 */
public class ColumnTypesTest {

    @Test
    public void typeToStringCoversAllConstants() {
        // Verify that typeToString() returns a meaningful name (no "type?<N>" fallback)
        // for every defined constant.
        assertTypeToString("string",             ColumnTypes.STRING);
        assertTypeToString("long",               ColumnTypes.LONG);
        assertTypeToString("integer",            ColumnTypes.INTEGER);
        assertTypeToString("bytearray",          ColumnTypes.BYTEARRAY);
        assertTypeToString("timestamp",          ColumnTypes.TIMESTAMP);
        assertTypeToString("null",               ColumnTypes.NULL);
        assertTypeToString("double",             ColumnTypes.DOUBLE);
        assertTypeToString("boolean",            ColumnTypes.BOOLEAN);
        assertTypeToString("floatarray",         ColumnTypes.FLOATARRAY);

        assertTypeToString("string not null",    ColumnTypes.NOTNULL_STRING);
        assertTypeToString("integer not null",   ColumnTypes.NOTNULL_INTEGER);
        assertTypeToString("long not null",      ColumnTypes.NOTNULL_LONG);
        assertTypeToString("bytearray not null", ColumnTypes.NOTNULL_BYTEARRAY);
        assertTypeToString("timestamp not null", ColumnTypes.NOTNULL_TIMESTAMP);
        assertTypeToString("double not null",    ColumnTypes.NOTNULL_DOUBLE);
        assertTypeToString("boolean not null",   ColumnTypes.NOTNULL_BOOLEAN);

        // NOTNULL_FLOATARRAY (18) was previously missing and returned "type?18".
        assertTypeToString("floatarray not null", ColumnTypes.NOTNULL_FLOATARRAY);
    }

    @Test
    public void typeToStringDoesNotReturnFallbackForKnownTypes() {
        // Belt-and-suspenders: none of the known types should produce the "type?N" fallback.
        int[] knownTypes = {
            ColumnTypes.STRING, ColumnTypes.LONG, ColumnTypes.INTEGER,
            ColumnTypes.BYTEARRAY, ColumnTypes.TIMESTAMP, ColumnTypes.NULL,
            ColumnTypes.DOUBLE, ColumnTypes.BOOLEAN, ColumnTypes.FLOATARRAY,
            ColumnTypes.NOTNULL_STRING, ColumnTypes.NOTNULL_INTEGER,
            ColumnTypes.NOTNULL_LONG, ColumnTypes.NOTNULL_BYTEARRAY,
            ColumnTypes.NOTNULL_TIMESTAMP, ColumnTypes.NOTNULL_DOUBLE,
            ColumnTypes.NOTNULL_BOOLEAN, ColumnTypes.NOTNULL_FLOATARRAY
        };
        for (int type : knownTypes) {
            String name = ColumnTypes.typeToString(type);
            assertFalse(
                    "typeToString(" + type + ") returned fallback: " + name,
                    name.startsWith("type?")
            );
        }
    }

    @Test
    public void isNotNullDataTypeCoversAllNotNullConstants() {
        // Every NOTNULL_* constant must be recognised as not-null. NOTNULL_FLOATARRAY was
        // previously missing from this switch, so a "floatarray not null" column would not
        // have its nullability enforced.
        assertTrue(ColumnTypes.isNotNullDataType(ColumnTypes.NOTNULL_STRING));
        assertTrue(ColumnTypes.isNotNullDataType(ColumnTypes.NOTNULL_INTEGER));
        assertTrue(ColumnTypes.isNotNullDataType(ColumnTypes.NOTNULL_LONG));
        assertTrue(ColumnTypes.isNotNullDataType(ColumnTypes.NOTNULL_BOOLEAN));
        assertTrue(ColumnTypes.isNotNullDataType(ColumnTypes.NOTNULL_BYTEARRAY));
        assertTrue(ColumnTypes.isNotNullDataType(ColumnTypes.NOTNULL_DOUBLE));
        assertTrue(ColumnTypes.isNotNullDataType(ColumnTypes.NOTNULL_TIMESTAMP));
        assertTrue(ColumnTypes.isNotNullDataType(ColumnTypes.NOTNULL_FLOATARRAY));

        // The nullable variants must return false.
        assertFalse(ColumnTypes.isNotNullDataType(ColumnTypes.STRING));
        assertFalse(ColumnTypes.isNotNullDataType(ColumnTypes.INTEGER));
        assertFalse(ColumnTypes.isNotNullDataType(ColumnTypes.LONG));
        assertFalse(ColumnTypes.isNotNullDataType(ColumnTypes.BOOLEAN));
        assertFalse(ColumnTypes.isNotNullDataType(ColumnTypes.BYTEARRAY));
        assertFalse(ColumnTypes.isNotNullDataType(ColumnTypes.DOUBLE));
        assertFalse(ColumnTypes.isNotNullDataType(ColumnTypes.TIMESTAMP));
        assertFalse(ColumnTypes.isNotNullDataType(ColumnTypes.FLOATARRAY));
    }

    @Test
    public void sqlDataTypeToJdbcTypeCoversFloatarray() {
        // "floatarray" must resolve to an explicit (non-fallthrough) mapping.
        // There is no standard JDBC type for float[]; Types.OTHER is the correct value.
        assertEquals(Types.OTHER, ColumnTypes.sqlDataTypeToJdbcType("floatarray"));

        // Verify the other standard types are still mapped correctly.
        assertEquals(Types.VARCHAR,   ColumnTypes.sqlDataTypeToJdbcType("string"));
        assertEquals(Types.BIGINT,    ColumnTypes.sqlDataTypeToJdbcType("long"));
        assertEquals(Types.INTEGER,   ColumnTypes.sqlDataTypeToJdbcType("integer"));
        assertEquals(Types.BLOB,      ColumnTypes.sqlDataTypeToJdbcType("bytearray"));
        assertEquals(Types.TIMESTAMP, ColumnTypes.sqlDataTypeToJdbcType("timestamp"));
        assertEquals(Types.NULL,      ColumnTypes.sqlDataTypeToJdbcType("null"));
        assertEquals(Types.DOUBLE,    ColumnTypes.sqlDataTypeToJdbcType("double"));
        assertEquals(Types.BOOLEAN,   ColumnTypes.sqlDataTypeToJdbcType("boolean"));
    }

    private static void assertTypeToString(String expected, int type) {
        assertEquals("typeToString(" + type + ")", expected, ColumnTypes.typeToString(type));
    }
}
