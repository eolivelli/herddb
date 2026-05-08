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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Tests for {@link Index#withProperty(String, String)} (issue #471).
 *
 * @author enrico.olivelli
 */
public class IndexWithPropertyTest {

    private Index baseIndex() {
        return Index.builder()
                .uuid("11111111-2222-3333-4444-555555555555")
                .name("vidx")
                .table("mytable")
                .tablespace("ts1")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property("numShards", "4")
                .property("similarity", "cosine")
                .build();
    }

    @Test
    public void withProperty_addsNewKey_preservesAllOtherFields() {
        Index original = baseIndex();
        Index copy = original.withProperty("rebuild", "true");

        // All other fields preserved.
        assertEquals(original.uuid, copy.uuid);
        assertEquals(original.name, copy.name);
        assertEquals(original.table, copy.table);
        assertEquals(original.tablespace, copy.tablespace);
        assertEquals(original.type, copy.type);
        assertArrayEquals(original.columnNames, copy.columnNames);
        assertEquals(original.columns.length, copy.columns.length);
        for (int i = 0; i < original.columns.length; i++) {
            assertEquals(original.columns[i].name, copy.columns[i].name);
            assertEquals(original.columns[i].type, copy.columns[i].type);
        }
        assertEquals(original.unique, copy.unique);

        // Original property values preserved + new property added.
        assertEquals("4", copy.properties.get("numShards"));
        assertEquals("cosine", copy.properties.get("similarity"));
        assertEquals("true", copy.properties.get("rebuild"));
        assertEquals(3, copy.properties.size());
    }

    @Test
    public void withProperty_doesNotMutateOriginal() {
        Index original = baseIndex();
        int originalPropCount = original.properties.size();
        Index copy = original.withProperty("rebuild", "true");

        // The new instance is distinct from the original.
        assertNotSame(original, copy);

        // Original properties map is unchanged.
        assertEquals(originalPropCount, original.properties.size());
        assertFalse(original.properties.containsKey("rebuild"));
        assertNull(original.properties.get("rebuild"));
    }

    @Test
    public void withProperty_replacesExistingKey() {
        Index original = baseIndex();
        Index copy = original.withProperty("numShards", "8");

        assertEquals("8", copy.properties.get("numShards"));
        // Original remains untouched.
        assertEquals("4", original.properties.get("numShards"));
        // Other properties still present in the copy.
        assertEquals("cosine", copy.properties.get("similarity"));
        assertEquals(2, copy.properties.size());
    }

    @Test
    public void withProperty_originalPropertiesMapIsImmutable() {
        Index original = baseIndex();
        // The map is unmodifiable per Index's constructor — this is the
        // invariant that {@link Index#withProperty} relies on to safely
        // share the original map across copies (the helper makes its own
        // mutable HashMap before adding the new entry).
        assertThrows(UnsupportedOperationException.class,
                () -> original.properties.put("foo", "bar"));
    }

    @Test
    public void withProperty_copyPropertiesMapIsAlsoImmutable() {
        Index original = baseIndex();
        Index copy = original.withProperty("rebuild", "true");
        // The copy's map must also be unmodifiable so callers cannot mutate
        // it under the engine's feet after the Index has been published.
        assertThrows(UnsupportedOperationException.class,
                () -> copy.properties.put("foo", "bar"));
    }

    @Test
    public void withProperty_roundTripThroughSerializeDeserializeKeepsNewKey() {
        Index original = baseIndex();
        Index copy = original.withProperty("rebuild", "true");

        byte[] serialised = copy.serialize();
        Index deserialised = Index.deserialize(serialised);

        assertEquals(copy.uuid, deserialised.uuid);
        assertEquals(copy.name, deserialised.name);
        assertEquals(copy.type, deserialised.type);
        assertEquals(copy.unique, deserialised.unique);
        assertEquals("true", deserialised.properties.get("rebuild"));
        assertEquals("4", deserialised.properties.get("numShards"));
        assertEquals("cosine", deserialised.properties.get("similarity"));
        assertEquals(3, deserialised.properties.size());
    }

    @Test
    public void withProperty_addingToIndexThatHadNoPropertiesProducesSinglePropertyMap() {
        Index original = Index.builder()
                .uuid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
                .name("hidx")
                .table("mytable")
                .tablespace("ts1")
                .type(Index.TYPE_HASH)
                .column("k", ColumnTypes.STRING)
                .build();
        assertTrue(original.properties.isEmpty());

        Index copy = original.withProperty("rebuild", "true");
        assertEquals(1, copy.properties.size());
        assertEquals("true", copy.properties.get("rebuild"));
        assertTrue(original.properties.isEmpty());
    }

    @Test
    public void withProperty_nullKeyThrows() {
        Index original = baseIndex();
        assertThrows(IllegalArgumentException.class,
                () -> original.withProperty(null, "true"));
    }

    @Test
    public void withProperty_nullValueIsAccepted() {
        // The persisted Index format uses writeUTF for keys AND values, which
        // does not allow nulls — but the in-memory Index map can carry a null
        // value.  Surface that asymmetry deliberately so callers know they
        // must coerce nulls to a sentinel ("") before serialising.  We do not
        // attempt to forbid it at the helper level: callers (the
        // CREATE INDEX path) only ever pass literal "true" / "false".
        Index original = baseIndex();
        Index copy = original.withProperty("rebuild", null);
        assertNull(copy.properties.get("rebuild"));
        assertTrue(copy.properties.containsKey("rebuild"));
        // The original is unchanged.
        assertSame(baseIndex().properties.size(), original.properties.size());
    }
}
