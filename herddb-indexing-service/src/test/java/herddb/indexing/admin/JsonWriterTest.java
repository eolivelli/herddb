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

package herddb.indexing.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

public class JsonWriterTest {

    @Test
    public void testNull() {
        assertEquals("null", JsonWriter.toJson(null));
    }

    @Test
    public void testPrimitives() {
        assertEquals("42", JsonWriter.toJson(42));
        assertEquals("3.14", JsonWriter.toJson(3.14));
        assertEquals("true", JsonWriter.toJson(Boolean.TRUE));
        assertEquals("\"hello\"", JsonWriter.toJson("hello"));
    }

    @Test
    public void testStringEscaping() {
        assertEquals("\"line1\\nline2\"", JsonWriter.toJson("line1\nline2"));
        assertEquals("\"quote\\\"here\"", JsonWriter.toJson("quote\"here"));
        assertEquals("\"back\\\\slash\"", JsonWriter.toJson("back\\slash"));
        assertEquals("\"tab\\there\"", JsonWriter.toJson("tab\there"));
    }

    @Test
    public void testControlCharactersAreEscaped() {
        String s = JsonWriter.toJson("\u0001");
        assertEquals("\"\\u0001\"", s);
    }

    @Test
    public void testMapPreservesInsertionOrder() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("a", 1);
        m.put("b", "two");
        m.put("c", true);
        assertEquals("{\"a\":1,\"b\":\"two\",\"c\":true}", JsonWriter.toJson(m));
    }

    @Test
    public void testNestedStructures() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("items", Arrays.asList(1, 2, 3));
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("name", "docs");
        inner.put("count", 10);
        m.put("nested", inner);
        assertEquals("{\"items\":[1,2,3],\"nested\":{\"name\":\"docs\",\"count\":10}}",
                JsonWriter.toJson(m));
    }

    @Test
    public void testArray() {
        String s = JsonWriter.toJson(new int[]{1, 2, 3});
        assertEquals("[1,2,3]", s);
    }

    @Test
    public void testByteArrayIsSerializedAsList() {
        String s = JsonWriter.toJson(new byte[]{1, 2});
        assertTrue(s.startsWith("["));
        assertTrue(s.endsWith("]"));
    }
}
