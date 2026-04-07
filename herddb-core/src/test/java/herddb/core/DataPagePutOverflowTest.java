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

package herddb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import herddb.model.Record;
import herddb.utils.Bytes;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Test;

/**
 * Unit tests for {@link DataPage#put(Record)} overflow behavior.
 * <p>
 * Verifies that when a put fails because the page is full, the previous
 * record is preserved rather than lost.
 */
public class DataPagePutOverflowTest {

    private static Record makeRecord(String key, int valueSize) {
        return new Record(Bytes.from_string(key), Bytes.from_array(new byte[valueSize]));
    }

    private static long estimateSize(Record r) {
        return DataPage.estimateEntrySize(r);
    }

    /**
     * When updating an existing record with a larger value that overflows
     * the page, the old record must still be present after put returns false.
     */
    @Test
    public void testPutOverflowPreservesOldRecord() {
        Record smallRecord = makeRecord("key1", 50);
        Record filler = makeRecord("filler", 50);
        Record biggerRecord = makeRecord("key1", 200);

        // Page fits small + filler but not bigger + filler.
        // Also bigger alone must fit (otherwise DataPage.put throws "record too big for any page").
        long maxSize = estimateSize(smallRecord) + estimateSize(filler) + 20;
        assertTrue("bigger record alone must fit in maxSize", estimateSize(biggerRecord) <= maxSize);

        DataPage page = new DataPage(null, 1, maxSize, 0, new ConcurrentHashMap<>(), false);

        assertTrue("Small record should fit", page.put(smallRecord));
        assertTrue("Filler should fit", page.put(filler));
        assertEquals(2, page.size());

        // Now try to replace "key1" with a bigger value — page overflows
        assertFalse("Bigger record should not fit with filler", page.put(biggerRecord));

        // The OLD record must still be present
        Record found = page.get(Bytes.from_string("key1"));
        assertNotNull("Old record must be preserved after failed put", found);
        assertEquals("Old record value must be intact", smallRecord.value, found.value);
        assertEquals("Page size must still be 2 (key1 + filler)", 2, page.size());
    }

    /**
     * When inserting a new record (no previous) that overflows,
     * the key must not remain in the page.
     */
    @Test
    public void testPutOverflowNewRecordRemovedCleanly() {
        Record filler = makeRecord("filler", 50);
        long maxSize = estimateSize(filler) * 2 + 20; // fits ~2 records
        DataPage page = new DataPage(null, 1, maxSize, 0, new ConcurrentHashMap<>(), false);

        assertTrue("Filler should fit", page.put(filler));
        Record filler2 = makeRecord("filler2", 50);
        assertTrue("Filler2 should fit", page.put(filler2));

        // Now try to insert a new key that doesn't fit (but is not "too big for any page")
        Record newRecord = makeRecord("new_key", 50);
        assertTrue("New record alone must fit in maxSize", estimateSize(newRecord) <= maxSize);
        assertFalse("New record should not fit in full page", page.put(newRecord));

        // The new key must not be in the page
        assertNull("New key must not remain after failed put", page.get(Bytes.from_string("new_key")));
        assertEquals("Page size must still be 2", 2, page.size());
    }

    /**
     * After a failed put (overflow), the page's used memory must be unchanged.
     */
    @Test
    public void testPutOverflowMemoryAccountingStable() {
        Record small = makeRecord("key1", 50);
        Record filler = makeRecord("filler", 50);
        long maxSize = estimateSize(small) + estimateSize(filler) + 20;
        DataPage page = new DataPage(null, 1, maxSize, 0, new ConcurrentHashMap<>(), false);

        assertTrue(page.put(small));
        assertTrue(page.put(filler));

        long memBefore = page.getUsedMemory();

        // Try overflow update (bigger value, but not "too big for any page")
        Record bigger = makeRecord("key1", 200);
        assertTrue("Record alone must fit in maxSize", estimateSize(bigger) <= maxSize);
        assertFalse(page.put(bigger));

        long memAfter = page.getUsedMemory();
        assertEquals("Used memory must be unchanged after failed put", memBefore, memAfter);
    }

    /**
     * Multiple consecutive failed puts on the same key must not corrupt the page.
     */
    @Test
    public void testRepeatedOverflowOnSameKey() {
        Record original = makeRecord("key1", 50);
        Record filler = makeRecord("filler", 50);
        long maxSize = estimateSize(original) + estimateSize(filler) + 20;
        DataPage page = new DataPage(null, 1, maxSize, 0, new ConcurrentHashMap<>(), false);

        assertTrue(page.put(original));
        assertTrue(page.put(filler));

        // Try multiple overflows
        for (int i = 0; i < 10; i++) {
            Record bigger = makeRecord("key1", 200);
            assertFalse("Attempt " + i + " should fail", page.put(bigger));
        }

        // Original record must still be intact
        Record found = page.get(Bytes.from_string("key1"));
        assertNotNull("Original record must survive all failed puts", found);
        assertEquals("Original value must be intact", original.value, found.value);
        assertEquals(2, page.size());
    }

    /**
     * A successful put after a failed put must work correctly.
     */
    @Test
    public void testSuccessfulPutAfterFailedPut() {
        Record small = makeRecord("key1", 50);
        Record filler = makeRecord("filler", 50);
        long maxSize = estimateSize(small) + estimateSize(filler) + 20;
        DataPage page = new DataPage(null, 1, maxSize, 0, new ConcurrentHashMap<>(), false);

        assertTrue(page.put(small));
        assertTrue(page.put(filler));

        // Fail — too big
        Record bigger = makeRecord("key1", 200);
        assertFalse(page.put(bigger));

        // Now put a record with the same size (which fits since it replaces the existing)
        byte[] mediumValue = new byte[50];
        mediumValue[0] = 99;
        Record medium = new Record(Bytes.from_string("key1"), Bytes.from_array(mediumValue));
        assertTrue("Same-size replacement should fit", page.put(medium));

        Record found = page.get(Bytes.from_string("key1"));
        assertNotNull(found);
        assertEquals("Should be the medium record now", medium.value, found.value);
    }
}
