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

package herddb.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.PageSet.DataPageMetaData;
import herddb.log.LogSequenceNumber;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Round-trip + compression-ratio tests for {@link TableStatus} wiring added
 * for issue #161. Focuses on the wire format; the hammer suite covers the
 * real checkpoint path.
 */
public class TableStatusCompressionTest {

    @Test
    public void roundTripPreservesFields() throws IOException {
        Map<Long, DataPageMetaData> activePages = new LinkedHashMap<>();
        activePages.put(10L, newPageMeta(4096, 128, 7));
        activePages.put(11L, newPageMeta(8192, 256, 0));

        TableStatus original = new TableStatus("mytable",
                new LogSequenceNumber(3, 99),
                new byte[]{1, 2, 3}, 42L, activePages);

        TableStatus roundTripped = serializeAndRead(original);
        assertTableStatusFieldsEqual(original, roundTripped);
    }

    @Test
    public void compressesLargeActivePageMap() throws IOException {
        Map<Long, DataPageMetaData> activePages = new HashMap<>();
        for (long id = 0; id < 5000; id++) {
            activePages.put(id, newPageMeta(4096, 128, id % 97));
        }
        TableStatus original = new TableStatus("wide",
                new LogSequenceNumber(1, 1),
                new byte[]{9}, 5000L, activePages);

        // Size the serialised form.
        VisibleByteArrayOutputStream buffer = new VisibleByteArrayOutputStream();
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(buffer)) {
            original.serialize(out);
        }
        int compressedSize = buffer.toByteArray().length;

        // Estimate uncompressed size: ~4 bytes/page id (VLong) + 3 × ~2 bytes per
        // DataPageMetaData VLong field. Even against a generous 20 bytes/entry,
        // a 5000-entry map is ~100 KiB; GZIP-at-9 on repetitive VLong sequences
        // should come in well under that.
        assertTrue(
                "5000 active pages should compress to < 20 KiB, got " + compressedSize,
                compressedSize < 20_000);
        System.out.println("TableStatus 5000 active pages compressed size = " + compressedSize);

        TableStatus roundTripped = deserializeFromBytes(buffer.toByteArray());
        assertTableStatusFieldsEqual(original, roundTripped);
    }

    @Test
    public void readsLegacyUncompressedPayload() throws IOException {
        Map<Long, DataPageMetaData> activePages = new LinkedHashMap<>();
        activePages.put(1L, newPageMeta(1024, 64, 2));
        activePages.put(2L, newPageMeta(1024, 64, 5));

        TableStatus original = new TableStatus("legacytable",
                new LogSequenceNumber(5, 55),
                new byte[]{7, 8}, 3L, activePages);

        VisibleByteArrayOutputStream buffer = new VisibleByteArrayOutputStream();
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(buffer)) {
            out.writeVLong(1); // legacy version
            out.writeVLong(0); // legacy flags (no compression)
            out.writeUTF(original.tableName);
            out.writeLong(original.sequenceNumber.ledgerId);
            out.writeLong(original.sequenceNumber.offset);
            out.writeLong(original.nextPageId);
            out.writeArray(original.nextPrimaryKeyValue);
            out.writeVInt(activePages.size());
            for (Map.Entry<Long, DataPageMetaData> e : activePages.entrySet()) {
                out.writeVLong(e.getKey());
                e.getValue().serialize(out);
            }
        }

        TableStatus roundTripped = deserializeFromBytes(buffer.toByteArray());
        assertTableStatusFieldsEqual(original, roundTripped);
    }

    private static TableStatus serializeAndRead(TableStatus input) throws IOException {
        VisibleByteArrayOutputStream buffer = new VisibleByteArrayOutputStream();
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(buffer)) {
            input.serialize(out);
        }
        return deserializeFromBytes(buffer.toByteArray());
    }

    private static TableStatus deserializeFromBytes(byte[] bytes) throws IOException {
        try (ExtendedDataInputStream in = new ExtendedDataInputStream(new SimpleByteArrayInputStream(bytes))) {
            return TableStatus.deserialize(in);
        }
    }

    private static void assertTableStatusFieldsEqual(TableStatus expected, TableStatus actual) {
        assertEquals(expected.tableName, actual.tableName);
        assertEquals(expected.sequenceNumber, actual.sequenceNumber);
        assertEquals(expected.nextPageId, actual.nextPageId);
        assertArrayEquals(expected.nextPrimaryKeyValue, actual.nextPrimaryKeyValue);
        assertEquals(expected.activePages.keySet(), actual.activePages.keySet());
    }

    /**
     * Construct a DataPageMetaData by round-tripping three VLongs through the
     * public deserialize() helper. Avoids reaching the package-private
     * constructor in {@code herddb.core}.
     */
    private static DataPageMetaData newPageMeta(long size, long avgRecordSize, long dirt) throws IOException {
        VisibleByteArrayOutputStream buf = new VisibleByteArrayOutputStream();
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(buf)) {
            out.writeVLong(size);
            out.writeVLong(avgRecordSize);
            out.writeVLong(dirt);
        }
        try (ExtendedDataInputStream in = new ExtendedDataInputStream(new SimpleByteArrayInputStream(buf.toByteArray()))) {
            return DataPageMetaData.deserialize(in);
        }
    }
}
