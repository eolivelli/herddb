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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.log.LogSequenceNumber;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.Test;

/**
 * Round-trip + compression-ratio tests for {@link IndexStatus} wiring added
 * for issue #161.
 */
public class IndexStatusCompressionTest {

    @Test
    public void roundTripPreservesFields() throws IOException {
        Set<Long> activePages = new LinkedHashSet<>(Arrays.asList(3L, 4L, 7L));
        IndexStatus original = new IndexStatus("myindex",
                new LogSequenceNumber(2, 9),
                42L, activePages, new byte[]{1, 2, 3, 4});

        IndexStatus roundTripped = serializeAndRead(original);
        assertEquals(original, roundTripped);
    }

    @Test
    public void compressesLargeActivePageSet() throws IOException {
        Set<Long> activePages = new HashSet<>();
        for (long id = 0; id < 5000; id++) {
            activePages.add(id);
        }
        IndexStatus original = new IndexStatus("wide",
                new LogSequenceNumber(1, 1),
                5000L, activePages, new byte[0]);

        VisibleByteArrayOutputStream buffer = new VisibleByteArrayOutputStream();
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(buffer)) {
            original.serialize(out);
        }
        int compressedSize = buffer.toByteArray().length;

        // 5000 sequential VLongs = ~10–15 KiB raw; GZIP@9 should drop well
        // below 10 KiB.
        assertTrue(
                "5000 active pages should compress to < 10 KiB, got " + compressedSize,
                compressedSize < 10_000);
        System.out.println("IndexStatus 5000 active pages compressed size = " + compressedSize);

        IndexStatus roundTripped = deserializeFromBytes(buffer.toByteArray());
        assertEquals(original, roundTripped);
    }

    @Test
    public void readsLegacyUncompressedPayload() throws IOException {
        Set<Long> activePages = new LinkedHashSet<>(Arrays.asList(10L, 11L));
        IndexStatus original = new IndexStatus("legacyindex",
                new LogSequenceNumber(4, 4),
                99L, activePages, new byte[]{5, 6});

        VisibleByteArrayOutputStream buffer = new VisibleByteArrayOutputStream();
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(buffer)) {
            out.writeVLong(1); // legacy version
            out.writeVLong(0); // legacy flags (no compression)
            out.writeUTF(original.indexName);
            out.writeLong(original.sequenceNumber.ledgerId);
            out.writeLong(original.sequenceNumber.offset);
            out.writeVLong(original.newPageId);
            out.writeVInt(activePages.size());
            for (long pageId : activePages) {
                out.writeVLong(pageId);
            }
            out.writeArray(original.indexData);
        }

        IndexStatus roundTripped = deserializeFromBytes(buffer.toByteArray());
        assertEquals(original, roundTripped);
    }

    private static IndexStatus serializeAndRead(IndexStatus input) throws IOException {
        VisibleByteArrayOutputStream buffer = new VisibleByteArrayOutputStream();
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(buffer)) {
            input.serialize(out);
        }
        return deserializeFromBytes(buffer.toByteArray());
    }

    private static IndexStatus deserializeFromBytes(byte[] bytes) throws IOException {
        try (ExtendedDataInputStream in = new ExtendedDataInputStream(new SimpleByteArrayInputStream(bytes))) {
            return IndexStatus.deserialize(in);
        }
    }
}
