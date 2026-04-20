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

package herddb.remote;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.model.Record;
import herddb.remote.LazyDataPageFormat.FixedHeader;
import herddb.remote.LazyDataPageFormat.RecordMetadata;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.Test;

/**
 * Unit tests for {@link LazyDataPageFormat}.
 */
public class LazyDataPageFormatTest {

    private static Record mkRecord(String key, byte[] value) {
        return new Record(Bytes.from_string(key), Bytes.from_array(value));
    }

    private static List<Record> makeRecords(int n, int valueSize) {
        final List<Record> records = new ArrayList<>(n);
        final Random r = new Random(42L + n + valueSize);
        for (int i = 0; i < n; i++) {
            final byte[] v = new byte[valueSize];
            r.nextBytes(v);
            records.add(mkRecord("k-" + i, v));
        }
        return records;
    }

    @Test
    public void fixedHeaderSizeConstantMatchesLayout() {
        // 4 (magic) + 1 (version) + 1 (flags) + 4 (numRecords) + 4 (indexSize) + 8 (valueSize) = 22
        assertEquals(22, LazyDataPageFormat.FIXED_HEADER_SIZE);
        // sanity-check that a freshly-written empty page actually starts with those bytes
        ByteBuf buf = LazyDataPageFormat.write(Collections.emptyList());
        try {
            // magic
            assertEquals(LazyDataPageFormat.MAGIC, buf.getInt(0));
            // version
            assertEquals(LazyDataPageFormat.VERSION_V2, buf.getByte(4));
            // flags
            assertEquals(LazyDataPageFormat.FLAGS_DEFAULT, buf.getByte(5));
            // numRecords
            assertEquals(0, buf.getInt(6));
            // indexSize
            assertEquals(0, buf.getInt(10));
            // valueSize
            assertEquals(0L, buf.getLong(14));
            // fixed header + empty sections + footer
            assertEquals(LazyDataPageFormat.FIXED_HEADER_SIZE + LazyDataPageFormat.FOOTER_SIZE,
                    buf.readableBytes());
        } finally {
            buf.release();
        }
    }

    @Test
    public void roundTripEmpty() throws DataStorageManagerException {
        roundTrip(Collections.emptyList());
    }

    @Test
    public void roundTripSingleRecord() throws DataStorageManagerException {
        roundTrip(Collections.singletonList(
                mkRecord("theKey", "theValue".getBytes())));
    }

    @Test
    public void roundTripManyRecords() throws DataStorageManagerException {
        roundTrip(makeRecords(1000, 128));
    }

    @Test
    public void roundTripVariableValueSizes() throws DataStorageManagerException {
        final List<Record> records = new ArrayList<>();
        records.add(mkRecord("empty", new byte[0]));
        records.add(mkRecord("small", new byte[]{1, 2, 3}));
        final byte[] big = new byte[4096];
        new Random(7).nextBytes(big);
        records.add(mkRecord("big", big));
        records.add(mkRecord("medium", "some UTF-8 text \u00e9\u00e8".getBytes()));
        roundTrip(records);
    }

    private static void roundTrip(List<Record> records) throws DataStorageManagerException {
        ByteBuf buf = LazyDataPageFormat.write(records);
        try {
            // header
            FixedHeader h = LazyDataPageFormat.readHeader(buf);
            assertEquals(records.size(), h.numRecords);
            assertEquals(LazyDataPageFormat.FIXED_HEADER_SIZE, h.valueSectionStart() - h.indexSize);
            long expectedValueBytes = 0;
            for (Record r : records) {
                expectedValueBytes += r.value.getLength();
            }
            assertEquals(expectedValueBytes, h.valueSize);
            assertEquals((long) buf.readableBytes(), h.totalSize());

            // readAllRecords must return structurally equal records
            List<Record> out = LazyDataPageFormat.readAllRecords(buf);
            assertEquals(records.size(), out.size());
            for (int i = 0; i < records.size(); i++) {
                Record in = records.get(i);
                Record parsed = out.get(i);
                assertEquals("key mismatch at " + i, in.key, parsed.key);
                assertArrayEquals("value mismatch at " + i,
                        in.value.to_array(), parsed.value.to_array());
            }
        } finally {
            buf.release();
        }
    }

    @Test
    public void readIndexReturnsOffsetsThatLocateEveryValue() throws DataStorageManagerException {
        List<Record> records = makeRecords(50, 37);
        ByteBuf buf = LazyDataPageFormat.write(records);
        try {
            FixedHeader h = LazyDataPageFormat.readHeader(buf);
            ByteBuf indexSlice = buf.slice(LazyDataPageFormat.FIXED_HEADER_SIZE, h.indexSize);
            List<RecordMetadata> metadata = LazyDataPageFormat.readIndex(indexSlice, h.numRecords);
            assertEquals(records.size(), metadata.size());
            int valueSectionStart = LazyDataPageFormat.FIXED_HEADER_SIZE + h.indexSize;
            for (int i = 0; i < metadata.size(); i++) {
                RecordMetadata m = metadata.get(i);
                Record orig = records.get(i);
                assertEquals("key mismatch at " + i, orig.key, m.key);
                assertEquals("value length mismatch at " + i,
                        orig.value.getLength(), m.valueLength);
                byte[] extracted = new byte[m.valueLength];
                buf.getBytes(valueSectionStart + (int) m.valueOffset, extracted);
                assertArrayEquals("value content mismatch at " + i,
                        orig.value.to_array(), extracted);
            }
        } finally {
            buf.release();
        }
    }

    @Test
    public void valueOffsetsAreMonotonicAndContiguous() throws DataStorageManagerException {
        List<Record> records = makeRecords(20, 17);
        ByteBuf buf = LazyDataPageFormat.write(records);
        try {
            FixedHeader h = LazyDataPageFormat.readHeader(buf);
            ByteBuf indexSlice = buf.slice(LazyDataPageFormat.FIXED_HEADER_SIZE, h.indexSize);
            List<RecordMetadata> metadata = LazyDataPageFormat.readIndex(indexSlice, h.numRecords);
            long expectedOffset = 0;
            for (RecordMetadata m : metadata) {
                assertEquals(expectedOffset, m.valueOffset);
                expectedOffset += m.valueLength;
            }
            assertEquals(h.valueSize, expectedOffset);
        } finally {
            buf.release();
        }
    }

    @Test
    public void corruptedFooterIsDetected() {
        List<Record> records = makeRecords(3, 16);
        ByteBuf buf = LazyDataPageFormat.write(records);
        try {
            // flip a byte in the footer
            int footerPos = buf.readableBytes() - LazyDataPageFormat.FOOTER_SIZE;
            buf.setByte(footerPos, buf.getByte(footerPos) ^ 0xFF);
            try {
                LazyDataPageFormat.readAllRecords(buf);
                fail("expected footer hash mismatch");
            } catch (DataStorageManagerException expected) {
                assertTrue(expected.getMessage().contains("footer hash mismatch"));
            }
        } finally {
            buf.release();
        }
    }

    @Test
    public void corruptedBodyIsDetectedByHash() {
        List<Record> records = makeRecords(5, 16);
        ByteBuf buf = LazyDataPageFormat.write(records);
        try {
            // flip a byte in the middle of the values section
            int mid = buf.readableBytes() / 2;
            buf.setByte(mid, buf.getByte(mid) ^ 0x01);
            try {
                LazyDataPageFormat.readAllRecords(buf);
                fail("expected footer hash mismatch from body corruption");
            } catch (DataStorageManagerException expected) {
                assertTrue(expected.getMessage().contains("footer hash mismatch"));
            }
        } finally {
            buf.release();
        }
    }

    @Test
    public void looksLikeV2Sentinel() {
        ByteBuf v2 = LazyDataPageFormat.write(Collections.emptyList());
        try {
            assertTrue(LazyDataPageFormat.looksLikeV2(v2));
        } finally {
            v2.release();
        }
        // v1 pages start with VLong=1 -> 0x01; this must not be mistaken for v2
        ByteBuf v1Like = Unpooled.buffer(16);
        try {
            v1Like.writeByte(1);
            v1Like.writeByte(0);
            v1Like.writeInt(0);
            assertFalse(LazyDataPageFormat.looksLikeV2(v1Like));
        } finally {
            v1Like.release();
        }
        // too-short buffer: returns false, does not throw
        ByteBuf tiny = Unpooled.buffer(2);
        try {
            tiny.writeByte(0x48);
            tiny.writeByte(0x44);
            assertFalse(LazyDataPageFormat.looksLikeV2(tiny));
        } finally {
            tiny.release();
        }
    }

    @Test
    public void shortHeaderIsRejected() {
        ByteBuf tooShort = Unpooled.buffer();
        try {
            tooShort.writeInt(LazyDataPageFormat.MAGIC);
            tooShort.writeByte(LazyDataPageFormat.VERSION_V2);
            // remaining bytes missing
            try {
                LazyDataPageFormat.readHeader(tooShort);
                fail("expected exception");
            } catch (DataStorageManagerException expected) {
                assertNotNull(expected.getMessage());
            }
        } finally {
            tooShort.release();
        }
    }

    @Test
    public void badMagicIsRejected() {
        ByteBuf bad = Unpooled.buffer();
        try {
            bad.writeInt(0xDEADBEEF);
            bad.writeByte(LazyDataPageFormat.VERSION_V2);
            bad.writeByte(0);
            bad.writeInt(0);
            bad.writeInt(0);
            bad.writeLong(0);
            try {
                LazyDataPageFormat.readHeader(bad);
                fail("expected exception");
            } catch (DataStorageManagerException expected) {
                assertTrue(expected.getMessage().contains("magic"));
            }
        } finally {
            bad.release();
        }
    }

    @Test
    public void unsupportedVersionIsRejected() {
        ByteBuf bad = Unpooled.buffer();
        try {
            bad.writeInt(LazyDataPageFormat.MAGIC);
            bad.writeByte(99); // bad version
            bad.writeByte(0);
            bad.writeInt(0);
            bad.writeInt(0);
            bad.writeLong(0);
            try {
                LazyDataPageFormat.readHeader(bad);
                fail("expected exception");
            } catch (DataStorageManagerException expected) {
                assertTrue(expected.getMessage().contains("version"));
            }
        } finally {
            bad.release();
        }
    }

    @Test
    public void truncatedPageIsRejected() {
        List<Record> records = makeRecords(4, 32);
        ByteBuf buf = LazyDataPageFormat.write(records);
        try {
            // make a copy of the first few bytes (incomplete)
            int truncated = LazyDataPageFormat.FIXED_HEADER_SIZE + LazyDataPageFormat.FOOTER_SIZE + 4;
            byte[] prefix = new byte[truncated];
            buf.getBytes(0, prefix);
            ByteBuf partial = Unpooled.wrappedBuffer(prefix);
            try {
                LazyDataPageFormat.readAllRecords(partial);
                fail("expected exception on truncated page");
            } catch (DataStorageManagerException expected) {
                assertTrue("msg=" + expected.getMessage(),
                        expected.getMessage().contains("shorter")
                                || expected.getMessage().contains("truncated"));
            }
        } finally {
            buf.release();
        }
    }

    @Test
    public void largeValuesAreHandled() throws DataStorageManagerException {
        // value large enough to push VLong offsets past a single byte — exercise multi-byte VLong
        List<Record> records = new ArrayList<>();
        records.add(mkRecord("a", new byte[300]));
        records.add(mkRecord("b", new byte[300]));
        records.add(mkRecord("c", new byte[300]));
        // offsets will be 0, 300, 600 — 300 needs a 2-byte VLong, which is what we want to test
        roundTrip(records);
    }

    @Test
    public void duplicateKeysRoundTripPreservingOrder() throws DataStorageManagerException {
        // Records in a DataPage are unique-keyed, but the serializer should
        // not reject duplicates; it just writes them in order.
        List<Record> records = Arrays.asList(
                mkRecord("k", new byte[]{1, 1, 1}),
                mkRecord("k", new byte[]{2, 2, 2}));
        roundTrip(records);
    }
}
