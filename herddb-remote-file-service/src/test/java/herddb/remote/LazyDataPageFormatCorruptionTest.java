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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.storage.DataStorageManagerException;
import herddb.utils.ByteBufUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

/**
 * Corruption-handling unit tests for {@link LazyDataPageFormat}.
 *
 * <p>Verifies that a v2 page index entry with out-of-range
 * {@code valueOffset}/{@code valueLength} values is rejected at parse time
 * with a {@link DataStorageManagerException} (issue #416), so that:
 * <ul>
 *   <li>the eager read path
 *       ({@link LazyDataPageFormat#readAllRecords(ByteBuf)} →
 *       {@code RemoteFileDataStorageManager.readPage(...)}) never reaches
 *       the {@code new byte[m.valueLength]} allocation with a negative
 *       {@code valueLength} and never throws raw
 *       {@link NegativeArraySizeException} /
 *       {@link IndexOutOfBoundsException};
 *   <li>the lazy read path
 *       ({@link LazyDataPageFormat#readIndex(ByteBuf, int, long)} →
 *       {@code RemoteFileDataStorageManager.readPageIndex(...)} →
 *       {@code LazyDataPage.get(...)}) surfaces the same single corruption
 *       signal, so callers' defensive catches see a consistent exception
 *       type for both flows.
 * </ul>
 *
 * <p>Note on negative {@code valueOffset}: the v2 wire format encodes
 * {@code valueOffset} as a {@code VLong}, and the in-tree
 * {@code readVLong(...)} parser refuses to decode 10-byte forms that wrap
 * to negative values (it throws an {@link IllegalArgumentException}), so a
 * legitimate-looking v2 index cannot encode a negative {@code valueOffset}.
 * The {@code valueOffset < 0} guard inside
 * {@link LazyDataPageFormat#readIndex(ByteBuf, int, long)} is therefore a
 * defence-in-depth check for any future code path that might construct
 * {@link LazyDataPageFormat.RecordMetadata} bypassing the parser, and is
 * not exercised here.
 */
public class LazyDataPageFormatCorruptionTest {

    /**
     * Crafts a v2 index section containing a single entry with the supplied
     * key/value-offset/value-length, encoded on the wire exactly the way
     * {@link LazyDataPageFormat#write} would, except with the freedom to
     * inject otherwise-illegal {@code valueLength} bytes (negative VInts
     * cannot come out of {@code writeVInt} but can be hand-written).
     *
     * <p>Returned {@code ByteBuf} is a freshly allocated unpooled heap
     * buffer; the caller is responsible for releasing it.
     */
    private static ByteBuf craftIndexSection(byte[] key, long valueOffset, byte[] rawVIntValueLength) {
        ByteBuf b = Unpooled.buffer();
        ByteBufUtils.writeArray(b, key);
        ByteBufUtils.writeVLong(b, valueOffset);
        b.writeBytes(rawVIntValueLength);
        return b;
    }

    /**
     * Encodes {@code n} as a 5-byte VInt, allowing negative values that
     * {@link ByteBufUtils#writeVInt} would normally still accept (writeVInt
     * does not refuse negative ints — it just emits the 5-byte form whose
     * 5th byte sets bit 31). Returned bytes are exactly what
     * {@link ByteBufUtils#readVInt} consumes.
     */
    private static byte[] vIntFiveByteForm(int n) {
        byte[] out = new byte[5];
        out[0] = (byte) ((n & 0x7F) | 0x80);
        out[1] = (byte) (((n >>> 7) & 0x7F) | 0x80);
        out[2] = (byte) (((n >>> 14) & 0x7F) | 0x80);
        out[3] = (byte) (((n >>> 21) & 0x7F) | 0x80);
        out[4] = (byte) ((n >>> 28) & 0x7F);
        return out;
    }

    @Test
    public void readIndex_negativeValueLength_throwsDataStorageManagerException() {
        // Integer.MIN_VALUE encoded on the wire as a 5-byte VInt with bit 31 set.
        ByteBuf idx = craftIndexSection("k0".getBytes(), 0L, vIntFiveByteForm(Integer.MIN_VALUE));
        try {
            LazyDataPageFormat.readIndex(idx, 1, /* valueSize */ 1024L);
            fail("expected DataStorageManagerException");
        } catch (DataStorageManagerException e) {
            assertNotNull(e.getMessage());
            assertTrue("message should mention valueLength: " + e.getMessage(),
                    e.getMessage().contains("valueLength"));
            assertTrue("message should mention corruption: " + e.getMessage(),
                    e.getMessage().contains("corrupted"));
        } finally {
            idx.release();
        }
    }

    @Test
    public void readIndex_negativeValueLength_throwsEvenWithoutValueSizeBound() {
        // The 2-arg overload (no valueSize bound) must still reject the
        // negative valueLength because the per-entry guard fires first.
        ByteBuf idx = craftIndexSection("k0".getBytes(), 0L, vIntFiveByteForm(-1));
        try {
            LazyDataPageFormat.readIndex(idx, 1);
            fail("expected DataStorageManagerException");
        } catch (DataStorageManagerException e) {
            assertTrue("message should mention valueLength: " + e.getMessage(),
                    e.getMessage().contains("valueLength"));
        } finally {
            idx.release();
        }
    }

    @Test
    public void readIndex_valueOffsetPlusLengthOverflow_throwsDataStorageManagerException() {
        // valueOffset = Long.MAX_VALUE, valueLength = 1 → overflow.
        // Use vIntFiveByteForm to force a 5-byte VInt encoding (writeVInt would
        // shorten it but the result is the same after readVInt; using the
        // 5-byte form keeps the test independent of writeVInt's chosen size).
        ByteBuf idx = craftIndexSection("k0".getBytes(), Long.MAX_VALUE, vIntFiveByteForm(1));
        try {
            LazyDataPageFormat.readIndex(idx, 1, /* valueSize */ Long.MAX_VALUE);
            fail("expected DataStorageManagerException");
        } catch (DataStorageManagerException e) {
            assertTrue("message should mention overflow: " + e.getMessage(),
                    e.getMessage().contains("overflow"));
        } finally {
            idx.release();
        }
    }

    @Test
    public void readIndex_valueRangeBeyondValueSize_throwsDataStorageManagerException() {
        // valueOffset=0, valueLength=100, valueSize=50 → out of bounds.
        ByteBuf idx = craftIndexSection("k0".getBytes(), 0L, vIntFiveByteForm(100));
        try {
            LazyDataPageFormat.readIndex(idx, 1, /* valueSize */ 50L);
            fail("expected DataStorageManagerException");
        } catch (DataStorageManagerException e) {
            assertTrue("message should mention valueSize: " + e.getMessage(),
                    e.getMessage().contains("valueSize"));
        } finally {
            idx.release();
        }
    }

    @Test
    public void readIndex_valueRangeOnTheEdgeOfValueSize_succeeds() throws DataStorageManagerException {
        // valueOffset=10, valueLength=40, valueSize=50 → exactly at the
        // boundary; must NOT trigger the upper-bound check (endOffset == 50).
        ByteBuf idx = craftIndexSection("k0".getBytes(), 10L, vIntFiveByteForm(40));
        try {
            assertEquals(1, LazyDataPageFormat.readIndex(idx, 1, /* valueSize */ 50L).size());
        } finally {
            idx.release();
        }
    }

    @Test
    public void readAllRecords_corruptedIndexNegativeValueLength_throwsDataStorageManagerException()
            throws DataStorageManagerException {
        // Build a complete v2 page where the single index entry's valueLength
        // is hand-corrupted to Integer.MIN_VALUE while the fixed header is
        // structurally valid. The eager path (readAllRecords → readIndex)
        // must reject this before reaching `new byte[m.valueLength]`.
        ByteBuf page = buildPageWithSingleCorruptIndexEntry(
                /* valueOffset */ 0L,
                /* rawVIntValueLength */ vIntFiveByteForm(Integer.MIN_VALUE),
                /* valueSize */ 16L,
                /* valueBytes */ new byte[16]);
        try {
            LazyDataPageFormat.readAllRecords(page);
            fail("expected DataStorageManagerException");
        } catch (DataStorageManagerException e) {
            assertTrue("message should mention valueLength: " + e.getMessage(),
                    e.getMessage().contains("valueLength"));
        } catch (NegativeArraySizeException | IndexOutOfBoundsException e) {
            fail("expected DataStorageManagerException but got " + e.getClass().getSimpleName()
                    + ": " + e.getMessage());
        } finally {
            page.release();
        }
    }

    @Test
    public void readAllRecords_corruptedIndexValueRangeBeyondValueSize_throwsDataStorageManagerException()
            throws DataStorageManagerException {
        // Single index entry that claims valueOffset=0, valueLength=100 but
        // the values section is only 16 bytes long.
        ByteBuf page = buildPageWithSingleCorruptIndexEntry(
                /* valueOffset */ 0L,
                /* rawVIntValueLength */ vIntFiveByteForm(100),
                /* valueSize */ 16L,
                /* valueBytes */ new byte[16]);
        try {
            LazyDataPageFormat.readAllRecords(page);
            fail("expected DataStorageManagerException");
        } catch (DataStorageManagerException e) {
            assertTrue("message should mention valueSize: " + e.getMessage(),
                    e.getMessage().contains("valueSize"));
        } catch (NegativeArraySizeException | IndexOutOfBoundsException e) {
            fail("expected DataStorageManagerException but got " + e.getClass().getSimpleName()
                    + ": " + e.getMessage());
        } finally {
            page.release();
        }
    }

    @Test
    public void readAllRecords_corruptedIndexValueOffsetOverflow_throwsDataStorageManagerException()
            throws DataStorageManagerException {
        // valueOffset=Long.MAX_VALUE, valueLength=1 → overflow on the long sum.
        ByteBuf page = buildPageWithSingleCorruptIndexEntry(
                /* valueOffset */ Long.MAX_VALUE,
                /* rawVIntValueLength */ vIntFiveByteForm(1),
                /* valueSize */ 1L,
                /* valueBytes */ new byte[1]);
        try {
            LazyDataPageFormat.readAllRecords(page);
            fail("expected DataStorageManagerException");
        } catch (DataStorageManagerException e) {
            assertTrue("message should mention overflow: " + e.getMessage(),
                    e.getMessage().contains("overflow"));
        } catch (NegativeArraySizeException | IndexOutOfBoundsException e) {
            fail("expected DataStorageManagerException but got " + e.getClass().getSimpleName()
                    + ": " + e.getMessage());
        } finally {
            page.release();
        }
    }

    /**
     * Hand-builds a v2 page with a single hand-crafted index entry. The
     * fixed header's {@code numRecords=1}, {@code indexSize}, and
     * {@code valueSize} are computed and patched after the fact so the page
     * passes the {@link LazyDataPageFormat#readHeader} sanity checks; the
     * footer is written as the {@code NO_HASH_PRESENT} sentinel
     * (checksum-disabled write path).
     */
    private static ByteBuf buildPageWithSingleCorruptIndexEntry(
            long valueOffset, byte[] rawVIntValueLength, long valueSize, byte[] valueBytes) {
        ByteBuf buf = Unpooled.buffer();
        // reserve fixed header
        buf.writeZero(LazyDataPageFormat.FIXED_HEADER_SIZE);
        final int indexStart = buf.writerIndex();
        // single index entry
        ByteBufUtils.writeArray(buf, "k0".getBytes());
        ByteBufUtils.writeVLong(buf, valueOffset);
        buf.writeBytes(rawVIntValueLength);
        final int indexEnd = buf.writerIndex();
        final int indexSize = indexEnd - indexStart;
        // values section
        buf.writeBytes(valueBytes);
        // footer (NO_HASH_PRESENT sentinel)
        buf.writeLong(0L);
        // patch fixed header
        buf.setInt(0, LazyDataPageFormat.MAGIC);
        buf.setByte(4, LazyDataPageFormat.VERSION_V2);
        buf.setByte(5, LazyDataPageFormat.FLAGS_DEFAULT);
        buf.setInt(6, /* numRecords */ 1);
        buf.setInt(10, indexSize);
        buf.setLong(14, valueSize);
        return buf;
    }
}
