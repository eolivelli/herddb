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

import herddb.model.Record;
import herddb.storage.DataStorageManagerException;
import herddb.utils.ByteBufUtils;
import herddb.utils.Bytes;
import herddb.utils.XXHash64Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * On-disk format (version 2) for data pages written by
 * {@link RemoteFileDataStorageManager}.
 *
 * <p>The purpose of this format is to let the storage manager read only the
 * keys and per-record offsets at page-load time, and then fetch individual
 * record values on-demand via byte-range reads against the remote file
 * service. See the class-level documentation of
 * {@link RemoteFileDataStorageManager} for the full rationale.
 *
 * <p>Layout:
 * <pre>
 *   +--------------------------------------------------------------+
 *   | fixed header ({@value #FIXED_HEADER_SIZE} bytes):            |
 *   |   magic      [int]    = {@value #MAGIC_HEX} ("HDP2")         |
 *   |   version    [byte]   = {@value #VERSION_V2}                 |
 *   |   flags      [byte]   = 0                                    |
 *   |   numRecords [int]                                           |
 *   |   indexSize  [int]    bytes of the Index section             |
 *   |   valueSize  [long]   bytes of the Values section            |
 *   +--------------------------------------------------------------+
 *   | index section (indexSize bytes):                             |
 *   |   repeated numRecords times:                                 |
 *   |     VInt   keyLength                                         |
 *   |     byte[] keyBytes                                          |
 *   |     VLong  valueOffset   (relative to Values section start)  |
 *   |     VInt   valueLength                                       |
 *   +--------------------------------------------------------------+
 *   | values section (valueSize bytes):                            |
 *   |   concatenation of per-record raw value byte[]               |
 *   +--------------------------------------------------------------+
 *   | footer ({@value #FOOTER_SIZE} bytes):                        |
 *   |   xxhash64 over all preceding bytes                          |
 *   +--------------------------------------------------------------+
 * </pre>
 *
 * <p>The fixed-width header lets a reader issue a single
 * {@code readFileRange(path, 0, FIXED_HEADER_SIZE)} to discover
 * {@code indexSize} and {@code valueSize}, and then a second range read to
 * fetch the index section. Each value can then be fetched independently via
 * {@code readFileRange(path, valueSectionStart + valueOffset, valueLength)}.
 *
 * <p>Magic distinguishes v2 pages from the legacy v1 format written by
 * {@code FileDataStorageManager} (v1 files start with a VLong=1, whose first
 * byte is {@code 0x01}, while v2 files start with {@code 'H' = 0x48}).
 */
public final class LazyDataPageFormat {

    /** Magic for v2 pages: ASCII {@code "HDP2"}. */
    public static final int MAGIC = 0x48445032;

    /** Hex string form of {@link #MAGIC}, used only in Javadoc. */
    private static final String MAGIC_HEX = "0x48445032";

    public static final byte VERSION_V2 = 2;
    public static final byte FLAGS_DEFAULT = 0;

    /** Bytes = 4 (magic) + 1 (version) + 1 (flags) + 4 (numRecords) + 4 (indexSize) + 8 (valueSize). */
    public static final int FIXED_HEADER_SIZE = 22;

    /** xxhash64 footer size in bytes. */
    public static final int FOOTER_SIZE = 8;

    private static final int OFFSET_MAGIC = 0;
    private static final int OFFSET_VERSION = 4;
    private static final int OFFSET_FLAGS = 5;
    private static final int OFFSET_NUM_RECORDS = 6;
    private static final int OFFSET_INDEX_SIZE = 10;
    private static final int OFFSET_VALUE_SIZE = 14;

    private LazyDataPageFormat() {
    }

    /**
     * Parsed fixed-header fields.
     */
    public static final class FixedHeader {
        public final int numRecords;
        public final int indexSize;
        public final long valueSize;

        public FixedHeader(int numRecords, int indexSize, long valueSize) {
            this.numRecords = numRecords;
            this.indexSize = indexSize;
            this.valueSize = valueSize;
        }

        /** Absolute byte offset where the values section starts. */
        public long valueSectionStart() {
            return (long) FIXED_HEADER_SIZE + (long) indexSize;
        }

        /** Absolute byte offset of the footer (= size of everything before the footer). */
        public long footerOffset() {
            return valueSectionStart() + valueSize;
        }

        /** Total on-disk size in bytes. */
        public long totalSize() {
            return footerOffset() + FOOTER_SIZE;
        }
    }

    /**
     * Per-record metadata parsed from the index section: the key plus the
     * offset and length of its value inside the values section.
     */
    public static final class RecordMetadata {
        public final Bytes key;
        /** Offset relative to the start of the values section. */
        public final long valueOffset;
        public final int valueLength;

        public RecordMetadata(Bytes key, long valueOffset, int valueLength) {
            this.key = key;
            this.valueOffset = valueOffset;
            this.valueLength = valueLength;
        }
    }

    /**
     * Returns {@code true} if the first four readable bytes of {@code buf}
     * match the v2 magic. Does not advance {@code buf}.
     */
    public static boolean looksLikeV2(ByteBuf buf) {
        if (buf.readableBytes() < 4) {
            return false;
        }
        return buf.getInt(buf.readerIndex()) == MAGIC;
    }

    /**
     * Serialises {@code records} into a fresh pooled heap {@link ByteBuf}
     * containing a v2 page. Caller takes ownership and must release the
     * returned buffer.
     */
    public static ByteBuf write(Collection<Record> records) {
        final int numRecords = records.size();
        long totalValueBytes = 0L;
        long estimatedIndexBytes = 0L;
        for (Record r : records) {
            totalValueBytes += r.value.getLength();
            // upper bounds: VInt <=5 bytes, VLong <=10 bytes
            estimatedIndexBytes += 5L + r.key.getLength() + 10L + 5L;
        }
        long estimatedTotal = (long) FIXED_HEADER_SIZE + estimatedIndexBytes + totalValueBytes + FOOTER_SIZE;
        int initialCapacity = (int) Math.min(estimatedTotal, Integer.MAX_VALUE);
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(Math.max(initialCapacity, 64));
        try {
            // reserve space for the fixed header; we'll patch it after we know the sizes.
            buf.writeZero(FIXED_HEADER_SIZE);
            final int indexStart = buf.writerIndex();
            long valueOffset = 0L;
            for (Record r : records) {
                ByteBufUtils.writeArray(buf, r.key);
                ByteBufUtils.writeVLong(buf, valueOffset);
                final int vLen = r.value.getLength();
                ByteBufUtils.writeVInt(buf, vLen);
                valueOffset += vLen;
            }
            final int indexEnd = buf.writerIndex();
            final int indexSize = indexEnd - indexStart;
            // values section
            for (Record r : records) {
                buf.writeBytes(r.value.getBuffer(), r.value.getOffset(), r.value.getLength());
            }
            final int valueEnd = buf.writerIndex();
            final long valueSize = (long) (valueEnd - indexEnd);
            // patch fixed header
            buf.setInt(OFFSET_MAGIC, MAGIC);
            buf.setByte(OFFSET_VERSION, VERSION_V2);
            buf.setByte(OFFSET_FLAGS, FLAGS_DEFAULT);
            buf.setInt(OFFSET_NUM_RECORDS, numRecords);
            buf.setInt(OFFSET_INDEX_SIZE, indexSize);
            buf.setLong(OFFSET_VALUE_SIZE, valueSize);
            // footer: xxhash64 over everything written so far
            final long hash = hashBytes(buf, 0, valueEnd);
            buf.writeLong(hash);
            return buf;
        } catch (RuntimeException t) {
            buf.release();
            throw t;
        }
    }

    /**
     * Parses the fixed header from {@code buf}. Does not advance the reader
     * index; callers that want to read the index section afterwards should
     * call {@link ByteBuf#readerIndex(int)} to reposition past the header.
     *
     * @throws DataStorageManagerException if the magic or version don't match
     *     or the buffer is too short.
     */
    public static FixedHeader readHeader(ByteBuf buf) throws DataStorageManagerException {
        final int readerIdx = buf.readerIndex();
        if (buf.readableBytes() < FIXED_HEADER_SIZE) {
            throw new DataStorageManagerException("short v2 header: only "
                    + buf.readableBytes() + " bytes readable, need " + FIXED_HEADER_SIZE);
        }
        final int magic = buf.getInt(readerIdx + OFFSET_MAGIC);
        if (magic != MAGIC) {
            throw new DataStorageManagerException("bad v2 magic: expected 0x"
                    + Integer.toHexString(MAGIC) + ", got 0x" + Integer.toHexString(magic));
        }
        final byte version = buf.getByte(readerIdx + OFFSET_VERSION);
        if (version != VERSION_V2) {
            throw new DataStorageManagerException("unsupported v2 page version: " + version);
        }
        final byte flags = buf.getByte(readerIdx + OFFSET_FLAGS);
        if (flags != FLAGS_DEFAULT) {
            throw new DataStorageManagerException("unsupported v2 page flags: " + flags);
        }
        final int numRecords = buf.getInt(readerIdx + OFFSET_NUM_RECORDS);
        final int indexSize = buf.getInt(readerIdx + OFFSET_INDEX_SIZE);
        final long valueSize = buf.getLong(readerIdx + OFFSET_VALUE_SIZE);
        if (numRecords < 0 || indexSize < 0 || valueSize < 0L) {
            throw new DataStorageManagerException("negative v2 header field: numRecords="
                    + numRecords + ", indexSize=" + indexSize + ", valueSize=" + valueSize);
        }
        return new FixedHeader(numRecords, indexSize, valueSize);
    }

    /**
     * Reads {@code numRecords} {@link RecordMetadata} entries from the index
     * section (already positioned at the start of the index and containing at
     * least {@code indexSize} readable bytes).
     */
    public static List<RecordMetadata> readIndex(ByteBuf indexSection, int numRecords)
            throws DataStorageManagerException {
        final List<RecordMetadata> result = new ArrayList<>(numRecords);
        try {
            for (int i = 0; i < numRecords; i++) {
                final byte[] keyBytes = ByteBufUtils.readArray(indexSection);
                final long valueOffset = ByteBufUtils.readVLong(indexSection);
                final int valueLength = ByteBufUtils.readVInt(indexSection);
                result.add(new RecordMetadata(Bytes.from_array(keyBytes), valueOffset, valueLength));
            }
        } catch (IndexOutOfBoundsException e) {
            throw new DataStorageManagerException("truncated v2 index section", e);
        }
        return result;
    }

    /**
     * Eagerly parses a complete v2 page and returns the full list of
     * {@link Record}s, verifying the xxhash64 footer. Used by code paths that
     * always need every value (e.g. {@code fullTableScan}) or by fallback
     * paths.
     */
    public static List<Record> readAllRecords(ByteBuf wholeFile) throws DataStorageManagerException {
        final int startIdx = wholeFile.readerIndex();
        final int available = wholeFile.readableBytes();
        if (available < FIXED_HEADER_SIZE + FOOTER_SIZE) {
            throw new DataStorageManagerException("truncated v2 page: " + available + " bytes");
        }
        final FixedHeader h = readHeader(wholeFile);
        final long declared = h.totalSize();
        if (declared > (long) available) {
            throw new DataStorageManagerException("v2 page shorter than declared: "
                    + available + " < " + declared);
        }
        // verify footer hash (over bytes [startIdx .. startIdx + footerOffset))
        final int footerPos = startIdx + (int) h.footerOffset();
        final long expected = wholeFile.getLong(footerPos);
        final long actual = hashBytes(wholeFile, startIdx, (int) h.footerOffset());
        if (expected != actual) {
            throw new DataStorageManagerException("v2 page footer hash mismatch");
        }
        // parse index
        final ByteBuf indexSlice = wholeFile.slice(startIdx + FIXED_HEADER_SIZE, h.indexSize);
        final List<RecordMetadata> metadata = readIndex(indexSlice, h.numRecords);
        // extract values
        final int valueSectionStart = startIdx + FIXED_HEADER_SIZE + h.indexSize;
        final List<Record> result = new ArrayList<>(h.numRecords);
        for (RecordMetadata m : metadata) {
            final byte[] value = new byte[m.valueLength];
            if (m.valueLength > 0) {
                wholeFile.getBytes(valueSectionStart + (int) m.valueOffset, value);
            }
            result.add(new Record(m.key, Bytes.from_array(value)));
        }
        return result;
    }

    /**
     * Returns the absolute byte offset within the v2 file where the value for
     * a record with the given {@code valueOffset} (relative to the values
     * section) begins.
     */
    public static long absoluteValueOffset(FixedHeader h, long valueOffset) {
        return h.valueSectionStart() + valueOffset;
    }

    private static long hashBytes(ByteBuf buf, int offset, int length) {
        if (buf.hasArray()) {
            return XXHash64Utils.hash(buf.array(), buf.arrayOffset() + offset, length);
        }
        final byte[] tmp = new byte[length];
        buf.getBytes(offset, tmp);
        return XXHash64Utils.hash(tmp, 0, length);
    }
}
