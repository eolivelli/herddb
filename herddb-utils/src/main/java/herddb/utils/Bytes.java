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

package herddb.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * A wrapper for byte[], in order to use it as keys on HashMaps.
 *
 * <h3>Storage modes</h3>
 * Two backing representations are supported:
 * <ul>
 *   <li><b>On-heap</b> (the default): {@code buffer + offset + length} hold a
 *       {@code byte[]} slice. Effectively immutable; safely publishable by any
 *       constructor that initialises {@link #buffer} and {@link #offset}.</li>
 *   <li><b>Off-heap</b> (via {@link #fromOffHeap(ByteBuf)}, used by issue #399's
 *       slab owners): {@code offHeap} holds a Netty {@link ByteBuf} slice into
 *       a pooled direct-memory slab. The on-heap fields stay {@code null}/0
 *       until the first {@code byte[]} accessor lazily materialises the bytes
 *       and releases the slice.</li>
 * </ul>
 *
 * <h3>Lifecycle of off-heap-backed instances</h3>
 * {@code release()} returns the slice to its pool. After {@code release()}
 * (without a preceding lazy materialisation) every accessor that reads bytes
 * throws {@link IllegalStateException}. The slab owner must guarantee
 * <em>quiescence</em> (no other thread can be inside any read path on this
 * {@code Bytes}) before calling {@code release()} — the standard discipline
 * for pooled buffers. In practice the pattern is:
 * <pre>
 *   1) remove the {@code Bytes} (and the surrounding {@code Record} / index node)
 *      from every {@code ConcurrentHashMap} / scan structure that could expose it;
 *   2) wait for any in-flight reader (e.g. drain a page-level read lock);
 *   3) call {@code release()}.
 * </pre>
 *
 * <h3>Thread-safety</h3>
 * Concurrent readers of an on-heap-backed {@code Bytes} need no synchronisation:
 * {@link #buffer}, {@link #offset}, {@link #offHeap} and {@link #hashCode} are
 * declared {@code volatile} so that reads observe the constructor-time writes
 * via the JLS happens-before edge, even though {@code buffer} and {@code offset}
 * lost their {@code final} modifier to support the off-heap path's lazy
 * materialisation. Off-heap-backed reads remain safe under the quiescence
 * contract above; cross-thread reads concurrent with {@code release()} are
 * undefined and are the slab owner's bug.
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2", "EI_EXPOSE_REP", "UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD",
        // synchronized writers (release / materialiseFromOffHeap) vs. unsynchronized
        // readers — tolerated under the quiescence contract documented on
        // release(); volatile fields provide the necessary visibility.
        "IS2_INCONSISTENT_SYNC"})
public final class Bytes implements Comparable<Bytes>, SizeAwareObject {

    public static final Bytes POSITIVE_INFINITY = new Bytes(new byte[0]);

    public static final Bytes EMPTY_ARRAY = new Bytes(new byte[0]);

    private static final boolean UNALIGNED = PlatformDependent.isUnaligned();
    private static final boolean HAS_UNSAFE = PlatformDependent.hasUnsafe();
    private static final boolean BIG_ENDIAN_NATIVE_ORDER = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    /**
     * Sign-bit XOR masks used by {@link #putInt}/{@link #putLong}/{@link #toInt}/{@link #toLong}
     * to produce an order-preserving big-endian encoding: unsigned-lex byte comparison on the
     * encoded bytes matches signed numeric order, so int/long primary keys naturally sort.
     * Encoders and decoders both flip the sign bit, so Java values round-trip identically.
     * The IEEE-754 bit transport for float/double goes through the raw variants
     * ({@link #putRawInt}/{@link #putRawLong}/{@link #toRawInt}/{@link #toRawLong}) and is unaffected.
     */
    private static final int INT_SIGN_FLIP_MASK = 0x80000000;
    private static final long LONG_SIGN_FLIP_MASK = 0x8000000000000000L;

    /**
     * Estimated size of a Bytes instance (excluding the data array contents).
     * <p>
     * With compressed oops (heap &lt; 32GB):
     * header(12) + buffer ref(4) + offset(4) + length(4) + hashCode(4)
     * + offHeap ref(4) + deserialized ref(4) = 36 bytes → padded to 40
     * <p>
     * Without compressed oops (heap &gt;= 32GB):
     * header(16) + buffer ref(8) + offset(4) + length(4) + hashCode(4)
     * + offHeap ref(8) + deserialized ref(8) + padding(4) = 56 bytes
     */
    private static final int CONSTANT_BYTE_SIZE = ObjectSizeUtils.COMPRESSED_OOPS
            ? 40
            : 56;

    public static long estimateSize(byte[] value) {
        return value.length + CONSTANT_BYTE_SIZE;
    }

    /**
     * On-heap backing array. Lazily materialised when this {@code Bytes} was
     * constructed off-heap and a byte[] accessor is invoked. Once materialised
     * the off-heap reference is released and {@code buffer} stays cached so
     * subsequent reads are O(1). {@code volatile} so a reader on another core
     * sees the materialised value without acquiring the instance monitor.
     */
    private volatile byte[] buffer;
    private volatile int offset;
    private final int length;
    private volatile int hashCode = -1;

    /**
     * Off-heap backing slice when this {@code Bytes} was constructed via
     * {@link #fromOffHeap(ByteBuf)}. {@code null} for on-heap-backed
     * instances and after {@link #release()} or after lazy materialisation.
     * {@code volatile} so a reader observes a consistent
     * {@code (offHeap, buffer)} pair across the materialisation transition.
     */
    private volatile ByteBuf offHeap;

    public Object deserialized;

    @Override
    public long getEstimatedSize() {
        return length + CONSTANT_BYTE_SIZE;
    }

    public static byte[] string_to_array(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static Bytes from_string(String s) {
        return new Bytes(s.getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] longToByteArray(long value) {
        byte[] res = new byte[8];
        putLong(res, 0, value);
        return res;
    }

    public static byte[] intToByteArray(int value) {
        byte[] res = new byte[4];
        putInt(res, 0, value);
        return res;
    }

    public static byte[] doubleToByteArray(double value) {
        byte[] res = new byte[8];
        putRawLong(res, 0, Double.doubleToLongBits(value));
        return res;
    }

    public static byte[] timestampToByteArray(java.sql.Timestamp value) {
        byte[] res = new byte[8];
        putLong(res, 0, value.getTime());
        return res;
    }

    private static final byte[] BOOLEAN_TRUE = {1};
    private static final byte[] BOOLEAN_FALSE = {0};

    @SuppressFBWarnings("MS_EXPOSE_REP")
    public static byte[] booleanToByteArray(boolean value) {
        return value ? BOOLEAN_TRUE : BOOLEAN_FALSE;
    }

    public static Bytes from_long(long value) {
        byte[] res = new byte[8];
        putLong(res, 0, value);
        return new Bytes(res);
    }

    public static Bytes from_array(byte[] data) {
        return new Bytes(data);
    }

    public static Bytes from_array(byte[] data, int offset, int len) {
        return new Bytes(data, offset, len);
    }

    public static Bytes from_nullable_array(byte[] data) {
        if (data == null) {
            return null;
        }
        return new Bytes(data);
    }

    public byte[] to_array() {
        byte[] data = getBuffer();
        int srcOffset = offset;
        if (srcOffset == 0 && data.length == length) {
            return data;
        }
        byte[] copy = new byte[length];
        System.arraycopy(data, srcOffset, copy, 0, length);
        return copy;
    }

    public float[] to_float_array() {
        return to_float_array(getBuffer(), getOffset(), length);
    }

    public static float[] to_float_array(byte[] buffer, int offset, int length) {
        if (length % 4 != 0) {
            throw new IllegalArgumentException("Invalid byte array length");
        }
        int floatCount = length / 4;
        float[] result = new float[floatCount];
        if (HAS_UNSAFE && UNALIGNED) {
            for (int i = 0; i < floatCount; i++) {
                int v = PlatformDependent.getInt(buffer, offset);
                if (!BIG_ENDIAN_NATIVE_ORDER) {
                    v = Integer.reverseBytes(v);
                }
                result[i] = Float.intBitsToFloat(v);
                offset += 4;
            }
        } else {
            for (int i = 0; i < floatCount; i++) {
                int v = (buffer[offset] & 0xff) << 24
                        | (buffer[offset + 1] & 0xff) << 16
                        | (buffer[offset + 2] & 0xff) << 8
                        | buffer[offset + 3] & 0xff;
                result[i] = Float.intBitsToFloat(v);
                offset += 4;
            }
        }
        return result;
    }

    public static Bytes from_float_array(float[] floatArray) {
        int length = floatArray.length;
        byte[] res = new byte[length * 4];
        int offset = 0;
        for (float f : floatArray) {
            putRawInt(res, offset, Float.floatToRawIntBits(f));
            offset += 4;
        }
        return new Bytes(res);
    }

    public static Bytes from_int(int value) {
        byte[] res = new byte[4];
        putInt(res, 0, value);
        return new Bytes(res);
    }

    public static Bytes from_timestamp(java.sql.Timestamp value) {
        byte[] res = new byte[8];
        putLong(res, 0, value.getTime());
        return new Bytes(res);
    }

    public static Bytes from_boolean(boolean value) {
        return new Bytes(booleanToByteArray(value));
    }

    public static Bytes from_double(double value) {
        byte[] res = new byte[8];
        putRawLong(res, 0, Double.doubleToRawLongBits(value));
        return new Bytes(res);
    }

    public long to_long() {
        assert length == 8;
        return toLong(getBuffer(), getOffset());
    }

    public RawString to_RawString() {
        return RawString.newUnpooledRawString(getBuffer(), getOffset(), length);
    }

    public int to_int() {
        assert length == 4;
        return toInt(getBuffer(), getOffset());
    }

    public String to_string() {
        return new String(getBuffer(), getOffset(), length, StandardCharsets.UTF_8);
    }

    public static String to_string(byte[] data) {
        return new String(data, 0, data.length, StandardCharsets.UTF_8);
    }

    public static RawString to_rawstring(byte[] data) {
        return RawString.newUnpooledRawString(data, 0, data.length);
    }

    public java.sql.Timestamp to_timestamp() {
        assert length == 8;
        return toTimestamp(getBuffer(), getOffset());
    }

    public boolean to_boolean() {
        assert length == 1;
        return toBoolean(getBuffer(), getOffset());
    }

    public double to_double() {
        assert length == 8;
        return toDouble(getBuffer(), getOffset());
    }

    private Bytes(byte[] data) {
        this.buffer = data;
        this.offset = 0;
        this.length = buffer.length;
    }

    private Bytes(byte[] data, int offset, int length) {
        this.buffer = data;
        this.offset = offset;
        this.length = length;
    }

    /** Internal off-heap constructor; see {@link #fromOffHeap(ByteBuf)}. */
    private Bytes(ByteBuf slice) {
        this.buffer = null;
        this.offset = 0;
        this.length = slice.readableBytes();
        this.offHeap = slice;
    }

    /**
     * Wraps a {@link ByteBuf} slice into a {@code Bytes}, taking ownership of
     * one refcount on the slice. The slice's {@link ByteBuf#readableBytes()}
     * is the value's length; {@link ByteBuf#readerIndex()} is the start.
     *
     * <p>The intended workflow is:
     * <pre>
     *   ByteBuf slab = HerdDBByteBufAllocators.dataPagesAllocator()
     *       .directBuffer(totalSize);
     *   slab.writeBytes(payload);
     *   ByteBuf slice = slab.retainedSlice(off, len); // bump slab refcount
     *   Bytes b = Bytes.fromOffHeap(slice);
     * </pre>
     *
     * <p>The slice may be backed by direct or heap memory: this method does
     * not enforce the storage class. Issue-#399 callers always pass a direct
     * slice from {@code HerdDBByteBufAllocators.dataPagesAllocator()} or
     * {@code .indexPagesAllocator()}.
     *
     * <p>After {@link #release()} the underlying memory is returned to its
     * pool. Subsequent on-heap accessors ({@link #getBuffer()},
     * {@link #to_array()}, etc.) on a not-yet-released instance lazily
     * materialise the bytes into a fresh {@code byte[]} and release the
     * slice; after such materialisation the {@code Bytes} behaves like an
     * on-heap one and {@link #isShared()} returns {@code false} (the
     * materialised array is private and exactly {@code length} bytes long).
     *
     * <p>Callers must not retain or read the slice after this method returns;
     * lifecycle ownership transfers to the returned {@code Bytes}.
     *
     * @throws NullPointerException if {@code slice} is null.
     */
    public static Bytes fromOffHeap(ByteBuf slice) {
        if (slice == null) {
            throw new NullPointerException("slice");
        }
        return new Bytes(slice);
    }

    /**
     * Returns {@code true} if this {@code Bytes} is currently backed by an
     * off-heap {@link ByteBuf} slice (i.e. {@link #fromOffHeap(ByteBuf)} was
     * used to construct it and neither {@link #release()} nor lazy
     * materialisation has run yet).
     */
    public boolean isOffHeap() {
        return offHeap != null;
    }

    /**
     * Releases the underlying off-heap slice if any. Idempotent: calling
     * {@code release()} more than once, on an on-heap-backed {@code Bytes},
     * or after lazy materialisation is a no-op.
     *
     * <p><b>Quiescence contract</b>: after {@code release()} every accessor
     * that reads bytes (including {@link #getBuffer()}, {@link #to_array()},
     * {@link #to_long()}, {@link #equals(Object)}, {@link #hashCode()},
     * {@link #compareTo(Bytes)}, {@link #writeTo(ByteBuf)}, etc.) throws
     * {@link IllegalStateException}. The slab owner is responsible for
     * guaranteeing that no other thread is inside any read path on this
     * {@code Bytes} when {@code release()} is invoked. The standard pattern
     * — used by issue #399's slab owners — is:
     * <pre>
     *   1) remove the {@code Bytes} (and the surrounding {@code Record} or
     *      index node) from every map / scan structure that could expose it;
     *   2) drain any in-flight reader (e.g. acquire the page-level read
     *      lock, or wait for the slab's grace period);
     *   3) call {@code release()}.
     * </pre>
     * Concurrent reads racing against {@code release()} are undefined
     * behaviour and must be prevented by the slab owner.
     *
     * <p><b>The same quiescence requirement applies to any heap-accessor
     * call</b> on an off-heap-backed {@code Bytes}, because such a call may
     * trigger {@link #materialiseFromOffHeap()} which itself releases the
     * slice. A reader inside {@link #writeTo(ByteBuf)} / {@link #writeTo(OutputStream)}
     * / {@link #compareTo(Bytes)} / {@link #equals(Object)} / {@link #hashCode()}
     * snapshots {@code offHeap} into a local; if a sibling thread runs
     * {@link #getBuffer()} (or any other on-heap accessor) concurrently and
     * the slab returned to the pool while the snapshot is still in use, the
     * reader observes a use-after-free. The slab owner must therefore ensure
     * single-threaded access (or external synchronisation) before triggering
     * a materialising read on a {@code Bytes} that other threads may still
     * hold off-heap-backed references to.
     */
    public synchronized void release() {
        ByteBuf local = offHeap;
        if (local != null) {
            offHeap = null;
            local.release();
        }
    }

    /**
     * Lazily materialises the off-heap bytes into a fresh {@code byte[]}
     * cached in {@link #buffer}, then releases the slice. Idempotent.
     *
     * <p>This call counts as an internal {@link #release()} of the slice from
     * the slab-owner's point of view: it is subject to the same quiescence
     * contract documented on {@link #release()}. Concurrent threads holding
     * off-heap-backed snapshots of this {@code Bytes} must be drained by the
     * slab owner before any thread invokes a heap accessor.
     *
     * @throws IllegalStateException if {@link #release()} ran before
     *         materialisation: the bytes are no longer reachable.
     */
    private synchronized void materialiseFromOffHeap() {
        if (buffer != null) {
            return;
        }
        ByteBuf local = offHeap;
        if (local == null) {
            // release() ran without a preceding lazy materialisation. The
            // bytes are gone — surface as an IllegalStateException so callers
            // see a localised, actionable failure instead of a silent NPE
            // downstream.
            throw new IllegalStateException("Bytes already released");
        }
        byte[] copy = new byte[length];
        if (length > 0) {
            local.getBytes(local.readerIndex(), copy, 0, length);
        }
        buffer = copy;
        offset = 0;
        offHeap = null;
        local.release();
    }

    /**
     * Writes the value bytes directly into {@code dst} without materialising a
     * heap {@code byte[]}. Off-heap-backed instances copy slice → dst with no
     * intermediate allocation; on-heap-backed instances delegate to
     * {@link ByteBuf#writeBytes(byte[], int, int)}.
     *
     * @throws IllegalStateException if this {@code Bytes} was released without
     *         a preceding lazy materialisation.
     */
    public void writeTo(ByteBuf dst) {
        ByteBuf local = offHeap;
        byte[] data = buffer;
        if (data != null) {
            dst.writeBytes(data, offset, length);
            return;
        }
        if (local != null) {
            dst.writeBytes(local, local.readerIndex(), length);
            return;
        }
        throw new IllegalStateException("Bytes already released");
    }

    /**
     * Writes the value bytes directly into {@code out}. Off-heap-backed
     * instances copy via a single {@link ByteBuf#getBytes(int, OutputStream, int)}
     * call (no allocation); on-heap-backed instances delegate to
     * {@link OutputStream#write(byte[], int, int)}.
     *
     * @throws IllegalStateException if this {@code Bytes} was released without
     *         a preceding lazy materialisation.
     */
    public void writeTo(OutputStream out) throws IOException {
        ByteBuf local = offHeap;
        byte[] data = buffer;
        if (data != null) {
            out.write(data, offset, length);
            return;
        }
        if (local != null) {
            local.getBytes(local.readerIndex(), out, length);
            return;
        }
        throw new IllegalStateException("Bytes already released");
    }

    @Override
    public int hashCode() {
        int h = hashCode;
        if (h != -1) {
            return h;
        }
        // Snapshot the on-heap field first; if it's non-null we're done. Only
        // when the heap representation is missing do we fall to the off-heap
        // path, which has its own post-release check.
        byte[] data = buffer;
        if (data != null) {
            h = CompareBytesUtils.hashCode(data, offset, length);
        } else {
            ByteBuf local = offHeap;
            if (local == null) {
                throw new IllegalStateException("Bytes already released");
            }
            h = hashCodeFromByteBuf(local, local.readerIndex(), length);
        }
        this.hashCode = h;
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        try {
            final Bytes other = (Bytes) obj;
            if (length != other.length) {
                return false;
            }
            if (other.hashCode() != this.hashCode()) {
                return false;
            }
            return bytesEqual(this, other);
        } catch (ClassCastException otherClass) {
            return false;
        }
    }

    /**
     * {@code Arrays.hashCode}-compatible hash over a {@link ByteBuf} slice that
     * matches {@link CompareBytesUtils#hashCode(byte[], int, int)} byte-for-byte.
     * Used by the off-heap-backed equals/hashCode path so cross-comparison
     * between an off-heap and an on-heap {@code Bytes} with identical bytes
     * round-trips correctly.
     */
    private static int hashCodeFromByteBuf(ByteBuf buf, int start, int len) {
        int h = 1;
        for (int i = 0; i < len; i++) {
            h = 31 * h + buf.getByte(start + i);
        }
        return h;
    }

    /**
     * Byte-for-byte comparison that handles all four (on/off)-heap × (on/off)-heap
     * combinations without forcing materialisation. Lengths must already match.
     *
     * @throws IllegalStateException if either side has been {@link #release()}d
     *         without a preceding lazy materialisation.
     */
    private static boolean bytesEqual(Bytes a, Bytes b) {
        // Hot path: both on-heap (or already materialised). Snapshot the
        // fields once into locals so the JIT eliminates the volatile reads.
        byte[] aBuf = a.buffer;
        byte[] bBuf = b.buffer;
        if (aBuf != null && bBuf != null) {
            return CompareBytesUtils.arraysEquals(aBuf, a.offset, a.offset + a.length,
                    bBuf, b.offset, b.offset + b.length);
        }
        // At least one side is off-heap: compare via byte accessors. byteAt
        // re-snapshots the local fields each call; for hot off-heap paths
        // step 3+ will switch to a bulk getBytes copy.
        for (int i = 0; i < a.length; i++) {
            if (a.byteAt(i) != b.byteAt(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the {@code i}-th byte of the value, reading from whichever
     * representation is currently live (on-heap byte[] or off-heap slice).
     *
     * @throws IllegalStateException if {@link #release()} ran before
     *         materialisation: the bytes are no longer reachable.
     */
    private byte byteAt(int i) {
        byte[] data = buffer;
        if (data != null) {
            return data[offset + i];
        }
        ByteBuf local = offHeap;
        if (local != null) {
            return local.getByte(local.readerIndex() + i);
        }
        throw new IllegalStateException("Bytes already released");
    }

    /**
     * Encodes {@code value} as 8 order-preserving big-endian bytes (sign bit flipped),
     * so unsigned-lex comparison on the resulting bytes matches signed numeric order.
     */
    public static void putLong(byte[] array, int index, long value) {
        putRawLong(array, index, value ^ LONG_SIGN_FLIP_MASK);
    }

    /**
     * Encodes {@code value} as 4 order-preserving big-endian bytes (sign bit flipped),
     * so unsigned-lex comparison on the resulting bytes matches signed numeric order.
     */
    public static void putInt(byte[] array, int index, int value) {
        putRawInt(array, index, value ^ INT_SIGN_FLIP_MASK);
    }

    /**
     * Writes {@code value} as 8 plain big-endian bytes. Used for IEEE-754 bit
     * transport (double); not order-preserving.
     */
    public static void putRawLong(byte[] array, int index, long value) {
        if (HAS_UNSAFE && UNALIGNED) {
            PlatformDependent.putLong(array, index, BIG_ENDIAN_NATIVE_ORDER ? value : Long.reverseBytes(value));
        } else {
            array[index] = (byte) (value >>> 56);
            array[index + 1] = (byte) (value >>> 48);
            array[index + 2] = (byte) (value >>> 40);
            array[index + 3] = (byte) (value >>> 32);
            array[index + 4] = (byte) (value >>> 24);
            array[index + 5] = (byte) (value >>> 16);
            array[index + 6] = (byte) (value >>> 8);
            array[index + 7] = (byte) value;
        }
    }

    /**
     * Writes {@code value} as 4 plain big-endian bytes. Used for IEEE-754 bit
     * transport (float); not order-preserving.
     */
    public static void putRawInt(byte[] array, int index, int value) {
        if (HAS_UNSAFE && UNALIGNED) {
            PlatformDependent.putInt(array, index, BIG_ENDIAN_NATIVE_ORDER ? value : Integer.reverseBytes(value));
        } else {
            array[index] = (byte) (value >>> 24);
            array[index + 1] = (byte) (value >>> 16);
            array[index + 2] = (byte) (value >>> 8);
            array[index + 3] = (byte) value;
        }
    }

    public static void putBoolean(byte[] bytes, int offset, boolean val) {
        if (val) {
            bytes[offset] = 1;
        } else {
            bytes[offset] = (byte) 0x00;
        }
    }

    public static void putDouble(byte[] bytes, int offset, double val) {
        putRawLong(bytes, offset, Double.doubleToRawLongBits(val));
    }

    /**
     * Decodes 8 order-preserving big-endian bytes (written by {@link #putLong})
     * back to a {@code long}.
     */
    public static long toLong(byte[] array, int index) {
        return toRawLong(array, index) ^ LONG_SIGN_FLIP_MASK;
    }

    /**
     * Reads 8 plain big-endian bytes. Used for IEEE-754 bit transport (double);
     * pairs with {@link #putRawLong}.
     */
    public static long toRawLong(byte[] array, int index) {
        if (HAS_UNSAFE && UNALIGNED) {
            long v = PlatformDependent.getLong(array, index);
            return BIG_ENDIAN_NATIVE_ORDER ? v : Long.reverseBytes(v);
        }

        return ((long) array[index] & 0xff) << 56
                | //
                ((long) array[index + 1] & 0xff) << 48
                | //
                ((long) array[index + 2] & 0xff) << 40
                | //
                ((long) array[index + 3] & 0xff) << 32
                | //
                ((long) array[index + 4] & 0xff) << 24
                | //
                ((long) array[index + 5] & 0xff) << 16
                | //
                ((long) array[index + 6] & 0xff) << 8
                | //
                (long) array[index + 7] & 0xff;
    }

    public static int compareInt(byte[] array, int index, int value) {
        return Integer.compare(toInt(array, index), value);
    }

    public static int compareInt(byte[] array, int index, long value) {
        return Long.compare(toInt(array, index), value);
    }

    public static int compareLong(byte[] array, int index, int value) {
        return Long.compare(toLong(array, index), value);
    }

    public static int compareLong(byte[] array, int index, long value) {
        return Long.compare(toLong(array, index), value);
    }

    /**
     * Decodes 4 order-preserving big-endian bytes (written by {@link #putInt})
     * back to an {@code int}.
     */
    public static int toInt(byte[] array, int index) {
        return toRawInt(array, index) ^ INT_SIGN_FLIP_MASK;
    }

    /**
     * Reads 4 plain big-endian bytes. Used for IEEE-754 bit transport (float);
     * pairs with {@link #putRawInt}.
     */
    public static int toRawInt(byte[] array, int index) {
        if (HAS_UNSAFE && UNALIGNED) {
            int v = PlatformDependent.getInt(array, index);
            return BIG_ENDIAN_NATIVE_ORDER ? v : Integer.reverseBytes(v);
        }

        return (array[index] & 0xff) << 24
                | //
                (array[index + 1] & 0xff) << 16
                | //
                (array[index + 2] & 0xff) << 8
                | //
                array[index + 3] & 0xff;
    }

    public static java.sql.Timestamp toTimestamp(byte[] bytes, int offset) {
        long l = toLong(bytes, offset);
        if (l < 0) {
            return null;
        }
        return new java.sql.Timestamp(l);
    }

    public static boolean toBoolean(byte[] bytes, int offset) {
        return bytes[offset] == 1;
    }

    public static double toDouble(byte[] bytes, int offset) {
        return Double.longBitsToDouble(toRawLong(bytes, offset));
    }

    public static int compare(byte[] left, byte[] right) {
        return CompareBytesUtils.compare(left, right);
    }

    @Override
    public int compareTo(Bytes o) {
        if (this == POSITIVE_INFINITY) {
            return this == o ? 0 : 1;
        } else if (o == POSITIVE_INFINITY) {
            return -1;
        }
        // Reject released receivers / arguments before short-circuiting on
        // length, otherwise a released zero-length Bytes would silently
        // compare equal to its sibling (length - o.length == 0).
        if (this.buffer == null && this.offHeap == null) {
            throw new IllegalStateException("Bytes already released");
        }
        if (o.buffer == null && o.offHeap == null) {
            throw new IllegalStateException("Bytes already released");
        }
        // Hot path: both sides have on-heap byte[] (either natively or after
        // lazy materialisation). Snapshot fields once so the volatile reads
        // are folded by the JIT.
        byte[] aBuf = this.buffer;
        byte[] bBuf = o.buffer;
        if (aBuf != null && bBuf != null) {
            return CompareBytesUtils.compare(aBuf, this.offset, this.offset + length,
                    bBuf, o.offset, o.offset + o.length);
        }
        // Off-heap-aware path: byte-by-byte unsigned comparison via byteAt().
        int min = Math.min(this.length, o.length);
        for (int i = 0; i < min; i++) {
            int a = this.byteAt(i) & 0xff;
            int b = o.byteAt(i) & 0xff;
            if (a != b) {
                return a - b;
            }
        }
        return this.length - o.length;
    }

    public static boolean startsWith(byte[] left, int offset, int bufferlen, int max, byte[] right) {
        int endleft = offset + bufferlen;
        int endmax = offset + max;
        for (int i = offset, j = 0; i < endleft && j < right.length && i < endmax; i++, j++) {
            if (left[i] != right[j]) {
                return false;
            }
        }
        // equality
        return true;
    }

    /**
     * Variant of {@link #startsWith(byte[], int, int, int, byte[])} that takes
     * an explicit offset into the right-hand prefix array. Used by the
     * {@link #startsWith(Bytes)} overload to avoid copying a {@code Bytes}
     * value into a fresh {@code byte[]} when comparing prefixes.
     */
    public static boolean startsWith(byte[] left, int leftOffset, int leftLength,
                                     int max, byte[] right, int rightOffset, int rightLength) {
        int endleft = leftOffset + leftLength;
        int endmax = leftOffset + max;
        int endright = rightOffset + rightLength;
        for (int i = leftOffset, j = rightOffset;
             i < endleft && j < endright && i < endmax;
             i++, j++) {
            if (left[i] != right[j]) {
                return false;
            }
        }
        // equality
        return true;
    }

    public int getLength() {
        return length;
    }

    /**
     * Returns the on-heap backing array. For an off-heap-backed instance this
     * triggers a lazy copy of the slice into a fresh {@code byte[]} and then
     * releases the slice. After the call this {@code Bytes} behaves like an
     * on-heap one (subsequent {@code getBuffer()} calls are O(1)).
     *
     * @throws IllegalStateException if {@link #release()} ran before
     *         materialisation: the bytes are no longer reachable.
     */
    public byte[] getBuffer() {
        byte[] data = buffer;
        if (data != null) {
            return data;
        }
        // materialiseFromOffHeap throws IllegalStateException if released.
        materialiseFromOffHeap();
        return buffer;
    }

    /**
     * Returns the byte offset within {@link #getBuffer()} where this value's
     * bytes start. Triggers lazy materialisation if needed; after that, the
     * offset for an off-heap-materialised instance is always 0.
     *
     * @throws IllegalStateException if {@link #release()} ran before
     *         materialisation.
     */
    public int getOffset() {
        if (buffer != null) {
            return offset;
        }
        materialiseFromOffHeap();
        return offset;
    }

    @Override
    public String toString() {
        if (buffer == null && offHeap == null) {
            return "null";
        }
        // ONLY FOR TESTS
        return arraytohexstring(getBuffer(), getOffset(), length);
    }

    public static String arraytohexstring(byte[] buffer, int offset, int length) {
        StringBuilder string = new StringBuilder();
        for (int i = offset; i < offset + length; i++) {
            byte b = buffer[i];
            String hexString = Integer.toHexString(0x00FF & b);
            string.append(hexString.length() == 1 ? "0" + hexString : hexString);
        }
        return string.toString();
    }

    public ByteArrayCursor newCursor() {
        return ByteArrayCursor.wrap(getBuffer(), getOffset(), length);
    }

    public ByteBufCursor newByteBufCursor() {
        return ByteBufCursor.wrap(getBuffer(), getOffset(), length);
    }

    /**
     * Returns the next {@code Bytes} instance.
     * <p>
     * Depending on current instance it couldn't be possible to evaluate the
     * next one: if every bit in current byte array is already 1 next would
     * generate an overflow and isn't permitted.
     * </p>
     *
     * @return the next Bytes instance
     * @throws IllegalStateException if cannot evaluate a next value.
     */
    public Bytes next() {

        final byte[] src = getBuffer();
        final int srcOffset = getOffset();
        final byte[] dst = new byte[length];
        System.arraycopy(src, srcOffset, dst, 0, length);

        int idx = length - 1;

        /*
         * We alter bytes from last in a backward fashion. We could have done directly a manual copy with
         * increment when needed but System.arraycopy is really faster than manual for loop copy and in
         * standard cases we just need to very fiew bytes (normally just one)
         */
        while (idx > -1 && ++dst[idx] == 0) {
            --idx;
        }

        /* If addition gone up to the byte array end then there isn't any more space */
        if (idx == -1) {
            throw new IllegalStateException(
                    "Cannot generate a next value for a full 1 byte array, no space for another element");
        }

        return new Bytes(dst);

    }

    public boolean startsWith(int length, byte[] prefix) {
        return Bytes.startsWith(this.getBuffer(), this.getOffset(), this.length, length, prefix);
    }

    /**
     * Returns {@code true} if this value starts with the bytes carried by
     * {@code prefix}. Zero-copy when both sides are on-heap (or already
     * materialised); off-heap-backed instances trigger lazy materialisation
     * via {@link #getBuffer()} so the existing static helper keeps working.
     */
    public boolean startsWith(Bytes prefix) {
        return Bytes.startsWith(this.getBuffer(), this.getOffset(), this.length,
                prefix.length, prefix.getBuffer(), prefix.getOffset(), prefix.length);
    }

    /**
     * Ensure that this value is not retaining strong references to a shared buffer
     *
     * @return the buffer itself or a copy
     */
    public Bytes nonShared() {
        if (isShared()) {
            byte[] src = getBuffer();
            int srcOffset = getOffset();
            byte[] array = new byte[this.length];
            System.arraycopy(src, srcOffset, array, 0, length);
            return new Bytes(array, 0, length);
        }
        return this;
    }

    public boolean isShared() {
        byte[] data = buffer;
        if (data == null) {
            // Off-heap-backed (not yet materialised) is considered shared
            // because it points into a shared off-heap slab managed by the
            // caller; nonShared() will materialise into a private byte[].
            if (offHeap != null) {
                return true;
            }
            // released without materialising — surface the released state
            // consistently with every other byte-reading accessor.
            throw new IllegalStateException("Bytes already released");
        }
        return this.offset != 0 || this.length != data.length;
    }

}
