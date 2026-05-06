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
import io.netty.buffer.ByteBufUtil;
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
     * + offHeap ref(4) + ownsSlice byte+pad(4) + slabOwner ref(4)
     * + deserialized ref(4) = 44 → padded to 48
     * <p>
     * Without compressed oops (heap &gt;= 32GB):
     * header(16) + buffer ref(8) + offset(4) + length(4) + hashCode(4)
     * + offHeap ref(8) + ownsSlice byte+pad(8) + slabOwner ref(8)
     * + deserialized ref(8) = 76 → padded to 80
     */
    private static final int CONSTANT_BYTE_SIZE = ObjectSizeUtils.COMPRESSED_OOPS
            ? 48
            : 80;

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
     * {@link #fromOffHeap(ByteBuf)} or {@link #fromSharedSlab(ByteBuf, int, int, Object)}.
     * {@code null} for on-heap-backed instances and after {@link #release()}
     * or after lazy materialisation. {@code volatile} so a reader observes a
     * consistent {@code (offHeap, buffer)} pair across the materialisation
     * transition.
     */
    private volatile ByteBuf offHeap;

    /**
     * When {@code true}, {@link #release()} and {@link #materialiseFromOffHeap()}
     * decrement the slice's refcount; this is the {@link #fromOffHeap(ByteBuf)}
     * lifecycle where each {@code Bytes} owns one refcount on its slice.
     *
     * <p>When {@code false}, the {@code Bytes} is a non-owning view into a
     * larger slab whose lifecycle is managed by an external owner (typically
     * a {@code DataPage} via a {@link java.lang.ref.Cleaner}); see
     * {@link #fromSharedSlab(ByteBuf, int, int, Object)}. {@code release()}
     * is a no-op and lazy materialisation copies bytes without touching the
     * slab's refcount.
     */
    private final boolean ownsSlice;

    /**
     * Strong reference held to anchor the slab-owner against premature GC,
     * for {@code Bytes} produced by {@link #fromSharedSlab(ByteBuf, int, int, Object)}.
     * {@code null} for owned slices and on-heap-backed instances.
     *
     * <p>This is the standard JDK {@code Cleaner} anchor pattern: as long as
     * any {@code Bytes} references its slab owner, the owner stays
     * GC-reachable and its {@code Cleanable} does not release the slab,
     * preventing a use-after-free for in-flight readers.
     *
     * <p>Issue #411: nulled out by {@link #materialiseAndDetach()} so a
     * separator key promoted out of an index node (e.g. BLink {@code rightsep}
     * after a split) no longer pins the donor's {@link IndexKeySlab}.
     * Always nulled under {@code synchronized (this)} together with
     * {@link #offHeap}; the {@code volatile} keyword carries the visibility
     * guarantee for unsynchronized GC tracking.
     */
    private volatile Object slabOwner;

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
        this.ownsSlice = false;
        this.slabOwner = null;
    }

    private Bytes(byte[] data, int offset, int length) {
        this.buffer = data;
        this.offset = offset;
        this.length = length;
        this.ownsSlice = false;
        this.slabOwner = null;
    }

    /** Internal off-heap constructor; see {@link #fromOffHeap(ByteBuf)}. */
    private Bytes(ByteBuf slice) {
        this.buffer = null;
        this.offset = 0;
        this.length = slice.readableBytes();
        this.offHeap = slice;
        this.ownsSlice = true;
        this.slabOwner = null;
    }

    /**
     * Internal shared-slab constructor; see
     * {@link #fromSharedSlab(ByteBuf, int, int, Object)}.
     */
    private Bytes(ByteBuf sharedSlice, Object owner) {
        this.buffer = null;
        this.offset = 0;
        this.length = sharedSlice.readableBytes();
        this.offHeap = sharedSlice;
        this.ownsSlice = false;
        this.slabOwner = owner;
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
     * Wraps a non-owning slice of an externally-managed slab into a
     * {@code Bytes}. Used by {@code DataPage}, BLink and BRIN slab-packing
     * paths where many records share a single off-heap slab whose lifecycle
     * is governed by the slab owner (typically via a JDK {@link java.lang.ref.Cleaner}).
     *
     * <p>Semantics that differ from {@link #fromOffHeap(ByteBuf)}:
     * <ul>
     *   <li>{@link #release()} on this {@code Bytes} is a <b>no-op</b> — the
     *       caller must not release the slice, because it shares the slab's
     *       refcount and a premature decrement would invalidate every other
     *       slice on the same slab. The slab owner releases the slab once
     *       (decrementing the shared refcount) when its lifecycle ends.</li>
     *   <li>{@link #getBuffer()} (and any byte[] accessor) still copies the
     *       slice into a fresh {@code byte[]}, but does <b>not</b> release
     *       the slice for the same reason. After materialisation the
     *       {@code Bytes} behaves exactly like an on-heap one.</li>
     *   <li>The {@code Bytes} retains a strong reference to {@code owner}
     *       (typically the {@code DataPage} or BLink/BRIN node holding the
     *       slab) so the JDK {@code Cleaner} attached to {@code owner} does
     *       not run while any reader still holds this {@code Bytes}. This is
     *       the standard {@code Cleaner} anchor pattern; without it a
     *       reader could observe a use-after-free.</li>
     * </ul>
     *
     * @param slice  a non-owning slice (typically {@code slab.slice(off, len)});
     *               the returned {@code Bytes} reads bytes via the slice but
     *               never releases it.
     * @param owner  the slab owner whose {@code Cleaner.Cleanable} releases
     *               the slab; must be the object the {@code Cleaner} is
     *               attached to so that GC keeps it alive while readers
     *               exist.
     * @throws NullPointerException if {@code slice} or {@code owner} is null.
     */
    public static Bytes fromSharedSlab(ByteBuf slice, Object owner) {
        if (slice == null) {
            throw new NullPointerException("slice");
        }
        if (owner == null) {
            throw new NullPointerException("owner");
        }
        return new Bytes(slice, owner);
    }

    /**
     * Convenience overload that takes the slab plus an explicit
     * {@code (offset, length)} window. Internally builds {@code slab.slice(offset, length)}.
     */
    public static Bytes fromSharedSlab(ByteBuf slab, int offset, int length, Object owner) {
        if (slab == null) {
            throw new NullPointerException("slab");
        }
        if (owner == null) {
            throw new NullPointerException("owner");
        }
        return new Bytes(slab.slice(offset, length), owner);
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
        if (local == null) {
            return;
        }
        if (ownsSlice) {
            offHeap = null;
            local.release();
        }
        // Shared-slab Bytes does NOT release: the slab owner (anchored by
        // slabOwner) owns the single refcount on the shared slab and will
        // release it via its Cleaner. We deliberately keep `offHeap` alive
        // here so the bytes remain readable for the slab's lifetime.
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
        if (ownsSlice) {
            offHeap = null;
            local.release();
        }
        // Shared-slab Bytes: keep the slice reference alive for any concurrent
        // off-heap reader (the slab owner's quiescence contract excludes
        // races, but losing the field would also break the slabOwner GC
        // anchor). The slab will be released by the slab owner's Cleaner.
    }

    /**
     * <b>Restricted-use API.</b> This method is intended <em>only</em> for
     * the BLink split / half-merge separator-handoff sites in
     * {@code herddb-utils}'s {@code BLink} (via the
     * {@code SizeEvaluator.detachSeparator} hook). Do not call it from
     * request-serving code or anywhere a sibling thread might be inside
     * a read path on this {@code Bytes}: the same quiescence contract
     * documented on {@link #release()} applies, and a violating concurrent
     * reader can observe an {@link IllegalStateException} or — for owned
     * slices — a use-after-free.
     *
     * <p>Eagerly copies the off-heap bytes into a private on-heap
     * {@code byte[]} and breaks every link to the underlying slab. After
     * this call the {@code Bytes} behaves exactly like an instance built
     * via {@link #from_array(byte[])}: {@link #isOffHeap()} returns
     * {@code false}, {@link #isShared()} returns {@code false}, no
     * {@link IndexKeySlab} is pinned, and equality / hashing semantics
     * are unchanged.
     *
     * <h3>Use case (issue #411)</h3>
     * BLink {@code Node.split()} promotes one of its keys to become the new
     * {@code rightsep} on the right sibling, and {@code Node.half_merge()}
     * inherits the right sibling's {@code rightsep}. Both promotions hand a
     * key that came from the donor's per-page {@link IndexKeySlab} into a
     * slot that may outlive the donor (after the donor is unloaded the
     * separator stays alive on the BLink anchor / parent index). Without a
     * defensive copy, that single separator pins the donor's multi-KiB slab
     * indefinitely. Calling {@code materialiseAndDetach()} at the handoff
     * site copies the bytes onto the heap and clears the slab anchor, so
     * the donor's slab can be reclaimed once the rest of its keys are
     * evicted.
     *
     * <h3>Semantics by current state</h3>
     * <ul>
     *   <li><b>Already on-heap</b> (this includes a previously-detached
     *       instance and a shared-slab instance whose lazy materialisation
     *       has already run): no-op, returns {@code this}.</li>
     *   <li><b>Owned-slice off-heap</b> ({@link #fromOffHeap(ByteBuf)}):
     *       copies the slice into a fresh {@code byte[]}, releases the
     *       slice (matching the existing {@link #materialiseFromOffHeap()}
     *       contract), nulls {@link #offHeap}.</li>
     *   <li><b>Shared-slab off-heap</b> ({@link #fromSharedSlab(ByteBuf, int, int, Object)}):
     *       copies the slice into a fresh {@code byte[]}, then nulls both
     *       {@link #offHeap} and {@link #slabOwner}. The slab's refcount is
     *       <em>not</em> decremented (the slab owner still holds the single
     *       refcount); however, dropping the slab-owner anchor frees the
     *       JDK {@link java.lang.ref.Cleaner} attached to the slab to run
     *       once every other slice on the same slab also becomes
     *       GC-unreachable.</li>
     * </ul>
     *
     * <h3>Quiescence contract</h3>
     * Same as {@link #release()} and {@link #materialiseFromOffHeap()}:
     * the caller must guarantee no other thread is inside a read path on
     * this {@code Bytes} at the moment of detach. The BLink call sites
     * satisfy this naturally — the rightsep handoff in {@code split()} /
     * {@code half_merge()} runs under the donor / receiver write lock and
     * before any reader could observe the new separator slot.
     *
     * <h3>Idempotency</h3>
     * Calling {@code materialiseAndDetach()} a second (or n-th) time on
     * the same instance is a no-op: the first call sets {@link #buffer},
     * and every subsequent call observes that and returns immediately.
     *
     * @return {@code this}, for fluent chaining
     * @throws IllegalStateException if {@link #release()} ran before
     *         materialisation and the bytes are no longer reachable.
     */
    public synchronized Bytes materialiseAndDetach() {
        if (buffer != null) {
            // Already on-heap, or a previous detach / lazy materialisation
            // already copied the bytes. For a shared-slab instance whose
            // lazy materialisation ran (offHeap still non-null, slabOwner
            // still non-null), this also clears the residual anchor so
            // detach is fully effective even on those instances.
            if (offHeap != null) {
                offHeap = null;
            }
            if (slabOwner != null) {
                slabOwner = null;
            }
            return this;
        }
        ByteBuf local = offHeap;
        if (local == null) {
            throw new IllegalStateException("Bytes already released");
        }
        byte[] copy = new byte[length];
        if (length > 0) {
            local.getBytes(local.readerIndex(), copy, 0, length);
        }
        buffer = copy;
        offset = 0;
        offHeap = null;
        if (ownsSlice) {
            // Owned slice: this Bytes held the single refcount; release it
            // exactly like materialiseFromOffHeap does.
            local.release();
        }
        // Shared-slab: do NOT release; the slab owner owns the refcount.
        // Drop the slab anchor so GC can reclaim the slab owner once every
        // other slice on the slab also becomes unreachable.
        slabOwner = null;
        return this;
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
     *
     * <p>Issue #429 removed the per-call temporary {@code byte[length]}
     * allocation (which dominated the alloc profile on the off-heap key
     * insert path) by delegating to
     * {@link CompareBytesUtils#hashCode(ByteBuf, int, int)}. That helper
     * fast-paths heap-backed {@link ByteBuf} instances through the
     * tight byte[] loop and reads off-heap slices directly via
     * {@link ByteBuf#getByte} with no copy.
     */
    private static int hashCodeFromByteBuf(ByteBuf buf, int start, int len) {
        return CompareBytesUtils.hashCode(buf, start, len);
    }

    /**
     * Byte-for-byte comparison that handles all four (on/off)-heap × (on/off)-heap
     * combinations. Lengths must already match.
     *
     * <p>The implementation funnels every case it can through the tight
     * {@link CompareBytesUtils#arraysEquals byte[]} comparison
     * (which itself dispatches to
     * {@link PlatformDependent#equals(byte[], int, byte[], int, int)} and
     * compares 8-byte words at a time). Specifically:
     * <ul>
     *   <li>Both sides on-heap: directly use the byte[] path.</li>
     *   <li>An off-heap side whose backing {@link ByteBuf} exposes a
     *       backing array via {@link ByteBuf#hasArray()} is promoted to
     *       its array for the comparison.</li>
     *   <li>Both sides off-heap with no backing array: dispatch to
     *       {@link ByteBufUtil#equals(ByteBuf, int, ByteBuf, int, int)}
     *       which does an 8-byte word-at-a-time comparison via
     *       {@link ByteBuf#getLong}, again without copying.</li>
     *   <li>Mixed (one byte[], one truly direct {@link ByteBuf}):
     *       compare 8 bytes at a time using
     *       {@link Bytes#toRawLong(byte[], int)} on the heap side and a
     *       big-endian-canonicalised {@link ByteBuf#getLong} on the
     *       direct side, with a byte tail.</li>
     * </ul>
     * No path allocates a {@code byte[]} (the previous implementation
     * fell back to a per-byte loop in the mixed case to avoid a bulk
     * copy; we now get the same no-allocation behaviour with 8x fewer
     * virtual calls on the hot path).
     *
     * @throws IllegalStateException if either side has been {@link #release()}d
     *         without a preceding lazy materialisation.
     */
    private static boolean bytesEqual(Bytes a, Bytes b) {
        final int len = a.length;

        // Snapshot all four references once so the JIT can eliminate
        // the volatile reads and reason about the local copies.
        byte[] aBuf = a.buffer;
        byte[] bBuf = b.buffer;

        // Hot path: both on-heap (or already materialised).
        if (aBuf != null && bBuf != null) {
            return CompareBytesUtils.arraysEquals(aBuf, a.offset, a.offset + len,
                    bBuf, b.offset, b.offset + len);
        }

        ByteBuf aSlice = a.offHeap;
        ByteBuf bSlice = b.offHeap;
        if ((aBuf == null && aSlice == null) || (bBuf == null && bSlice == null)) {
            throw new IllegalStateException("Bytes already released");
        }

        // Promote heap-backed ByteBufs to their underlying array so the
        // hot byte[]-vs-byte[] path can be reused.
        int aOff = a.offset;
        int bOff = b.offset;
        if (aBuf == null && aSlice.hasArray()) {
            aBuf = aSlice.array();
            aOff = aSlice.arrayOffset() + aSlice.readerIndex();
        }
        if (bBuf == null && bSlice.hasArray()) {
            bBuf = bSlice.array();
            bOff = bSlice.arrayOffset() + bSlice.readerIndex();
        }
        if (aBuf != null && bBuf != null) {
            return CompareBytesUtils.arraysEquals(aBuf, aOff, aOff + len,
                    bBuf, bOff, bOff + len);
        }

        // Both sides truly direct (no backing array on either): Netty's
        // ByteBufUtil.equals reads 8-byte longs from each side via
        // ByteBuf.getLong (which on a Direct buffer maps to a single
        // Unsafe access) and compares them, with a byte tail. This is
        // the fastest portable comparison we can do here.
        if (aBuf == null && bBuf == null) {
            return ByteBufUtil.equals(aSlice, aSlice.readerIndex(),
                    bSlice, bSlice.readerIndex(), len);
        }

        // Mixed (one byte[], one truly direct ByteBuf): chunked
        // long-at-a-time compare with byte tail. Both sides
        // canonicalised to big-endian so the long equality test is a
        // valid byte-equality test regardless of the ByteBuf's
        // configured order.
        final byte[] heap = aBuf != null ? aBuf : bBuf;
        final int heapOff = aBuf != null ? aOff : bOff;
        final ByteBuf direct = aBuf == null ? aSlice : bSlice;
        final int directOff = direct.readerIndex();
        return equalsHeapVsDirect(heap, heapOff, direct, directOff, len);
    }

    private static boolean equalsHeapVsDirect(byte[] heap, int hOff, ByteBuf direct, int dOff, int length) {
        final int longCount = length >>> 3;
        final int tail = length & 7;
        final boolean directIsBigEndian = direct.order() == ByteOrder.BIG_ENDIAN;
        for (int i = 0; i < longCount; i++) {
            final int hp = hOff + (i << 3);
            final int dp = dOff + (i << 3);
            // toRawLong returns the eight bytes interpreted as a
            // big-endian 64-bit value. ByteBuf.getLong does the same on
            // a default (BE) buffer; on a configured-LE buffer we flip
            // the result so the equality predicate is purely about the
            // underlying bytes.
            final long h = toRawLong(heap, hp);
            long d = direct.getLong(dp);
            if (!directIsBigEndian) {
                d = Long.reverseBytes(d);
            }
            if (h != d) {
                return false;
            }
        }
        final int hTail = hOff + (longCount << 3);
        final int dTail = dOff + (longCount << 3);
        for (int i = 0; i < tail; i++) {
            if (heap[hTail + i] != direct.getByte(dTail + i)) {
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
        // Off-heap-aware path. We snapshot offHeap and the slice's reader
        // index once at the start so the inner loop avoids the per-byte
        // volatile + virtual-call cost of byteAt(). For small index keys
        // (8-24 bytes typical for BLink) this is faster than a bulk
        // getBytes() copy because the per-call byte[] allocation dominates
        // for tiny payloads (issue #399 step-4 review).
        ByteBuf aSlice = this.offHeap;
        ByteBuf bSlice = o.offHeap;
        int aBaseIdx = aSlice == null ? 0 : aSlice.readerIndex();
        int bBaseIdx = bSlice == null ? 0 : bSlice.readerIndex();
        int aOff = this.offset;
        int bOff = o.offset;
        int min = Math.min(this.length, o.length);
        for (int i = 0; i < min; i++) {
            int a = (aBuf != null ? aBuf[aOff + i] : aSlice.getByte(aBaseIdx + i)) & 0xff;
            int b = (bBuf != null ? bBuf[bOff + i] : bSlice.getByte(bBaseIdx + i)) & 0xff;
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
