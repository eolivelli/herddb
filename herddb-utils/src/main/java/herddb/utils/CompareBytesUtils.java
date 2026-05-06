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

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Byte-array comparison and hashing helpers.
 *
 * <p>The compare/equals helpers delegate to the JDK's
 * {@link Arrays#compareUnsigned} / {@link Arrays#equals} which are
 * intrinsified by HotSpot since Java 9.
 *
 * <p>{@link #hashCode(byte[], int, int)} and {@link #hashCode(ByteBuf, int, int)}
 * implement the same int-block-then-byte-tail polynomial as
 * {@link io.netty.buffer.ByteBufUtil#hashCode(ByteBuf)} and finalise
 * the result with a Knuth/golden-ratio multiplicative mix to defend
 * against {@link java.util.concurrent.ConcurrentHashMap} bin
 * treeification on tightly clustered inputs (issue #429).
 */
public final class CompareBytesUtils {

    private CompareBytesUtils() {
    }

    public static int compare(byte[] left, byte[] right) {
        return Arrays.compareUnsigned(left, right);
    }

    public static boolean arraysEquals(byte[] left, byte[] right) {
        return Arrays.equals(left, right);
    }

    public static int compare(
            byte[] left, int fromIndex, int toIndex,
            byte[] right, int fromIndex2, int toIndex2
    ) {
        return Arrays.compareUnsigned(left, fromIndex, toIndex,
                right, fromIndex2, toIndex2);
    }

    public static boolean arraysEquals(
            byte[] left, int fromIndex, int toIndex,
            byte[] right, int fromIndex2, int toIndex2
    ) {
        return Arrays.equals(left, fromIndex, toIndex,
                right, fromIndex2, toIndex2);
    }

    /**
     * 32-bit truncation of the golden-ratio constant
     * &phi;&nbsp;=&nbsp;(&radic;5&nbsp;&minus;&nbsp;1)&nbsp;/&nbsp;2 mapped onto a
     * 32-bit range and made odd. This is Knuth's recommended multiplier for
     * Fibonacci/multiplicative hashing: every increment of the input
     * advances the output by an amount whose representation visits the
     * full 32-bit space without clustering, which is exactly what the
     * downstream {@link java.util.concurrent.ConcurrentHashMap#spread spread}
     * step needs to keep bins balanced.
     */
    private static final int GOLDEN_RATIO_32 = 0x9E3779B9;

    /**
     * Length-mixing constant (any odd 32-bit non-multiple-of-{@code 31}
     * works); chosen to ensure that two arrays differing only in length
     * (e.g. the empty array vs. a one-zero-byte array) produce different
     * pre-mix accumulators.
     */
    private static final int LENGTH_MIX = 0x27D4EB2F;

    /** Native byte order, captured once. */
    private static final boolean BIG_ENDIAN_NATIVE_ORDER =
            ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    /** {@code true} if {@link PlatformDependent#getInt(byte[], int)} is safe to use. */
    private static final boolean HAS_FAST_INT_READ =
            PlatformDependent.hasUnsafe() && PlatformDependent.isUnaligned();

    /**
     * Pre-computed result of {@link #hashCode(byte[], int, int)} on the
     * empty input. Stored as a constant so the empty-input path on both
     * the {@code byte[]} and the {@link ByteBuf} overloads can return
     * without allocating a fresh empty array on every call.
     */
    public static final int EMPTY_HASH = finaliseHash(1, 0);

    /**
     * Mix the raw {@code 31x}-polynomial accumulator into a well-spread
     * 32-bit hash.
     *
     * <p>The naive {@code 31 * result + b} polynomial gives near-linear
     * output for inputs that vary only in the low-order bytes (e.g.
     * small consecutive integer primary keys serialised as 4 bytes),
     * which clusters {@link java.util.concurrent.ConcurrentHashMap} bins
     * enough to trip {@code TREEIFY_THRESHOLD} and force the bin into a
     * red-black tree. Treeified bins then call
     * {@code ConcurrentHashMap.comparableClassFor} on every put, which
     * walks generic-signature reflection metadata and dominates the
     * hot-path CPU profile (issue #429).
     *
     * <p>The mix below runs in three or four extra ALU ops &mdash;
     * trivial compared to the byte loop that produced {@code result}
     * &mdash; but produces a near-uniform output even for tightly
     * clustered inputs. Equality semantics are unchanged because both
     * sides of any {@code Bytes.equals} comparison run through the same
     * function.
     */
    private static int finaliseHash(int result, int length) {
        // Fold length into the accumulator so arrays differing only in
        // trailing zero-padding length still hash distinctly.
        result ^= length * LENGTH_MIX;
        // Knuth multiplicative finaliser using the golden-ratio constant.
        // Multiplying by an odd 32-bit number whose binary representation
        // has high entropy spreads any localized variation in `result`
        // across all 32 output bits.
        result *= GOLDEN_RATIO_32;
        // Final XOR-shift mixes the high bits down so the low bits used
        // for bin selection (CHM does `h & (n-1)`) are well-distributed.
        result ^= result >>> 16;
        return result;
    }

    /**
     * BE-canonical 4-byte int read. Uses {@link PlatformDependent}'s
     * {@code Unsafe}-backed accessor where available (with an endian
     * flip on little-endian platforms) and falls back to a manual
     * shift/OR otherwise. Returning a deterministic BE-interpreted int
     * is what allows the {@link ByteBuf} overload to feed the same
     * polynomial via {@link ByteBuf#getInt} on a default (BE) buffer.
     */
    private static int readBigEndianInt(byte[] a, int idx) {
        if (HAS_FAST_INT_READ) {
            int v = PlatformDependent.getInt(a, idx);
            return BIG_ENDIAN_NATIVE_ORDER ? v : Integer.reverseBytes(v);
        }
        return ((a[idx] & 0xff) << 24)
                | ((a[idx + 1] & 0xff) << 16)
                | ((a[idx + 2] & 0xff) << 8)
                | (a[idx + 3] & 0xff);
    }

    /**
     * Polynomial-then-mix hash for byte slices.
     *
     * <p>Implementation matches {@link io.netty.buffer.ByteBufUtil#hashCode(ByteBuf)}
     * — read 4 bytes at a time as a big-endian {@code int}, fold each
     * into the {@code 31*h + word} polynomial, then handle the
     * remaining 0&ndash;3 bytes one at a time. This is roughly 4&times;
     * fewer polynomial steps than a byte-by-byte loop, and lets the
     * off-heap overload read {@code ByteBuf.getInt} directly (a single
     * {@code Unsafe} access on a Direct buffer) instead of iterating
     * byte-by-byte.
     *
     * @see #finaliseHash(int, int) for the post-loop golden-ratio mix.
     */
    public static int hashCode(byte a[], int offset, int length) {
        if (a == null) {
            return 0;
        }
        if (length == 0) {
            return EMPTY_HASH;
        }
        int hashCode = 1;
        int idx = offset;
        final int intCount = length >>> 2;
        final int byteCount = length & 3;
        for (int i = 0; i < intCount; i++) {
            hashCode = 31 * hashCode + readBigEndianInt(a, idx);
            idx += 4;
        }
        for (int i = 0; i < byteCount; i++) {
            hashCode = 31 * hashCode + a[idx++];
        }
        return finaliseHash(hashCode, length);
    }

    /**
     * No-copy variant of {@link #hashCode(byte[], int, int)} that reads
     * directly from a {@link ByteBuf} slice using the same
     * int-block-then-byte-tail polynomial as the byte[] overload.
     *
     * <p>The previous implementation in {@code Bytes.hashCodeFromByteBuf}
     * allocated a temporary {@code byte[length]}, bulk-copied the slice
     * into it, and then called the byte-array overload. For an off-heap
     * {@link Bytes} that is used as a key in a
     * {@link java.util.concurrent.ConcurrentHashMap} this allocation
     * happened on every key-creation path. We avoid it here by:
     * <ul>
     *   <li>fast-pathing heap-backed {@link ByteBuf} instances (the
     *       common case for unpooled and pooled heap buffers) through
     *       the byte[] overload &mdash; same number of memory accesses,
     *       but the JIT-friendly tight array loop;</li>
     *   <li>iterating the truly-direct path with {@link ByteBuf#getInt}
     *       (matching {@link io.netty.buffer.ByteBufUtil#hashCode}),
     *       which inlines down to a single {@code Unsafe} access per
     *       4-byte word on an {@code AbstractByteBuf}.</li>
     * </ul>
     *
     * <p>The polynomial and the post-loop {@link #finaliseHash} are
     * shared with the byte[] overload, so the two paths produce
     * byte-for-byte identical hashes. This is the contract that
     * {@link Bytes#equals(Object)} relies on when comparing an on-heap
     * and an off-heap representation of the same content.
     *
     * @param buf    source buffer (or {@code null} which mirrors the
     *               {@code byte[] == null} short-circuit and returns 0)
     * @param offset absolute offset inside {@code buf} (i.e. the
     *               caller has already accounted for {@code readerIndex()}
     *               if appropriate)
     * @param length number of bytes to hash
     */
    public static int hashCode(ByteBuf buf, int offset, int length) {
        if (buf == null) {
            return 0;
        }
        if (length == 0) {
            return EMPTY_HASH;
        }
        if (buf.hasArray()) {
            return hashCode(buf.array(), buf.arrayOffset() + offset, length);
        }
        int hashCode = 1;
        int idx = offset;
        final int intCount = length >>> 2;
        final int byteCount = length & 3;
        // Canonicalise to BE so the polynomial result is independent of
        // the buffer's configured order (Netty's own ByteBufUtil.hashCode
        // uses the same trick).
        final boolean isBigEndian = buf.order() == ByteOrder.BIG_ENDIAN;
        for (int i = 0; i < intCount; i++) {
            int word = buf.getInt(idx);
            if (!isBigEndian) {
                word = Integer.reverseBytes(word);
            }
            hashCode = 31 * hashCode + word;
            idx += 4;
        }
        for (int i = 0; i < byteCount; i++) {
            hashCode = 31 * hashCode + buf.getByte(idx++);
        }
        return finaliseHash(hashCode, length);
    }
}
