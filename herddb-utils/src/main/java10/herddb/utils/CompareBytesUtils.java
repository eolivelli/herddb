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
 * Java 9+ compatible version. Uses {@code Arrays.compareUnsigned} and
 * {@code Arrays.equals(byte[], int, int, byte[], int, int)} which leverage
 * HotSpot intrinsics.
 *
 * <p>The {@link #hashCode(byte[], int, int)} and
 * {@link #hashCode(ByteBuf, int, int)} methods are kept byte-for-byte
 * compatible with the Java 8 base implementation in
 * {@code src/main/java/herddb/utils/CompareBytesUtils.java}; both versions
 * <em>must</em> evolve together because {@link Bytes#hashCode()} caches
 * the result and {@link Bytes#equals(Object)} uses the cached value as a
 * fast-path filter, so a mismatch between the base and the
 * {@code META-INF/versions/10/} variant would silently break equality
 * across artifacts running on different JDK versions.
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
     * 32-bit truncation of the golden-ratio constant. See the Java 8 base
     * version for the rationale; this constant must match the base
     * version exactly.
     */
    private static final int GOLDEN_RATIO_32 = 0x9E3779B9;

    /** Length-mixing constant; must match the base version. */
    private static final int LENGTH_MIX = 0x27D4EB2F;

    /** Native byte order, captured once. */
    private static final boolean BIG_ENDIAN_NATIVE_ORDER =
            ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    /** {@code true} if {@link PlatformDependent#getInt(byte[], int)} is safe to use. */
    private static final boolean HAS_FAST_INT_READ =
            PlatformDependent.hasUnsafe() && PlatformDependent.isUnaligned();

    /**
     * Pre-computed result of {@link #hashCode(byte[], int, int)} on the
     * empty input. Keeping both overloads' empty-input fast path
     * allocation-free.
     */
    public static final int EMPTY_HASH = finaliseHash(1, 0);

    /**
     * Mix the raw {@code 31x}-polynomial accumulator into a well-spread
     * 32-bit hash. See the Java 8 base version for the full rationale.
     */
    private static int finaliseHash(int result, int length) {
        result ^= length * LENGTH_MIX;
        result *= GOLDEN_RATIO_32;
        result ^= result >>> 16;
        return result;
    }

    /**
     * BE-canonical 4-byte int read; returns the same value on
     * little-endian and big-endian platforms so the polynomial result is
     * byte-order-independent. Mirrors the Java 8 base version.
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
     * Polynomial-then-mix hash for byte slices. See the Java 8 base
     * version for the full rationale; the algorithm here must match the
     * base version byte-for-byte.
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
     * directly from a {@link ByteBuf} slice. See the Java 8 base version
     * for the full rationale; the algorithm here must match the base
     * version byte-for-byte.
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
