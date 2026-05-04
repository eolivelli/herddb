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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.Test;

/**
 * Test on {@link Bytes} facility
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class BytesTest {

    public BytesTest() {
    }

    @Test
    public void testNext() {
        byte[] array1 = "test".getBytes(StandardCharsets.UTF_8);
        Bytes bytes1 = Bytes.from_array(array1);
        Bytes next = bytes1.next();
        byte[] array2 = "tesu".getBytes(StandardCharsets.UTF_8);
        System.out.println("a:" + bytes1.to_string());
        System.out.println("b:" + next.to_string());
        assertArrayEquals(array2, next.to_array());
    }

    @Test
    public void testNextSameByte() {

        byte[] array = new byte[]{0, 1};
        byte[] nextExpected = new byte[]{0, 2};

        Bytes bytes = Bytes.from_array(array);
        Bytes next = bytes.next();

        assertArrayEquals(nextExpected, next.to_array());
    }

    /**
     * Check that the change propagate to next byte if 255 (-1)
     */
    @Test
    public void testNextChangeByte() {

        byte[] array = new byte[]{0, -1};
        byte[] nextExpected = new byte[]{1, 0};

        Bytes bytes = Bytes.from_array(array);
        Bytes next = bytes.next();

        assertArrayEquals(nextExpected, next.to_array());
    }

    /**
     * Check that more than one byte is changed if needed
     */
    @Test
    public void testNextChangeByteMoreTimes() {

        byte[] array = new byte[]{0, -1, -1};
        byte[] nextExpected = new byte[]{1, 0, 0};

        Bytes bytes = Bytes.from_array(array);
        Bytes next = bytes.next();

        assertArrayEquals(nextExpected, next.to_array());
    }

    /**
     * Checks that prefix bytes aren't touched
     */
    @Test
    public void testNextChangeByteMoreTimesWithPrefix() {

        byte[] array = new byte[]{1, 0, -1, -1};
        byte[] nextExpected = new byte[]{1, 1, 0, 0};

        Bytes bytes = Bytes.from_array(array);
        Bytes next = bytes.next();

        assertArrayEquals(nextExpected, next.to_array());
    }

    /**
     * Checks that next fails if there is no more space
     */
    @Test(expected = IllegalStateException.class)
    public void testNextNoMoreSpace() {

        byte[] array = new byte[]{-1, -1};

        Bytes bytes = Bytes.from_array(array);
        bytes.next();
    }

    /**
     * Check that leading zeros are preserved
     */
    @Test
    public void testNextLenPreservation() {

        byte[] src = new byte[]{0, 0, 0, -1};

        Bytes bytes = Bytes.from_array(src);

        assertArrayEquals(src, bytes.to_array());

        Bytes next = bytes.next();

        assertEquals(bytes.getLength(), next.getLength());

    }

    @Test
    public void testIntRoundTrip() {
        for (int v : new int[] {Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -1_000_000, -1, 0, 1, 1_000_000,
                Integer.MAX_VALUE - 1, Integer.MAX_VALUE}) {
            assertEquals(v, Bytes.toInt(Bytes.intToByteArray(v), 0));
            assertEquals(v, Bytes.from_int(v).to_int());
        }
    }

    @Test
    public void testLongRoundTrip() {
        for (long v : new long[] {Long.MIN_VALUE, Long.MIN_VALUE + 1, -1_000_000_000_000L, -1L, 0L, 1L,
                1_000_000_000_000L, Long.MAX_VALUE - 1, Long.MAX_VALUE}) {
            assertEquals(v, Bytes.toLong(Bytes.longToByteArray(v), 0));
            assertEquals(v, Bytes.from_long(v).to_long());
        }
    }

    /**
     * After {@link Bytes#intToByteArray(int)} the unsigned-lex byte order matches signed numeric
     * order, so the BLink primary-key index naturally sorts INTEGER PKs.
     */
    @Test
    public void testIntOrderPreservation() {
        int[] curated = {Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -1_000_000, -2, -1, 0, 1, 2, 1_000_000,
                Integer.MAX_VALUE - 1, Integer.MAX_VALUE};
        for (int i = 0; i < curated.length; i++) {
            for (int j = 0; j < curated.length; j++) {
                assertOrderMatches(curated[i], curated[j]);
            }
        }
        Random rng = new Random(0xC0FFEE);
        for (int n = 0; n < 1000; n++) {
            assertOrderMatches(rng.nextInt(), rng.nextInt());
        }
    }

    /**
     * After {@link Bytes#longToByteArray(long)} unsigned-lex byte order matches signed numeric order.
     */
    @Test
    public void testLongOrderPreservation() {
        long[] curated = {Long.MIN_VALUE, Long.MIN_VALUE + 1, -1_000_000_000_000L, -2L, -1L, 0L, 1L, 2L,
                1_000_000_000_000L, Long.MAX_VALUE - 1, Long.MAX_VALUE};
        for (int i = 0; i < curated.length; i++) {
            for (int j = 0; j < curated.length; j++) {
                assertOrderMatches(curated[i], curated[j]);
            }
        }
        Random rng = new Random(0xCAFE);
        for (int n = 0; n < 1000; n++) {
            assertOrderMatches(rng.nextLong(), rng.nextLong());
        }
    }

    /**
     * Doubles must keep the IEEE-754 bit pattern intact (not order-preserving — that's by design,
     * see {@link Bytes#putDouble(byte[], int, double)} delegating to putRawLong).
     */
    @Test
    public void testDoubleRoundTripStaysRaw() {
        for (double v : new double[] {Double.NEGATIVE_INFINITY, -Math.PI, -1.0, -0.0, 0.0, 1.0, Math.PI,
                Double.POSITIVE_INFINITY, Double.MAX_VALUE, Double.MIN_VALUE}) {
            byte[] bytes = Bytes.doubleToByteArray(v);
            assertEquals(8, bytes.length);
            assertEquals(Double.doubleToRawLongBits(v), Bytes.toRawLong(bytes, 0));
            assertEquals(Double.compare(v, v), Double.compare(Bytes.toDouble(bytes, 0), v));
        }
    }

    /**
     * Floats must keep the IEEE-754 bit pattern intact too.
     */
    @Test
    public void testFloatArrayRoundTripStaysRaw() {
        float[] in = {Float.NEGATIVE_INFINITY, -1.0f, -0.0f, 0.0f, 1.0f, Float.POSITIVE_INFINITY};
        float[] out = Bytes.from_float_array(in).to_float_array();
        assertEquals(in.length, out.length);
        for (int i = 0; i < in.length; i++) {
            assertEquals(Float.floatToRawIntBits(in[i]), Float.floatToRawIntBits(out[i]));
        }
    }

    /**
     * Issue #391: the {@code startsWith(Bytes)} overload preserves the exact
     * semantics of the pre-existing {@code startsWith(int, byte[])} method
     * (compare up to {@code prefix.length} bytes; loop terminates when either
     * side is exhausted). The new overload must additionally respect the
     * {@link Bytes} prefix's {@code offset+length} so a shared view of a
     * larger backing buffer is compared correctly without leaking the slack
     * bytes.
     */
    @Test
    public void testStartsWithBytesOverload() {
        Bytes value = Bytes.from_array("foobar".getBytes(StandardCharsets.UTF_8));

        // Matching prefix.
        assertTrue(value.startsWith(Bytes.from_array("foo".getBytes(StandardCharsets.UTF_8))));
        // Full equality counts as a "startsWith".
        assertTrue(value.startsWith(Bytes.from_array("foobar".getBytes(StandardCharsets.UTF_8))));
        // Empty prefix matches everything.
        assertTrue(value.startsWith(Bytes.from_array(new byte[0])));
        // Mismatched bytes.
        assertFalse(value.startsWith(Bytes.from_array("bar".getBytes(StandardCharsets.UTF_8))));
        // Mismatch detected before either side runs out.
        assertFalse(value.startsWith(Bytes.from_array("foox".getBytes(StandardCharsets.UTF_8))));

        // Equivalence with the pre-existing byte[] overload: identical
        // truth-value for every input we can construct from the same arrays.
        byte[] receiver = "foobar".getBytes(StandardCharsets.UTF_8);
        byte[] shortPrefix = "foo".getBytes(StandardCharsets.UTF_8);
        Bytes legacyView = Bytes.from_array(receiver);
        assertEquals(legacyView.startsWith(shortPrefix.length, shortPrefix),
                     legacyView.startsWith(Bytes.from_array(shortPrefix)));

        // Legacy semantics that prefix-scan callers rely on: when the receiver
        // is shorter than (or equal in size to) the prefix, the loop short-
        // circuits when the receiver is exhausted and returns true if every
        // byte compared so far matched. This is the existing contract of
        // startsWith(int, byte[]) — pinned here so a future refactor cannot
        // silently change prefix-scan behaviour in MemoryHashIndexManager
        // and ConcurrentMapKeyToPageIndex.
        Bytes shortReceiver = Bytes.from_array("foo".getBytes(StandardCharsets.UTF_8));
        Bytes longerPrefix = Bytes.from_array("foobar".getBytes(StandardCharsets.UTF_8));
        assertTrue("legacy semantics: receiver shorter than prefix returns true when "
                + "every available byte matches",
                shortReceiver.startsWith(longerPrefix));

        // Shared prefix view (extra slack at the tail, only the first 3 bytes
        // are in scope). Must compare against the in-scope window only.
        byte[] sharedBuffer = "fooXXXX".getBytes(StandardCharsets.UTF_8);
        Bytes sharedPrefix = Bytes.from_array(sharedBuffer, 0, 3);
        assertTrue(value.startsWith(sharedPrefix));

        // Shared prefix with non-zero offset.
        byte[] withLeadingSlack = "ZZfooZZ".getBytes(StandardCharsets.UTF_8);
        Bytes offsetPrefix = Bytes.from_array(withLeadingSlack, 2, 3);
        assertTrue(value.startsWith(offsetPrefix));

        // Negative case across an offset prefix: "bar" buffered with leading
        // slack must not match "foobar".
        byte[] withLeadingSlack2 = "ZZbarZZ".getBytes(StandardCharsets.UTF_8);
        Bytes offsetMismatch = Bytes.from_array(withLeadingSlack2, 2, 3);
        assertFalse(value.startsWith(offsetMismatch));

        // Shared *value* view (the receiver), to confirm both sides handle
        // offset+length correctly.
        byte[] valueWithSlack = "ZZZfoobarZZ".getBytes(StandardCharsets.UTF_8);
        Bytes sharedValue = Bytes.from_array(valueWithSlack, 3, 6);
        assertTrue(sharedValue.startsWith(Bytes.from_array("foo".getBytes(StandardCharsets.UTF_8))));
        assertFalse(sharedValue.startsWith(Bytes.from_array("bar".getBytes(StandardCharsets.UTF_8))));
    }

    private static void assertOrderMatches(int a, int b) {
        int signed = Integer.compare(a, b);
        int lex = CompareBytesUtils.compare(Bytes.intToByteArray(a), Bytes.intToByteArray(b));
        assertTrue("a=" + a + " b=" + b + " signed=" + signed + " lex=" + lex,
                Integer.signum(signed) == Integer.signum(lex));
    }

    private static void assertOrderMatches(long a, long b) {
        int signed = Long.compare(a, b);
        int lex = CompareBytesUtils.compare(Bytes.longToByteArray(a), Bytes.longToByteArray(b));
        assertTrue("a=" + a + " b=" + b + " signed=" + signed + " lex=" + lex,
                Integer.signum(signed) == Integer.signum(lex));
    }

}
