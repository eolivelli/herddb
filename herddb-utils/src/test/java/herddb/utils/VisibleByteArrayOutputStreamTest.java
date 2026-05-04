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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.Test;

/**
 * @author enrico.olivelli
 */
public class VisibleByteArrayOutputStreamTest {

    @Test
    public void testMd5() throws Exception {
        byte[] content = "foo".getBytes(StandardCharsets.UTF_8);
        byte[] md5;
        try (VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(1000)) {
            oo.write(content);
            assertArrayEquals(content, oo.toByteArray());
            md5 = oo.xxhash64();
            System.out.println("hash:" + Arrays.toString(md5));
            System.out.println("content:" + Arrays.toString(content));
        }

        byte[] expected = XXHash64Utils.digest(content, 0, content.length);
        System.out.println("expected:" + Arrays.toString(expected));
        assertArrayEquals(expected, md5);
    }

    @Test
    public void testToByteArrayNoCopy() throws Exception {
        byte[] content = "foo".getBytes(StandardCharsets.UTF_8);
        byte[] content2 = "fooa".getBytes(StandardCharsets.UTF_8);
        try (VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(3)) {
            oo.write(content);
            assertArrayEquals(content, oo.toByteArray());
            assertNotSame(content, oo.toByteArray());
            // accessing directly the buffer
            assertSame(oo.getBuffer(), oo.toByteArrayNoCopy());

            oo.write('a');
            assertArrayEquals(content2, oo.toByteArray());
            assertNotSame(content, oo.toByteArray());
            assertNotSame(oo.getBuffer(), oo.toByteArrayNoCopy());

        }
    }

    /**
     * Issue #391: {@code stealBytes()} returns a {@link Bytes} view over the
     * just-written content, replaces the internal buffer so the caller has
     * exclusive ownership of the previous bytes, and resets the size counter.
     * Subsequent writes must not overwrite previously-stolen bytes.
     */
    @Test
    public void testStealBytes() throws Exception {
        byte[] content1 = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] content2 = "world!".getBytes(StandardCharsets.UTF_8);
        try (VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(1024)) {
            // Buffer over-allocated relative to content1: takes the trim path.
            oo.write(content1);
            byte[] internalBuffer1 = oo.getBuffer();
            Bytes stolen1 = oo.stealBytes();
            assertEquals(content1.length, stolen1.getLength());
            assertEquals(0, stolen1.getOffset());
            assertArrayEquals(content1, stolen1.to_array());
            // The Bytes view aliases the previous internal buffer (zero-copy).
            assertSame(internalBuffer1, stolen1.getBuffer());
            // The stream now has a fresh buffer and an empty size.
            assertEquals(0, oo.size());
            assertNotSame(internalBuffer1, oo.getBuffer());

            // Writing again must NOT mutate stolen1's bytes.
            oo.write(content2);
            byte[] internalBuffer2 = oo.getBuffer();
            assertNotSame(internalBuffer1, internalBuffer2);
            // Re-check stolen1 unchanged.
            assertArrayEquals(content1, stolen1.to_array());

            Bytes stolen2 = oo.stealBytes();
            assertArrayEquals(content2, stolen2.to_array());
            // Both stolen views must remain independent.
            assertArrayEquals(content1, stolen1.to_array());
        }
    }

    /**
     * Issue #391: empty stream returns an empty {@code Bytes}, and same-sized
     * subsequent records hit the steady-state zero-copy path (the replacement
     * buffer is sized exactly to the previous {@code count}, so the next write
     * fills it without growing).
     */
    @Test
    public void testStealBytesEmptyAndSteadyState() throws Exception {
        try (VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(1024)) {
            // Empty stream: stealBytes returns a 0-length Bytes view and the
            // stream is reset to a healthy minimum buffer size.
            Bytes empty = oo.stealBytes();
            assertEquals(0, empty.getLength());
            assertTrue("replacement buffer must have a minimum capacity",
                    oo.getBuffer().length >= 32);

            // First "real" record (over-allocated buffer → trim path).
            byte[] record = new byte[515];
            Arrays.fill(record, (byte) 0x42);
            oo.write(record);
            Bytes first = oo.stealBytes();
            assertEquals(record.length, first.getLength());
            assertArrayEquals(record, first.to_array());
            // The replacement buffer is now sized to the just-served call,
            // so the next same-sized write must not grow.
            assertEquals(record.length, oo.getBuffer().length);

            // Second same-sized record: steady-state. The internal buffer
            // matches the record exactly, so stealBytes hands back the buffer
            // without trimming.
            byte[] internalBefore = oo.getBuffer();
            oo.write(record);
            Bytes second = oo.stealBytes();
            assertArrayEquals(record, second.to_array());
            assertSame("steady-state should be zero-copy",
                    internalBefore, second.getBuffer());

            // first and second remain independent.
            assertArrayEquals(record, first.to_array());
            assertArrayEquals(record, second.to_array());
        }
    }

    /**
     * Issue #391: calling {@code stealBytes()} twice in a row — once on a
     * stream with content, then again on the now-empty stream — must yield
     * (a) the originally-written content, and (b) a fresh 0-length view
     * over the just-installed replacement buffer (NOT the previously-stolen
     * one). This pins down the contract that consecutive steals never alias
     * each other's backing arrays.
     */
    @Test
    public void testStealBytesIdempotentOnEmptyStream() throws Exception {
        try (VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(64)) {
            byte[] payload = "abcd".getBytes(StandardCharsets.UTF_8);
            oo.write(payload);
            Bytes firstSteal = oo.stealBytes();
            byte[] firstStealBuffer = firstSteal.getBuffer();
            assertArrayEquals(payload, firstSteal.to_array());

            // Stream is now empty; a second steal must return a 0-length
            // Bytes that does NOT alias the previously-stolen buffer.
            byte[] replacementBufferBefore = oo.getBuffer();
            Bytes secondSteal = oo.stealBytes();
            assertEquals(0, secondSteal.getLength());
            assertNotSame("second steal must not alias the first",
                    firstStealBuffer, secondSteal.getBuffer());
            assertSame("second steal aliases the just-installed replacement buffer",
                    replacementBufferBefore, secondSteal.getBuffer());

            // First-steal content remains intact.
            assertArrayEquals(payload, firstSteal.to_array());
        }
    }

}
