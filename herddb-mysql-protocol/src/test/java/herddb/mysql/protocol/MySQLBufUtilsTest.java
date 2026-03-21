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

package herddb.mysql.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests for {@link MySQLBufUtils}.
 */
public class MySQLBufUtilsTest {

    @Test
    public void testLengthEncodedIntSmall() {
        ByteBuf buf = Unpooled.buffer();
        try {
            MySQLBufUtils.writeLengthEncodedInt(buf, 42);
            assertEquals(42, MySQLBufUtils.readLengthEncodedInt(buf));
        } finally {
            buf.release();
        }
    }

    @Test
    public void testLengthEncodedIntTwoBytes() {
        ByteBuf buf = Unpooled.buffer();
        try {
            MySQLBufUtils.writeLengthEncodedInt(buf, 300);
            assertEquals(300, MySQLBufUtils.readLengthEncodedInt(buf));
        } finally {
            buf.release();
        }
    }

    @Test
    public void testLengthEncodedIntThreeBytes() {
        ByteBuf buf = Unpooled.buffer();
        try {
            long value = 70000L;
            MySQLBufUtils.writeLengthEncodedInt(buf, value);
            assertEquals(value, MySQLBufUtils.readLengthEncodedInt(buf));
        } finally {
            buf.release();
        }
    }

    @Test
    public void testLengthEncodedIntEightBytes() {
        ByteBuf buf = Unpooled.buffer();
        try {
            long value = 0x1000000L + 1;
            MySQLBufUtils.writeLengthEncodedInt(buf, value);
            assertEquals(value, MySQLBufUtils.readLengthEncodedInt(buf));
        } finally {
            buf.release();
        }
    }

    @Test
    public void testLengthEncodedString() {
        ByteBuf buf = Unpooled.buffer();
        try {
            String original = "Hello, MySQL!";
            MySQLBufUtils.writeLengthEncodedString(buf, original, StandardCharsets.UTF_8);
            String decoded = MySQLBufUtils.readLengthEncodedString(buf, StandardCharsets.UTF_8);
            assertEquals(original, decoded);
        } finally {
            buf.release();
        }
    }

    @Test
    public void testNullTerminatedString() {
        ByteBuf buf = Unpooled.buffer();
        try {
            String original = "testdb";
            MySQLBufUtils.writeNullTerminatedString(buf, original, StandardCharsets.UTF_8);
            String decoded = MySQLBufUtils.readNullTerminatedString(buf, StandardCharsets.UTF_8);
            assertEquals(original, decoded);
        } finally {
            buf.release();
        }
    }

    @Test
    public void testFixedLengthInt() {
        ByteBuf buf = Unpooled.buffer();
        try {
            MySQLBufUtils.writeFixedLengthInt(buf, 0x010203, 3);
            assertEquals(0x010203, MySQLBufUtils.readFixedLengthInt(buf, 3));
        } finally {
            buf.release();
        }
    }

    @Test
    public void testFixedLengthIntTwoBytes() {
        ByteBuf buf = Unpooled.buffer();
        try {
            MySQLBufUtils.writeFixedLengthInt(buf, 0xABCD, 2);
            assertEquals(0xABCD, MySQLBufUtils.readFixedLengthInt(buf, 2));
        } finally {
            buf.release();
        }
    }

    @Test
    public void testLengthEncodedIntBoundary250() {
        ByteBuf buf = Unpooled.buffer();
        try {
            MySQLBufUtils.writeLengthEncodedInt(buf, 250);
            assertEquals(1, buf.readableBytes()); // single byte
            assertEquals(250, MySQLBufUtils.readLengthEncodedInt(buf));
        } finally {
            buf.release();
        }
    }

    @Test
    public void testLengthEncodedIntBoundary251() {
        ByteBuf buf = Unpooled.buffer();
        try {
            MySQLBufUtils.writeLengthEncodedInt(buf, 251);
            assertEquals(3, buf.readableBytes()); // 0xFC + 2 bytes
            assertEquals(251, MySQLBufUtils.readLengthEncodedInt(buf));
        } finally {
            buf.release();
        }
    }

    @Test
    public void testLengthEncodedEmptyString() {
        ByteBuf buf = Unpooled.buffer();
        try {
            MySQLBufUtils.writeLengthEncodedString(buf, "", StandardCharsets.UTF_8);
            String decoded = MySQLBufUtils.readLengthEncodedString(buf, StandardCharsets.UTF_8);
            assertEquals("", decoded);
        } finally {
            buf.release();
        }
    }
}
