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

import static org.junit.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

/**
 * Verifies that {@link ByteBufUtils#varIntLen} and {@link ByteBufUtils#varLongLen}
 * return the exact same byte count that {@link ByteBufUtils#writeVInt} /
 * {@link ByteBufUtils#writeVLong} actually emit into a {@link ByteBuf}.
 *
 * <p>Covers every byte-count transition plus boundary values at each threshold.
 */
public class ByteBufUtilsVarLenTest {

    // -------------------------------------------------------------------------
    // varIntLen
    // -------------------------------------------------------------------------

    /** Values at or near each VInt byte-count boundary. */
    private static final int[] VAR_INT_CASES = {
        0, 1, 0x7E, 0x7F,           // 1 byte
        0x80, 0x81, 0x3FFE, 0x3FFF, // 2 bytes
        0x4000, 0x1FFFFE, 0x1FFFFF, // 3 bytes
        0x200000, 0xFFFFFFE, 0xFFFFFFF, // 4 bytes
        0x10000000, Integer.MAX_VALUE,  // 5 bytes (positive)
        -1, -2, Integer.MIN_VALUE,       // 5 bytes (negative VInt)
    };

    @Test
    public void testVarIntLenMatchesWriteVInt() {
        ByteBuf buf = Unpooled.buffer(32);
        try {
            for (int i : VAR_INT_CASES) {
                buf.clear();
                ByteBufUtils.writeVInt(buf, i);
                int actual = buf.writerIndex();
                int predicted = ByteBufUtils.varIntLen(i);
                assertEquals("varIntLen(" + i + ")", actual, predicted);
            }
        } finally {
            buf.release();
        }
    }

    // -------------------------------------------------------------------------
    // varLongLen
    // -------------------------------------------------------------------------

    /** Values at or near each VLong byte-count boundary (non-negative only,
     *  because writeVLong rejects negative values). */
    private static final long[] VAR_LONG_CASES = {
        0L, 1L, 0x7EL, 0x7FL,                       // 1 byte
        0x80L, 0x3FFFL,                               // 2 bytes
        0x4000L, 0x1FFFFFL,                           // 3 bytes
        0x200000L, 0xFFFFFFFL,                        // 4 bytes
        0x10000000L, 0x7FFFFFFFFL,                    // 5 bytes
        0x800000000L, 0x3FFFFFFFFFFL,                 // 6 bytes
        0x40000000000L, 0x1FFFFFFFFFFFFL,             // 7 bytes
        0x2000000000000L, 0xFFFFFFFFFFFFFFL,          // 8 bytes
        0x100000000000000L, Long.MAX_VALUE,           // 9 bytes
    };

    @Test
    public void testVarLongLenMatchesWriteVLong() {
        ByteBuf buf = Unpooled.buffer(16);
        try {
            for (long v : VAR_LONG_CASES) {
                buf.clear();
                ByteBufUtils.writeVLong(buf, v);
                int actual = buf.writerIndex();
                int predicted = ByteBufUtils.varLongLen(v);
                assertEquals("varLongLen(" + v + ")", actual, predicted);
            }
        } finally {
            buf.release();
        }
    }
}
