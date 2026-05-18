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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import herddb.proto.Pdu;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Regression tests for issue #582:
 * {@link RemoteFileServiceClient#parseReadFileRangeResponse} and
 * {@link RemoteFileServiceClient#parseReadFileResponse} must release the
 * allocated direct {@code ByteBuf buf} when the PDU-content read throws
 * after the allocation but before returning the buffer to the caller.
 *
 * <h3>How the leak is triggered</h3>
 * Both parsers follow the pattern:
 * <pre>
 *   ByteBuf buf = allocator.directBuffer(len);   // ← allocation
 *   ByteBuf slice = PduCodec.Xxx.readContent(pdu); // ← may throw IOOBE
 *   buf.writeBytes(slice);
 *   return buf;
 * </pre>
 * When the server returns a truncated response ({@code contentLength=1000}
 * but fewer actual bytes), {@code readContent} executes
 * {@code buffer.retainedSlice(offset, 1000)} on a short buffer and throws
 * {@link IndexOutOfBoundsException}. Before the fix, {@code buf} was leaked
 * (refCnt=1, never decremented). The fix wraps the body with a
 * {@code boolean success + try/finally} guard that calls {@code buf.release()}
 * whenever an exception propagates, irrespective of its type.
 *
 * <h3>Test strategy — deterministic direct unit tests</h3>
 * The tests call the package-private static parser methods directly, passing
 * a fresh {@link PooledByteBufAllocator} that is exclusively used by the test.
 * After the parser throws, {@link PoolArenaMetric#numActiveAllocations()} on
 * that fresh allocator must be exactly {@code 0} — a strict equality that is
 * sound because no background thread can interact with the fresh allocator.
 *
 * <p>This avoids the timing-race that made the previous metric-delta approach
 * unreliable: the old {@code assertTrue(after - before < N)} check could
 * produce a false-negative when {@code N} genuine leaks coincided with a
 * near-equal count of background releases from other allocators.
 */
public class RemoteFileServiceClientReadFileRangeByteBufLeakTest {

    /** Per-test timeout guard — each test should complete in well under 1 s. */
    @Rule
    public Timeout testTimeout = Timeout.seconds(30);

    // -------------------------------------------------------------------------
    // parseReadFileRangeResponse — issue #582 primary fix
    // -------------------------------------------------------------------------

    /**
     * Verifies that {@code parseReadFileRangeResponse} releases its allocated
     * {@code ByteBuf} when {@link IndexOutOfBoundsException} is thrown by
     * {@code PduCodec.ReadFileRangeResponse.readContent(pdu)} on a truncated
     * response (reported path in issue #582).
     *
     * <p>The PDU claims {@code contentLength=1000} but only carries 5 bytes of
     * payload. {@code readContent} calls {@code retainedSlice(16, 1000)} on
     * a 21-byte buffer, which immediately throws. Before the fix, the
     * {@code directBuffer(1000)} allocation was leaked.
     *
     * <p>Repeated {@code ITERATIONS} times: with the bug, each iteration leaves
     * one un-released buffer; with the fix the count always returns to 0.
     */
    @Test
    public void testParseReadFileRangeResponse_releasesBufOnParseError() {
        PooledByteBufAllocator freshAllocator = new PooledByteBufAllocator(true);

        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            // Build a malformed ReadFileRange response PDU:
            // VERSION(1)+FLAGS(1)+TYPE(1)+MSGID(8)+found(1)+contentLen(4)+data(5) = 21 bytes.
            // contentLength claims 1000 but only 5 bytes follow → retainedSlice(16,1000) throws.
            ByteBuf wire = PooledByteBufAllocator.DEFAULT.directBuffer(21);
            wire.writeByte(3);                             // VERSION_3
            wire.writeByte(Pdu.FLAGS_ISRESPONSE);
            wire.writeByte(Pdu.TYPE_FS_READ_FILE_RANGE);
            wire.writeLong(42L);                           // message id
            wire.writeByte(1);                             // found = true
            wire.writeInt(1000);                           // contentLength = 1000 (lie)
            wire.writeBytes(new byte[5]);                  // truncated: only 5 bytes

            Pdu pdu = Pdu.newPdu(wire, Pdu.TYPE_FS_READ_FILE_RANGE, Pdu.FLAGS_ISRESPONSE, 42L);
            try {
                RemoteFileServiceClient.parseReadFileRangeResponse(pdu, freshAllocator);
                fail("Expected IndexOutOfBoundsException from retainedSlice on truncated buffer");
            } catch (IndexOutOfBoundsException e) {
                // Expected: retainedSlice(16, 1000) on a 21-byte buffer throws immediately.
            } finally {
                pdu.close(); // releases the 'wire' ByteBuf (allocated from DEFAULT, not freshAllocator)
            }

            // KEY ASSERTION: the directBuffer(1000) from freshAllocator must have
            // been released by the fix's finally block. With the bug it would be
            // leaked (refCnt=1), raising numActiveAllocations by 1 each iteration.
            long active = activeDirectAllocs(freshAllocator);
            assertEquals(
                    "iteration " + (i + 1) + ": directBuffer(1000) was not released "
                            + "by parseReadFileRangeResponse on IndexOutOfBoundsException "
                            + "(issue #582 regression)",
                    0L, active);
        }
    }

    /**
     * Verifies that {@code parseReadFileRangeResponse} returns {@code null}
     * (without allocating any buffer) when {@code found == 0}.
     */
    @Test
    public void testParseReadFileRangeResponse_notFound_returnsNull() {
        PooledByteBufAllocator freshAllocator = new PooledByteBufAllocator(true);

        ByteBuf wire = PooledByteBufAllocator.DEFAULT.directBuffer(16);
        wire.writeByte(3);                             // VERSION_3
        wire.writeByte(Pdu.FLAGS_ISRESPONSE);
        wire.writeByte(Pdu.TYPE_FS_READ_FILE_RANGE);
        wire.writeLong(1L);                            // message id
        wire.writeByte(0);                             // found = false
        wire.writeInt(0);                              // contentLength irrelevant

        Pdu pdu = Pdu.newPdu(wire, Pdu.TYPE_FS_READ_FILE_RANGE, Pdu.FLAGS_ISRESPONSE, 1L);
        try {
            ByteBuf result = RemoteFileServiceClient.parseReadFileRangeResponse(pdu, freshAllocator);
            assertNull("not-found response must return null", result);
            assertEquals("no allocation must occur for not-found response",
                    0L, activeDirectAllocs(freshAllocator));
        } finally {
            pdu.close();
        }
    }

    // -------------------------------------------------------------------------
    // parseReadFileResponse — proactive fix for the same latent bug
    // -------------------------------------------------------------------------

    /**
     * Verifies that {@code parseReadFileResponse} releases its allocated
     * {@code ByteBuf} when {@link IndexOutOfBoundsException} is thrown by
     * {@code PduCodec.ReadFileResponse.readContent(pdu)} on a truncated
     * response (same latent bug as issue #582, fixed proactively).
     *
     * <p>Same assertion strategy as
     * {@link #testParseReadFileRangeResponse_releasesBufOnParseError()}.
     */
    @Test
    public void testParseReadFileResponse_releasesBufOnParseError() {
        PooledByteBufAllocator freshAllocator = new PooledByteBufAllocator(true);

        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            // Build a malformed ReadFile response PDU:
            // VERSION(1)+FLAGS(1)+TYPE(1)+MSGID(8)+found(1)+contentLen(4)+data(5) = 21 bytes.
            // contentLength claims 1000 but only 5 bytes follow → retainedSlice(16,1000) throws.
            ByteBuf wire = PooledByteBufAllocator.DEFAULT.directBuffer(21);
            wire.writeByte(3);                         // VERSION_3
            wire.writeByte(Pdu.FLAGS_ISRESPONSE);
            wire.writeByte(Pdu.TYPE_FS_READ_FILE);
            wire.writeLong(99L);                       // message id
            wire.writeByte(1);                         // found = true
            wire.writeInt(1000);                       // contentLength = 1000 (lie)
            wire.writeBytes(new byte[5]);              // truncated: only 5 bytes

            Pdu pdu = Pdu.newPdu(wire, Pdu.TYPE_FS_READ_FILE, Pdu.FLAGS_ISRESPONSE, 99L);
            try {
                RemoteFileServiceClient.parseReadFileResponse(pdu, freshAllocator);
                fail("Expected IndexOutOfBoundsException from retainedSlice on truncated buffer");
            } catch (IndexOutOfBoundsException e) {
                // Expected: retainedSlice(16, 1000) on a 21-byte buffer throws immediately.
            } finally {
                pdu.close();
            }

            long active = activeDirectAllocs(freshAllocator);
            assertEquals(
                    "iteration " + (i + 1) + ": directBuffer(1000) was not released "
                            + "by parseReadFileResponse on IndexOutOfBoundsException "
                            + "(issue #582 regression — proactive fix)",
                    0L, active);
        }
    }

    /**
     * Verifies that {@code parseReadFileResponse} returns {@code null} (without
     * allocating) when {@code found == 0}.
     */
    @Test
    public void testParseReadFileResponse_notFound_returnsNull() {
        PooledByteBufAllocator freshAllocator = new PooledByteBufAllocator(true);

        ByteBuf wire = PooledByteBufAllocator.DEFAULT.directBuffer(16);
        wire.writeByte(3);
        wire.writeByte(Pdu.FLAGS_ISRESPONSE);
        wire.writeByte(Pdu.TYPE_FS_READ_FILE);
        wire.writeLong(2L);
        wire.writeByte(0);                             // found = false
        wire.writeInt(0);

        Pdu pdu = Pdu.newPdu(wire, Pdu.TYPE_FS_READ_FILE, Pdu.FLAGS_ISRESPONSE, 2L);
        try {
            ByteBuf result = RemoteFileServiceClient.parseReadFileResponse(pdu, freshAllocator);
            assertNull("not-found response must return null", result);
            assertEquals("no allocation must occur for not-found response",
                    0L, activeDirectAllocs(freshAllocator));
        } finally {
            pdu.close();
        }
    }

    /**
     * Verifies that {@code parseReadFileRangeResponse} returns a valid
     * {@code ByteBuf} containing the expected content when the PDU is
     * well-formed ({@code contentLength} matches the actual payload length).
     */
    @Test
    public void testParseReadFileRangeResponse_validResponse_returnsBuf() {
        PooledByteBufAllocator freshAllocator = new PooledByteBufAllocator(true);

        byte[] content = "hello world".getBytes();
        // VERSION(1)+FLAGS(1)+TYPE(1)+MSGID(8)+found(1)+contentLen(4)+content(N)
        ByteBuf wire = PooledByteBufAllocator.DEFAULT.directBuffer(16 + content.length);
        wire.writeByte(3);
        wire.writeByte(Pdu.FLAGS_ISRESPONSE);
        wire.writeByte(Pdu.TYPE_FS_READ_FILE_RANGE);
        wire.writeLong(7L);
        wire.writeByte(1);                             // found = true
        wire.writeInt(content.length);                 // accurate contentLength
        wire.writeBytes(content);

        Pdu pdu = Pdu.newPdu(wire, Pdu.TYPE_FS_READ_FILE_RANGE, Pdu.FLAGS_ISRESPONSE, 7L);
        ByteBuf result = null;
        try {
            result = RemoteFileServiceClient.parseReadFileRangeResponse(pdu, freshAllocator);
            assertNotNull("well-formed response must return a non-null ByteBuf", result);
            assertEquals("returned ByteBuf must contain exactly the payload",
                    content.length, result.readableBytes());
            // One active allocation: the returned buf (held by the test).
            assertEquals("exactly one allocation must be live while result is held",
                    1L, activeDirectAllocs(freshAllocator));
        } finally {
            if (result != null) {
                result.release();
            }
            pdu.close();
        }
        assertEquals("no active allocations after result is released",
                0L, activeDirectAllocs(freshAllocator));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Returns the sum of {@link PoolArenaMetric#numActiveAllocations()} across
     * all direct arenas of {@code allocator}.
     */
    private static long activeDirectAllocs(PooledByteBufAllocator allocator) {
        return allocator.metric().directArenas().stream()
                .mapToLong(PoolArenaMetric::numActiveAllocations)
                .sum();
    }
}
