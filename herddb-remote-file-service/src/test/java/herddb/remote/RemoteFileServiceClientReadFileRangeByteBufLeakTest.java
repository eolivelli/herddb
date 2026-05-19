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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import herddb.proto.Pdu;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.concurrent.CompletableFuture;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Regression tests for issue #582 and issue #596.
 *
 * <h3>Issue #582 — exception-path leak</h3>
 * {@link RemoteFileServiceClient#parseReadFileRangeResponse} and
 * {@link RemoteFileServiceClient#parseReadFileResponse} must release the
 * allocated direct {@code ByteBuf buf} when the PDU-content read throws
 * after the allocation but before returning the buffer to the caller.
 *
 * <h3>Issue #596 — normal-path leak (future-cancellation race)</h3>
 * When the parser returns a {@code ByteBuf} successfully but
 * {@code CompletableFuture.complete(value)} returns {@code false} — because
 * the future was already cancelled or completed exceptionally by a concurrent
 * timeout / channel-idle event — the {@code ByteBuf} is abandoned with
 * refcount = 1. {@link RemoteFileServiceClient#completeOrRelease} fixes this
 * by calling {@link io.netty.util.ReferenceCountUtil#safeRelease} whenever
 * {@code complete()} returns {@code false}. Tests for this fix verify that
 * pre-cancelled or already-exceptionally-completed futures cause the buffer
 * to be released, while a normally-completing future leaves the buffer owned
 * by the caller.
 *
 * <h3>How the leak is triggered</h3>
 * Both parsers follow the pattern:
 * <pre>
 *   ByteBuf buf = allocator.directBuffer(len);     // ← allocation
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
 * <h3>Test strategy — spy allocator + {@code refCnt()} check</h3>
 * Each test passes a thin {@link PooledByteBufAllocator} subclass (the
 * "spy") that overrides {@code directBuffer(int)} to record the
 * last-returned {@code ByteBuf} reference. After the parser throws, the test
 * asserts {@code captured.refCnt() == 0}. This is deterministic:
 * <ul>
 *   <li>With the fix: the {@code finally} block calls {@code buf.release()},
 *       decrementing the reference count from 1 to 0 regardless of whether the
 *       underlying memory goes into the allocator's thread-local cache or back
 *       to the arena. {@code refCnt()} reflects the logical reference count on
 *       the object, not the allocator's deallocation counters.
 *   <li>Without the fix: {@code buf.release()} is never called, leaving
 *       {@code refCnt() == 1}.
 * </ul>
 * Note: {@link io.netty.buffer.PoolArenaMetric#numActiveAllocations()} is
 * intentionally <em>not</em> used here. When a pooled buffer is freed it is
 * returned to the {@code PoolThreadCache} rather than the arena, and
 * {@code deallocations} counters are not incremented — so
 * {@code numActiveAllocations()} stays at 1 even after a correct
 * {@code release()}, making it an unreliable leak-detection signal.
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
     * {@code ByteBuf} (refCnt → 0) when {@link IndexOutOfBoundsException} is
     * thrown by {@code PduCodec.ReadFileRangeResponse.readContent(pdu)} on a
     * truncated response (the production failure path from issue #582).
     *
     * <p>The PDU claims {@code contentLength=1000} but only carries 5 bytes of
     * payload. {@code readContent} calls {@code retainedSlice(16, 1000)} on a
     * 21-byte buffer, which immediately throws. Before the fix, the
     * {@code directBuffer(1000)} allocation was leaked (refCnt remained 1).
     *
     * <p>Repeated {@code ITERATIONS} times to confirm no accumulation across
     * multiple calls (the allocator may recycle object instances, but each
     * cycle must start with a fresh refCnt = 1 and end at 0).
     */
    @Test
    public void testParseReadFileRangeResponse_releasesBufOnParseError() {
        final ByteBuf[] captured = {null};
        PooledByteBufAllocator spy = spyAllocator(captured);

        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            captured[0] = null;

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
                RemoteFileServiceClient.parseReadFileRangeResponse(pdu, spy);
                fail("Expected IndexOutOfBoundsException from retainedSlice on truncated buffer");
            } catch (IndexOutOfBoundsException e) {
                // Expected: retainedSlice(16, 1000) on a 21-byte buffer throws immediately.
            } finally {
                pdu.close(); // releases the 'wire' ByteBuf (allocated from DEFAULT)
            }

            assertNotNull("spy must have been asked to allocate a directBuffer", captured[0]);
            // KEY ASSERTION: the fix's finally block must have called buf.release(),
            // decrementing refCnt from 1 to 0. Without the fix refCnt stays at 1.
            assertEquals(
                    "iteration " + (i + 1) + ": directBuffer(1000) refCnt must be 0 after "
                            + "parseReadFileRangeResponse threw IndexOutOfBoundsException "
                            + "(issue #582 regression: missing release in finally block)",
                    0, captured[0].refCnt());
        }
    }

    /**
     * Verifies that {@code parseReadFileRangeResponse} returns {@code null}
     * without allocating any buffer when {@code found == 0}.
     */
    @Test
    public void testParseReadFileRangeResponse_notFound_returnsNull() {
        final ByteBuf[] captured = {null};
        PooledByteBufAllocator spy = spyAllocator(captured);

        ByteBuf wire = PooledByteBufAllocator.DEFAULT.directBuffer(16);
        wire.writeByte(3);                             // VERSION_3
        wire.writeByte(Pdu.FLAGS_ISRESPONSE);
        wire.writeByte(Pdu.TYPE_FS_READ_FILE_RANGE);
        wire.writeLong(1L);                            // message id
        wire.writeByte(0);                             // found = false
        wire.writeInt(0);                              // contentLength irrelevant

        Pdu pdu = Pdu.newPdu(wire, Pdu.TYPE_FS_READ_FILE_RANGE, Pdu.FLAGS_ISRESPONSE, 1L);
        try {
            ByteBuf result = RemoteFileServiceClient.parseReadFileRangeResponse(pdu, spy);
            assertNull("not-found response must return null", result);
            assertNull("spy must not have been asked to allocate for not-found response",
                    captured[0]);
        } finally {
            pdu.close();
        }
    }

    /**
     * Verifies that {@code parseReadFileRangeResponse} returns a valid buffer
     * whose refCnt tracks correctly through use and release.
     */
    @Test
    public void testParseReadFileRangeResponse_validResponse_returnsBuf() {
        final ByteBuf[] captured = {null};
        PooledByteBufAllocator spy = spyAllocator(captured);

        byte[] content = "hello world".getBytes();
        // VERSION(1)+FLAGS(1)+TYPE(1)+MSGID(8)+found(1)+contentLen(4)+content(N) = 16+N bytes
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
            result = RemoteFileServiceClient.parseReadFileRangeResponse(pdu, spy);
            assertNotNull("well-formed response must return a non-null ByteBuf", result);
            assertEquals("returned ByteBuf must contain exactly the payload bytes",
                    content.length, result.readableBytes());
            assertNotNull("spy must have been asked to allocate", captured[0]);
            assertEquals("returned buffer must have refCnt == 1 while caller holds it",
                    1, captured[0].refCnt());
        } finally {
            if (result != null) {
                result.release();
            }
            pdu.close();
        }
        assertEquals("refCnt must drop to 0 after caller releases the buffer",
                0, captured[0].refCnt());
    }

    // -------------------------------------------------------------------------
    // parseReadFileResponse — proactive fix for the same latent bug
    // -------------------------------------------------------------------------

    /**
     * Verifies that {@code parseReadFileResponse} releases its allocated
     * {@code ByteBuf} (refCnt → 0) when {@link IndexOutOfBoundsException} is
     * thrown by {@code PduCodec.ReadFileResponse.readContent(pdu)} on a
     * truncated response (same latent bug as issue #582, fixed proactively).
     *
     * <p>Same spy-allocator strategy as
     * {@link #testParseReadFileRangeResponse_releasesBufOnParseError()}.
     */
    @Test
    public void testParseReadFileResponse_releasesBufOnParseError() {
        final ByteBuf[] captured = {null};
        PooledByteBufAllocator spy = spyAllocator(captured);

        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            captured[0] = null;

            // Build a malformed ReadFile response PDU (same layout as ReadFileRange).
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
                RemoteFileServiceClient.parseReadFileResponse(pdu, spy);
                fail("Expected IndexOutOfBoundsException from retainedSlice on truncated buffer");
            } catch (IndexOutOfBoundsException e) {
                // Expected.
            } finally {
                pdu.close();
            }

            assertNotNull("spy must have been asked to allocate", captured[0]);
            assertEquals(
                    "iteration " + (i + 1) + ": directBuffer(1000) refCnt must be 0 after "
                            + "parseReadFileResponse threw IndexOutOfBoundsException "
                            + "(issue #582 regression — proactive fix)",
                    0, captured[0].refCnt());
        }
    }

    /**
     * Verifies that {@code parseReadFileResponse} returns {@code null} without
     * allocating when {@code found == 0}.
     */
    @Test
    public void testParseReadFileResponse_notFound_returnsNull() {
        final ByteBuf[] captured = {null};
        PooledByteBufAllocator spy = spyAllocator(captured);

        ByteBuf wire = PooledByteBufAllocator.DEFAULT.directBuffer(16);
        wire.writeByte(3);
        wire.writeByte(Pdu.FLAGS_ISRESPONSE);
        wire.writeByte(Pdu.TYPE_FS_READ_FILE);
        wire.writeLong(2L);
        wire.writeByte(0);                             // found = false
        wire.writeInt(0);

        Pdu pdu = Pdu.newPdu(wire, Pdu.TYPE_FS_READ_FILE, Pdu.FLAGS_ISRESPONSE, 2L);
        try {
            ByteBuf result = RemoteFileServiceClient.parseReadFileResponse(pdu, spy);
            assertNull("not-found response must return null", result);
            assertNull("spy must not have been asked to allocate for not-found response",
                    captured[0]);
        } finally {
            pdu.close();
        }
    }

    /**
     * Verifies that {@code parseReadFileResponse} returns a valid buffer whose
     * refCnt tracks correctly through use and release.
     */
    @Test
    public void testParseReadFileResponse_validResponse_returnsBuf() {
        final ByteBuf[] captured = {null};
        PooledByteBufAllocator spy = spyAllocator(captured);

        byte[] content = "hello".getBytes();
        ByteBuf wire = PooledByteBufAllocator.DEFAULT.directBuffer(16 + content.length);
        wire.writeByte(3);
        wire.writeByte(Pdu.FLAGS_ISRESPONSE);
        wire.writeByte(Pdu.TYPE_FS_READ_FILE);
        wire.writeLong(3L);
        wire.writeByte(1);                             // found = true
        wire.writeInt(content.length);
        wire.writeBytes(content);

        Pdu pdu = Pdu.newPdu(wire, Pdu.TYPE_FS_READ_FILE, Pdu.FLAGS_ISRESPONSE, 3L);
        ByteBuf result = null;
        try {
            result = RemoteFileServiceClient.parseReadFileResponse(pdu, spy);
            assertNotNull("well-formed response must return a non-null ByteBuf", result);
            assertEquals("returned ByteBuf must contain exactly the payload bytes",
                    content.length, result.readableBytes());
            assertNotNull("spy must have been asked to allocate", captured[0]);
            assertEquals("returned buffer must have refCnt == 1 while caller holds it",
                    1, captured[0].refCnt());
        } finally {
            if (result != null) {
                result.release();
            }
            pdu.close();
        }
        assertEquals("refCnt must drop to 0 after caller releases the buffer",
                0, captured[0].refCnt());
    }

    // -------------------------------------------------------------------------
    // completeOrRelease — issue #596: normal-path future-cancellation leak
    // -------------------------------------------------------------------------

    /**
     * Regression test for issue #596 (primary scenario):
     * {@link RemoteFileServiceClient#completeOrRelease} must release the
     * {@code ByteBuf} when the target future is already cancelled.
     *
     * <p>This simulates the production race: the safety-net timeout in
     * {@code getUnchecked()} cancels the outer {@link CompletableFuture}
     * while the network response is in flight.  When the callback finally
     * fires, {@code parser.parse()} allocates a {@code ByteBuf} successfully,
     * but {@code future.complete(buf)} returns {@code false}.  Before the fix,
     * {@code buf} was abandoned (refCnt = 1, never released).  The fix calls
     * {@code ReferenceCountUtil.safeRelease(buf)}, dropping refCnt to 0.
     */
    @Test
    public void testCompleteOrRelease_releasesWhenFutureCancelled() {
        final ByteBuf[] captured = {null};
        PooledByteBufAllocator spy = spyAllocator(captured);

        ByteBuf buf = spy.directBuffer(16);
        assertEquals("precondition: freshly allocated buffer must have refCnt == 1",
                1, buf.refCnt());

        CompletableFuture<ByteBuf> future = new CompletableFuture<>();
        future.cancel(false);  // simulate safety-net timeout

        RemoteFileServiceClient.completeOrRelease(future, buf);

        assertEquals(
                "completeOrRelease must release the ByteBuf when future.complete() returns false"
                        + " due to cancellation (issue #596 regression)",
                0, captured[0].refCnt());
    }

    /**
     * Issue #596, error-race variant: {@code completeOrRelease} must release
     * the {@code ByteBuf} when the future was already completed exceptionally
     * (e.g., a concurrent channel-idle error completed it before the parsed
     * response arrived).
     */
    @Test
    public void testCompleteOrRelease_releasesWhenFutureAlreadyCompletedExceptionally() {
        final ByteBuf[] captured = {null};
        PooledByteBufAllocator spy = spyAllocator(captured);

        ByteBuf buf = spy.directBuffer(16);
        assertEquals("precondition: freshly allocated buffer must have refCnt == 1",
                1, buf.refCnt());

        CompletableFuture<ByteBuf> future = new CompletableFuture<>();
        future.completeExceptionally(new java.io.IOException("channel closed"));

        RemoteFileServiceClient.completeOrRelease(future, buf);

        assertEquals(
                "completeOrRelease must release the ByteBuf when future is already "
                        + "completed exceptionally (issue #596 race variant)",
                0, captured[0].refCnt());
    }

    /**
     * Issue #596, normal-completion guard: {@code completeOrRelease} must NOT
     * release the {@code ByteBuf} when the future accepts the value (i.e., when
     * {@code future.complete(buf)} returns {@code true}).  The caller is then
     * responsible for releasing the buffer, exactly as before.
     */
    @Test
    public void testCompleteOrRelease_doesNotReleaseWhenFutureAcceptsValue() {
        final ByteBuf[] captured = {null};
        PooledByteBufAllocator spy = spyAllocator(captured);

        ByteBuf buf = spy.directBuffer(16);

        CompletableFuture<ByteBuf> future = new CompletableFuture<>();

        RemoteFileServiceClient.completeOrRelease(future, buf);

        // The buffer must still be alive — the caller (future's dependent) owns it.
        assertEquals(
                "completeOrRelease must NOT release the ByteBuf when future.complete() "
                        + "returns true: caller now owns the refcount (issue #596 guard)",
                1, captured[0].refCnt());
        assertEquals("future must hold the buffer", buf, future.getNow(null));

        // Cleanup — simulate the caller releasing the buffer.
        future.getNow(null).release();
        assertEquals("refCnt must drop to 0 after caller releases the buffer",
                0, captured[0].refCnt());
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    /**
     * Creates a {@link PooledByteBufAllocator} subclass that records the last
     * {@code ByteBuf} returned by {@code directBuffer(int)} into
     * {@code captureSlot[0]}.
     *
     * <p>Used so tests can assert {@code captureSlot[0].refCnt() == 0} after
     * a parser method throws, verifying the {@code finally} block's
     * {@code buf.release()} call without relying on arena-level counters.
     */
    private static PooledByteBufAllocator spyAllocator(ByteBuf[] captureSlot) {
        return new PooledByteBufAllocator(true) {
            @Override
            public ByteBuf directBuffer(int initialCapacity) {
                ByteBuf buf = super.directBuffer(initialCapacity);
                captureSlot[0] = buf;
                return buf;
            }
        };
    }
}
