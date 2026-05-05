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
package herddb.log;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import herddb.utils.Bytes;
import io.netty.buffer.ByteBuf;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * Verifies that {@link LogEntry#serializeAsByteBuf()} (the direct-{@code ByteBuf}
 * write path used by {@code BookkeeperCommitLog.writeEntry}) produces output
 * that:
 *
 * <ul>
 *   <li>is byte-for-byte identical to the existing {@link LogEntry#serialize()}
 *       {@code byte[]} write path (so a single deserializer reads both);</li>
 *   <li>round-trips through {@link LogEntry#deserialize(byte[])} into an equal
 *       {@link LogEntry};</li>
 *   <li>is correct when the same thread calls {@code serializeAsByteBuf} many
 *       times in a row;</li>
 *   <li>is correct when many threads call it concurrently.</li>
 * </ul>
 *
 * <p>Issue #408 changed the wire format: every entry now carries a small
 * {@code vint} {@code tableId} instead of the per-entry UTF-8 table name. This
 * test class is the regression gate for both that change and for the prior
 * issue #387 ({@code ByteBuf} direct-write path agreeing with the {@code byte[]}
 * write path).
 */
public class LogEntrySerializeAsByteBufTest {

    private static byte[] viaByteBuf(LogEntry entry) {
        ByteBuf buf = entry.serializeAsByteBuf();
        try {
            byte[] out = new byte[buf.readableBytes()];
            buf.readBytes(out);
            return out;
        } finally {
            buf.release();
        }
    }

    /**
     * Asserts that the {@code ByteBuf} write path agrees byte-for-byte with the
     * {@code byte[]} write path, and that the bytes round-trip back into an
     * equal entry.
     */
    private static void assertRoundTrip(LogEntry entry) throws Exception {
        byte[] viaArray = entry.serialize();
        byte[] viaBuffer = viaByteBuf(entry);

        assertArrayEquals(
                "ByteBuf and byte[] write paths produced different output for type=" + entry.type,
                viaArray, viaBuffer);

        LogEntry decoded = LogEntry.deserialize(viaBuffer);
        assertEquals("type", entry.type, decoded.type);
        assertEquals("transactionId", entry.transactionId, decoded.transactionId);
        assertEquals("timestamp", entry.timestamp, decoded.timestamp);
        assertEquals("tableId", entry.tableId, decoded.tableId);
        assertBytesEqual("key", entry.key, decoded.key);
        assertBytesEqual("value", entry.value, decoded.value);
    }

    private static void assertBytesEqual(String label, Bytes expected, Bytes actual) {
        if (expected == null) {
            assertNull(label, actual);
            return;
        }
        assertArrayEquals(label, expected.to_array(), actual.to_array());
    }

    private static Bytes b(String s) {
        return Bytes.from_string(s);
    }

    @Test
    public void testInsertRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(1L, LogEntryType.INSERT, 7L, 11, b("k1"), b("v1")));
    }

    @Test
    public void testUpdateRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(2L, LogEntryType.UPDATE, 7L, 11, b("k1"), b("v2")));
    }

    @Test
    public void testDeleteRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(3L, LogEntryType.DELETE, 7L, 11, b("k1"), null));
    }

    @Test
    public void testCreateTableRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(4L, LogEntryType.CREATE_TABLE, 0L, 11, null, b("table-def")));
    }

    @Test
    public void testAlterTableRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(5L, LogEntryType.ALTER_TABLE, 0L, 11, null, b("new-table-def")));
    }

    @Test
    public void testCreateIndexRoundTrip() throws Exception {
        // CREATE_INDEX entries are written with tableId == 0; the parent
        // table is encoded inside the serialized {@link Index} payload.
        assertRoundTrip(new LogEntry(6L, LogEntryType.CREATE_INDEX, 0L, 0, null, b("index-def")));
    }

    @Test
    public void testDropTableRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(7L, LogEntryType.DROP_TABLE, 0L, 11, null, null));
    }

    @Test
    public void testTruncateTableRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(8L, LogEntryType.TRUNCATE_TABLE, 0L, 11, null, null));
    }

    @Test
    public void testDropIndexRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(9L, LogEntryType.DROP_INDEX, 0L, 0, null, b("ix1")));
    }

    @Test
    public void testBeginTransactionRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(10L, LogEntryType.BEGINTRANSACTION, 42L, 0, null, null));
    }

    @Test
    public void testCommitTransactionRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(11L, LogEntryType.COMMITTRANSACTION, 42L, 0, null, null));
    }

    @Test
    public void testRollbackTransactionRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(12L, LogEntryType.ROLLBACKTRANSACTION, 42L, 0, null, null));
    }

    @Test
    public void testNoopRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(13L, LogEntryType.NOOP, 0L, 0, null, null));
    }

    @Test
    public void testTableConsistencyCheckRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(14L, LogEntryType.TABLE_CONSISTENCY_CHECK, 0L, 11, null, b("checksum")));
    }

    @Test
    public void testIndexingServiceRebalanceRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(15L, LogEntryType.INDEXING_SERVICE_REBALANCE, 0L, 0, null, b("descriptor-bytes")));
    }

    @Test
    public void testTableIdAtVariousMagnitudes() throws Exception {
        // Cover every vint width: 1-byte (≤ 127), 2-byte (128..16383),
        // 3-byte, 4-byte, and 5-byte (Integer.MAX_VALUE).
        for (int tableId : new int[]{1, 127, 128, 16_383, 16_384, 2_097_151, 2_097_152, 268_435_455, 268_435_456, Integer.MAX_VALUE}) {
            assertRoundTrip(new LogEntry(16L, LogEntryType.INSERT, 1L, tableId, b("k"), b("v")));
        }
    }

    @Test
    public void testTableIdZeroForControlEntries() throws Exception {
        // 0 is the "no table" sentinel — used by control entries that have no
        // table affinity. Verify they encode and decode losslessly with id 0.
        assertRoundTrip(new LogEntry(17L, LogEntryType.NOOP, 0L, 0, null, null));
        assertRoundTrip(new LogEntry(18L, LogEntryType.BEGINTRANSACTION, 1L, 0, null, null));
    }

    @Test
    public void testRepeatedSerializationOnSameThread() throws Exception {
        // Confirms there is no leftover state between calls (e.g. that we did
        // not accidentally introduce a per-thread cached buffer that retains
        // bytes from a prior call).
        for (int i = 0; i < 1000; i++) {
            LogEntry entry = new LogEntry(
                    i,
                    LogEntryType.INSERT,
                    i,
                    (i % 10) + 1,
                    b("k" + i),
                    b("v" + i));
            assertRoundTrip(entry);
        }
    }

    @Test
    public void testConcurrentSerialization() throws Exception {
        final int threads = 8;
        final int perThread = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger failures = new AtomicInteger();
            List<Future<?>> futures = new ArrayList<>(threads);
            for (int t = 0; t < threads; t++) {
                final int threadIdx = t;
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            LogEntry entry = new LogEntry(
                                    (long) threadIdx * perThread + i,
                                    LogEntryType.INSERT,
                                    threadIdx,
                                    threadIdx + 1,
                                    b("k-" + threadIdx + "-" + i),
                                    b("v-" + threadIdx + "-" + i));
                            byte[] viaArray = entry.serialize();
                            byte[] viaBuffer = viaByteBuf(entry);
                            if (!java.util.Arrays.equals(viaArray, viaBuffer)) {
                                failures.incrementAndGet();
                                return null;
                            }
                            LogEntry decoded = LogEntry.deserialize(viaBuffer);
                            if (decoded.transactionId != entry.transactionId
                                    || entry.tableId != decoded.tableId
                                    || !java.util.Arrays.equals(entry.key.to_array(), decoded.key.to_array())
                                    || !java.util.Arrays.equals(entry.value.to_array(), decoded.value.to_array())) {
                                failures.incrementAndGet();
                                return null;
                            }
                        }
                    // CHECKSTYLE.OFF: IllegalCatch — test failures from any thread
                    // should be surfaced via the failure counter rather than crash
                    // the worker thread silently.
                    } catch (Throwable t2) {
                    // CHECKSTYLE.ON: IllegalCatch
                        t2.printStackTrace();
                        failures.incrementAndGet();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
            assertEquals("concurrent serialization produced mismatched bytes or decoded values",
                    0, failures.get());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void testTruncatedBufferThrowsEofException() {
        LogEntry entry = new LogEntry(1L, LogEntryType.INSERT, 1L, 7, b("k"), b("v"));
        byte[] full = entry.serialize();
        // Drop the last byte to confirm the deserializer surfaces the partial
        // entry as an EOFException (relied upon by recovery code).
        byte[] truncated = new byte[full.length - 1];
        System.arraycopy(full, 0, truncated, 0, truncated.length);
        try {
            LogEntry.deserialize(truncated);
            fail("expected EOFException for truncated input");
        } catch (EOFException expected) {
            // ok
        }
    }

    /**
     * Truncating a serialized entry at <em>any</em> offset must surface as an
     * {@link EOFException} (or, for cuts that land mid-{@code vint}, as
     * {@link EOFException}/{@link RuntimeException}/{@link IllegalArgumentException}
     * depending on which field was truncated) — never as a silently-decoded
     * garbage entry. Recovery relies on this contract to detect a partial
     * trailing entry on a crashed log file. Exercises every cut point from 1
     * byte up to {@code N - 1} bytes.
     */
    @Test
    public void testTruncatedBufferAtEveryOffset() {
        LogEntry entry = new LogEntry(1L, LogEntryType.INSERT, 1L, 7, b("k"), b("v"));
        byte[] full = entry.serialize();
        for (int len = 1; len < full.length; len++) {
            byte[] truncated = new byte[len];
            System.arraycopy(full, 0, truncated, 0, len);
            try {
                LogEntry.deserialize(truncated);
                fail("expected EOFException/RuntimeException/IllegalArgumentException for truncation at len="
                        + len + " (full=" + full.length + ")");
            } catch (EOFException expected) {
                // ok — surfaced as EOF, recovery treats this as the end of the log
            // CHECKSTYLE.OFF: IllegalCatch — IllegalArgumentException can be thrown
            // by the type-switch when a vint cut lands mid-bytes and produces a
            // bogus type code; RuntimeException is thrown by deserialize when
            // readArray reads a negative length other than -1 (mid-vint cut).
            // Both of these behaviours are acceptable: the test asserts that
            // truncation never silently decodes to a valid LogEntry.
            } catch (IllegalArgumentException expected) {
                // ok
            } catch (RuntimeException expected) {
                // ok — wraps the underlying I/O error from a mid-vint cut
            }
            // CHECKSTYLE.ON: IllegalCatch
        }
    }

    /**
     * A {@code Bytes} key/value containing every byte value 0x00..0xFF must
     * round-trip cleanly. Particularly important because the new wire format
     * uses negative vint values (specifically -1) to encode null arrays, so a
     * payload with an embedded 0xFF byte must not be confused with a length
     * prefix.
     */
    @Test
    public void testKeyAndValueWithAllByteValues() throws Exception {
        byte[] all = new byte[256];
        for (int i = 0; i < 256; i++) {
            all[i] = (byte) i;
        }
        Bytes key = Bytes.from_array(all);
        Bytes value = Bytes.from_array(all.clone());
        LogEntry entry = new LogEntry(1L, LogEntryType.INSERT, 1L, 7, key, value);
        assertRoundTrip(entry);

        // Also via the Stream-writer path:
        byte[] streamBytes = entry.serialize();
        LogEntry decoded = LogEntry.deserialize(streamBytes);
        assertArrayEquals("key bytes must round-trip exactly", all, decoded.key.to_array());
        assertArrayEquals("value bytes must round-trip exactly", all, decoded.value.to_array());
    }

    /**
     * Asserts that {@code LogEntry.serialize()} (the Stream-based byte[] write
     * path) produces output that {@code LogEntry.deserialize(byte[])} decodes
     * into an equal entry, for every {@link LogEntryType}. The other tests in
     * this class compare the ByteBuf path against the Stream path (proving they
     * agree byte-for-byte), but none of them prove the Stream path's bytes are
     * actually decodable end-to-end. This test closes that gap.
     */
    @Test
    public void testStreamWritePathRoundTripsForEveryType() throws Exception {
        List<LogEntry> all = new ArrayList<>();
        all.add(new LogEntry(1L, LogEntryType.INSERT, 1L, 11, b("k"), b("v")));
        all.add(new LogEntry(1L, LogEntryType.UPDATE, 1L, 11, b("k"), b("v")));
        all.add(new LogEntry(1L, LogEntryType.DELETE, 1L, 11, b("k"), null));
        all.add(new LogEntry(1L, LogEntryType.CREATE_TABLE, 0L, 11, null, b("def")));
        all.add(new LogEntry(1L, LogEntryType.ALTER_TABLE, 0L, 11, null, b("def")));
        all.add(new LogEntry(1L, LogEntryType.CREATE_INDEX, 0L, 0, null, b("idef")));
        all.add(new LogEntry(1L, LogEntryType.DROP_TABLE, 0L, 11, null, null));
        all.add(new LogEntry(1L, LogEntryType.TRUNCATE_TABLE, 0L, 11, null, null));
        all.add(new LogEntry(1L, LogEntryType.DROP_INDEX, 0L, 0, null, b("ix1")));
        all.add(new LogEntry(1L, LogEntryType.BEGINTRANSACTION, 7L, 0, null, null));
        all.add(new LogEntry(1L, LogEntryType.COMMITTRANSACTION, 7L, 0, null, null));
        all.add(new LogEntry(1L, LogEntryType.ROLLBACKTRANSACTION, 7L, 0, null, null));
        all.add(new LogEntry(1L, LogEntryType.NOOP, 0L, 0, null, null));
        all.add(new LogEntry(1L, LogEntryType.TABLE_CONSISTENCY_CHECK, 0L, 11, null, b("checksum")));
        all.add(new LogEntry(1L, LogEntryType.INDEXING_SERVICE_REBALANCE, 0L, 0, null, b("payload")));

        for (LogEntry entry : all) {
            byte[] bytes = entry.serialize();
            LogEntry decoded = LogEntry.deserialize(bytes);
            assertEquals("type for " + entry, entry.type, decoded.type);
            assertEquals("transactionId for " + entry, entry.transactionId, decoded.transactionId);
            assertEquals("timestamp for " + entry, entry.timestamp, decoded.timestamp);
            assertEquals("tableId for " + entry, entry.tableId, decoded.tableId);
            assertBytesEqual("key for " + entry, entry.key, decoded.key);
            assertBytesEqual("value for " + entry, entry.value, decoded.value);
        }
    }
}
