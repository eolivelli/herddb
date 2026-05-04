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
 * <p>This is the regression gate for issue #387 — the change replaces the
 * per-call {@code DataOutputStream}/{@code ByteBufOutputStream} wrapper pair
 * with direct {@code ByteBuf} writes and switches the table-name encoding
 * from {@code DataOutputStream.writeUTF} (modified UTF-8 + 2-byte length cap)
 * to a vint-prefixed standard UTF-8 sequence.
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
        assertEquals("tableName", entry.tableName, decoded.tableName);
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
        assertRoundTrip(new LogEntry(1L, LogEntryType.INSERT, 7L, "t1", b("k1"), b("v1")));
    }

    @Test
    public void testUpdateRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(2L, LogEntryType.UPDATE, 7L, "t1", b("k1"), b("v2")));
    }

    @Test
    public void testDeleteRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(3L, LogEntryType.DELETE, 7L, "t1", b("k1"), null));
    }

    @Test
    public void testCreateTableRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(4L, LogEntryType.CREATE_TABLE, 0L, "t1", null, b("table-def")));
    }

    @Test
    public void testAlterTableRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(5L, LogEntryType.ALTER_TABLE, 0L, "t1", null, b("new-table-def")));
    }

    @Test
    public void testCreateIndexRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(6L, LogEntryType.CREATE_INDEX, 0L, "t1", null, b("index-def")));
    }

    @Test
    public void testDropTableRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(7L, LogEntryType.DROP_TABLE, 0L, "t1", null, null));
    }

    @Test
    public void testTruncateTableRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(8L, LogEntryType.TRUNCATE_TABLE, 0L, "t1", null, null));
    }

    @Test
    public void testDropIndexRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(9L, LogEntryType.DROP_INDEX, 0L, null, null, b("ix1")));
    }

    @Test
    public void testBeginTransactionRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(10L, LogEntryType.BEGINTRANSACTION, 42L, null, null, null));
    }

    @Test
    public void testCommitTransactionRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(11L, LogEntryType.COMMITTRANSACTION, 42L, null, null, null));
    }

    @Test
    public void testRollbackTransactionRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(12L, LogEntryType.ROLLBACKTRANSACTION, 42L, null, null, null));
    }

    @Test
    public void testNoopRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(13L, LogEntryType.NOOP, 0L, null, null, null));
    }

    @Test
    public void testTableConsistencyCheckRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(14L, LogEntryType.TABLE_CONSISTENCY_CHECK, 0L, "t1", null, b("checksum")));
    }

    @Test
    public void testIndexingServiceRebalanceRoundTrip() throws Exception {
        assertRoundTrip(new LogEntry(15L, LogEntryType.INDEXING_SERVICE_REBALANCE, 0L, null, null, b("descriptor-bytes")));
    }

    @Test
    public void testNonAsciiTableName() throws Exception {
        // Multi-byte UTF-8 table name (CJK + supplementary code point) — exercises
        // the new vint-prefixed standard-UTF-8 encoding (4 bytes for U+1F600,
        // not 6 bytes as modified UTF-8 / DataOutputStream.writeUTF would emit).
        String name = "中文_" + new String(Character.toChars(0x1F600));
        assertRoundTrip(new LogEntry(16L, LogEntryType.INSERT, 1L, name, b("k"), b("v")));
    }

    @Test
    public void testEmptyTableName() throws Exception {
        // Edge case: zero-length table name. Vint(0) + zero bytes.
        assertRoundTrip(new LogEntry(17L, LogEntryType.UPDATE, 1L, "", b("k"), b("v")));
    }

    @Test
    public void testLongTableName() throws Exception {
        // > 65535 UTF-8 bytes — would have overflowed DataOutputStream.writeUTF
        // (16-bit length cap), but the new vint-prefixed encoding has no such limit.
        StringBuilder sb = new StringBuilder(70_000);
        for (int i = 0; i < 70_000; i++) {
            sb.append('a');
        }
        assertRoundTrip(new LogEntry(18L, LogEntryType.INSERT, 1L, sb.toString(), b("k"), b("v")));
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
                    "t" + (i % 10),
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
                                    "table-" + threadIdx,
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
                                    || !entry.tableName.equals(decoded.tableName)
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
        LogEntry entry = new LogEntry(1L, LogEntryType.INSERT, 1L, "t1", b("k"), b("v"));
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
}
