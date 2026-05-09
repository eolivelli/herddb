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
package herddb.index.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import herddb.log.LogSequenceNumber;
import org.junit.Test;

/**
 * Round-trip + boundary tests for
 * {@link VectorIndexManager#encodeRebuildLsn} and
 * {@link VectorIndexManager#decodeRebuildLsn} (issue #471). The codec
 * is the on-the-wire contract between {@code TableSpaceManager.createIndex}
 * (which encodes the pinned checkpoint LSN onto the {@code Index}
 * properties) and the IndexingService (which decodes it on the
 * CREATE_INDEX log entry to know which table checkpoint to scan).
 *
 * @author enrico.olivelli
 */
public class VectorIndexManagerRebuildLsnCodecTest {

    @Test
    public void roundTrip_zeroLedger_zeroOffset() {
        roundTrip(0L, 0L);
    }

    @Test
    public void roundTrip_zeroLedger_maxOffset() {
        roundTrip(0L, Long.MAX_VALUE);
    }

    @Test
    public void roundTrip_maxLedger_zeroOffset() {
        roundTrip(Long.MAX_VALUE, 0L);
    }

    @Test
    public void roundTrip_maxLedger_maxOffset() {
        roundTrip(Long.MAX_VALUE, Long.MAX_VALUE);
    }

    @Test
    public void roundTrip_typicalProductionValues() {
        // A representative point: ledger ID in the millions, offset
        // in the billions — the shape we see in production traffic.
        roundTrip(2_345_678L, 3_456_789_012L);
    }

    private void roundTrip(long ledgerId, long offset) {
        LogSequenceNumber original = new LogSequenceNumber(ledgerId, offset);
        String encoded = VectorIndexManager.encodeRebuildLsn(original);
        // The encoding must be exactly "<ledgerId>:<offset>" with no
        // padding, prefixes, or trailing whitespace — keep the IS-side
        // parser dumb.
        assertEquals(ledgerId + ":" + offset, encoded);
        LogSequenceNumber decoded = VectorIndexManager.decodeRebuildLsn(encoded);
        assertEquals("ledgerId must round-trip", ledgerId, decoded.ledgerId);
        assertEquals("offset must round-trip", offset, decoded.offset);
    }

    @Test
    public void encode_rejectsNullLsn() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.encodeRebuildLsn(null));
    }

    @Test
    public void encode_rejectsStartOfTime() {
        // START_OF_TIME has ledgerId == -1, offset == -1 — it is the
        // sentinel the engine uses for "no checkpoint observed yet"
        // and has no business being pinned for a rebuild. Surface
        // the misuse loudly at the encoder.
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.encodeRebuildLsn(LogSequenceNumber.START_OF_TIME));
    }

    @Test
    public void encode_rejectsNegativeLedger() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.encodeRebuildLsn(new LogSequenceNumber(-1L, 5L)));
    }

    @Test
    public void encode_rejectsNegativeOffset() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.encodeRebuildLsn(new LogSequenceNumber(5L, -1L)));
    }

    @Test
    public void decode_rejectsNullEncoding() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn(null));
    }

    @Test
    public void decode_rejectsMissingColon() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn("12345"));
    }

    @Test
    public void decode_rejectsTooManyColons() {
        // Defensive: a future encoder bug that double-emits the
        // separator must be caught at decode time, not silently
        // accepted with truncated values.
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn("1:2:3"));
    }

    @Test
    public void decode_rejectsNonNumericLedger() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn("abc:5"));
    }

    @Test
    public void decode_rejectsNonNumericOffset() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn("5:abc"));
    }

    @Test
    public void decode_rejectsNegativeLedger() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn("-1:5"));
    }

    @Test
    public void decode_rejectsNegativeOffset() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn("5:-1"));
    }

    @Test
    public void decode_rejectsEmptyString() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn(""));
    }

    @Test
    public void decode_rejectsBareColon() {
        // Both components empty — Long.parseLong("") throws, but pin
        // the rejection here so a future "be lenient" parser change
        // cannot silently accept it as 0:0.
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn(":"));
    }

    @Test
    public void decode_rejectsEmptyLedger() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn(":5"));
    }

    @Test
    public void decode_rejectsEmptyOffset() {
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn("5:"));
    }

    @Test
    public void decode_rejectsLeadingPlus() {
        // Long.parseLong("+1") returns 1, but the encoder never
        // produces a '+' prefix — accepting one would break a defensive
        // re-encode-and-compare round-trip check.
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn("+1:5"));
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn("1:+5"));
    }

    @Test
    public void decode_rejectsLeadingZeroOnMultiDigit() {
        // Long.parseLong("01") returns 1, but the encoder never
        // produces a leading-zero shape — same reasoning as the '+'
        // rejection above. Single zero "0" is fine and is exercised
        // by roundTrip_zeroLedger_zeroOffset.
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn("01:5"));
        assertThrows(IllegalArgumentException.class,
                () -> VectorIndexManager.decodeRebuildLsn("1:05"));
    }
}
