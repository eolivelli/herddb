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

package herddb.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import herddb.utils.MetadataCompression;
import herddb.utils.VisibleByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Covers the GZIP-wrapped serialization added for issue #161 and the
 * magic-byte fallback that lets readers still parse legacy uncompressed
 * JSON payloads.
 */
public class LedgersInfoTest {

    @Test
    public void roundTripWith1000SequentialLedgersCompressesWell() throws Exception {
        LedgersInfo info = new LedgersInfo();
        for (long id = 14; id < 14 + 1000; id++) {
            info.addLedger(id);
        }
        info.setFirstLedger(14);

        byte[] uncompressed;
        try (VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream()) {
            new ObjectMapper().writeValue(oo, info);
            uncompressed = oo.toByteArray();
        }

        byte[] compressed = info.serialize();
        assertTrue("serialize() must produce a GZIP-framed payload",
                MetadataCompression.looksGzip(compressed));

        // 1000 sequential longs as JSON (field names + per-entry timestamps) is
        // highly repetitive: GZIP at best-compression reliably beats 5x here,
        // and usually much more. Keep the assertion conservative.
        assertTrue(
                "expected >5x compression, raw=" + uncompressed.length
                        + " compressed=" + compressed.length,
                compressed.length * 5 < uncompressed.length);
        System.out.println("LedgersInfo 1000 ledgers: raw=" + uncompressed.length
                + " compressed=" + compressed.length
                + " ratio=" + (uncompressed.length / (double) compressed.length));

        LedgersInfo back = LedgersInfo.deserialize(compressed, 42);
        assertEquals(info.getActiveLedgers(), back.getActiveLedgers());
        assertEquals(info.getFirstLedger(), back.getFirstLedger());
        assertEquals(42, back.getZkVersion());
    }

    @Test
    public void deserializeAcceptsLegacyPlainJson() {
        String legacy = "{\"activeLedgers\":[1,2,3],"
                + "\"ledgersTimestamps\":[100,200,300],"
                + "\"firstLedger\":1}";
        LedgersInfo back = LedgersInfo.deserialize(legacy.getBytes(StandardCharsets.UTF_8), 7);
        List<Long> expected = new ArrayList<>();
        expected.add(1L);
        expected.add(2L);
        expected.add(3L);
        assertEquals(expected, back.getActiveLedgers());
        assertEquals(1, back.getFirstLedger());
        assertEquals(7, back.getZkVersion());
    }

    @Test
    public void deserializeHandlesEmptyPayload() {
        LedgersInfo back = LedgersInfo.deserialize(new byte[0], 3);
        assertTrue(back.getActiveLedgers().isEmpty());
        assertEquals(3, back.getZkVersion());
    }
}
