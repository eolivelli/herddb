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
package herddb.indexing.segment;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import herddb.log.LogSequenceNumber;
import java.io.IOException;
import org.junit.Test;

public class TombstoneOverlayTest {

    @Test
    public void roundTripEmptyOverlay() throws Exception {
        TombstoneOverlay original = new TombstoneOverlay(
                "seg-1", new LogSequenceNumber(0L, 0L), 0L, new int[0]);
        byte[] data = original.serialize();
        TombstoneOverlay roundtripped = TombstoneOverlay.deserialize(data);
        assertEquals("seg-1", roundtripped.getSegmentUuid());
        assertEquals(0L, roundtripped.getOverlayGeneration());
        assertEquals(0, roundtripped.ordinalCount());
    }

    @Test
    public void roundTripPopulatedOverlay() throws Exception {
        int[] ords = {3, 17, 42, 5_000, 999_999};
        TombstoneOverlay original = new TombstoneOverlay(
                "seg-abc", new LogSequenceNumber(123L, 456L), 7L, ords);
        TombstoneOverlay roundtripped = TombstoneOverlay.deserialize(original.serialize());
        assertEquals("seg-abc", roundtripped.getSegmentUuid());
        assertEquals(123L, roundtripped.getTombstoneLsn().ledgerId);
        assertEquals(456L, roundtripped.getTombstoneLsn().offset);
        assertEquals(7L, roundtripped.getOverlayGeneration());
        assertArrayEquals(ords, roundtripped.getTombstonedOrdinals());
    }

    @Test
    public void deserializeUnknownVersionThrows() {
        byte[] bogus = new byte[]{0, 0, 0, 99, // version = 99 (unsupported)
                                  0, 0, 0, 0  // uuid len 0
        };
        try {
            TombstoneOverlay.deserialize(bogus);
            fail("expected IOException");
        } catch (IOException ok) {
            assertEquals(true, ok.getMessage().contains("version"));
        }
    }

    @Test
    public void deserializeImplausibleUuidLengthThrows() {
        byte[] bogus = new byte[]{0, 0, 0, 1,  // version = 1
                                  0x40, 0, 0, 0  // uuid len > 4096
        };
        try {
            TombstoneOverlay.deserialize(bogus);
            fail("expected IOException");
        } catch (IOException ok) {
            assertEquals(true, ok.getMessage().contains("uuid length"));
        }
    }
}
