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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * Validates fix D2: {@link TombstoneOverlay#deserialize} guards against an
 * implausible {@code count} field that would otherwise force a multi-GB
 * {@code int[]} allocation. Crafts hand-rolled wire payloads with various
 * malformed counts and asserts each is rejected.
 */
public class TombstoneOverlayBoundsTest {

    private static byte[] buildPayload(int count) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(TombstoneOverlay.WIRE_FORMAT_VERSION);
            byte[] uuidBytes = "fake-uuid".getBytes(StandardCharsets.UTF_8);
            dos.writeInt(uuidBytes.length);
            dos.write(uuidBytes);
            dos.writeLong(0L); // tombstoneLsn ledgerId
            dos.writeLong(0L); // tombstoneLsn offset
            dos.writeLong(1L); // generation
            dos.writeInt(count); // <-- under test
            // No ordinals written — readFully on the int[] read loop will EOF if the count
            // makes it past the bounds check (which it must not).
        }
        return baos.toByteArray();
    }

    @Test
    public void rejectsNegativeCount() {
        try {
            TombstoneOverlay.deserialize(buildPayload(-1));
            fail("expected IOException for negative count");
        } catch (IOException ok) {
            assertTrue(ok.getMessage().contains("implausible tombstone count"));
        }
    }

    @Test
    public void rejectsCountExceedingMaximum() {
        try {
            TombstoneOverlay.deserialize(buildPayload(TombstoneOverlay.MAX_TOMBSTONED_ORDINALS + 1));
            fail("expected IOException for over-cap count");
        } catch (IOException ok) {
            assertTrue("error must mention max", ok.getMessage().contains("implausible tombstone count"));
            assertTrue(ok.getMessage().contains("max="));
        }
    }

    @Test
    public void acceptsCountWellAboveTypicalWithoutBoundsRejection() throws Exception {
        // Review-pass-3 P3-7: verify the count-bounds check accepts a value well
        // above the typical sparse-delete pattern (a few thousand) without rejecting
        // it. Deliberately stay far BELOW {@link TombstoneOverlay#MAX_TOMBSTONED_ORDINALS}
        // (1<<28 = 256M) so we don't allocate a 1 GB int[] on every CI run — the
        // earlier version of this test pressured the heap of every neighbouring
        // test in the same Surefire fork. 1<<16 = 65 536 is plenty: it's two orders
        // of magnitude above realistic per-segment delete counts but only ~256 KB.
        int wellAboveTypical = 1 << 16;
        try {
            TombstoneOverlay.deserialize(buildPayload(wellAboveTypical));
            fail("expected exception because no ordinals were supplied");
        } catch (IOException eof) {
            // The bounds check passes; we then fail in readInt() trying to read the
            // first ordinal. EOFException.getMessage() may be null on some JVMs.
            String msg = eof.getMessage();
            assertTrue("must NOT be a count-bounds error",
                    msg == null || !msg.contains("implausible tombstone count"));
        }
    }

    @Test
    public void maxCapIsAtLeastReasonablyLarge() {
        // Sanity check that the cap is large enough for real workloads (~500M vectors
        // per 2 GiB segment).
        assertTrue("MAX must be >= 256M to fit a 2 GiB segment",
                TombstoneOverlay.MAX_TOMBSTONED_ORDINALS >= (1 << 28));
    }

    @Test
    public void roundTripAtSmallSizesUnchangedByBoundsCheck() throws Exception {
        TombstoneOverlay original = new TombstoneOverlay(
                "seg", new herddb.log.LogSequenceNumber(1L, 2L),
                3L, new int[]{4, 5, 6, 7});
        TombstoneOverlay roundtrip = TombstoneOverlay.deserialize(original.serialize());
        assertEquals(4, roundtrip.ordinalCount());
        assertEquals(7, roundtrip.ordinalAt(3));
    }

    /**
     * Validates fix B2 from the second pr-reviewer pass: the production read path
     * via {@link herddb.indexing.segment.TombstoneOverlayManager#loadOverlay}
     * also enforces the {@code MAX_TOMBSTONED_ORDINALS} cap, not just the
     * in-memory {@link TombstoneOverlay#deserialize}.
     */
    @Test
    public void loadOverlayRejectsImplausibleCount() throws Exception {
        // Build the overlay file ourselves and stage it through a real DSM.
        java.nio.file.Path baseDir = java.nio.file.Files.createTempDirectory("herd-overlay-bounds");
        herddb.file.FileDataStorageManager dsm = new herddb.file.FileDataStorageManager(baseDir);
        try {
            String ts = "ts";
            String idxUuid = "idx";
            String segUuid = "seg";
            dsm.initTablespace(ts);
            byte[] payload = buildPayload(TombstoneOverlay.MAX_TOMBSTONED_ORDINALS + 1);
            java.nio.file.Path tmp = java.nio.file.Files.createTempFile(baseDir, "ov-", ".bin");
            java.nio.file.Files.write(tmp, payload);
            dsm.writeMultipartIndexFile(ts,
                    herddb.indexing.segment.TombstoneOverlayManager.multipartUuidFor(idxUuid, segUuid),
                    herddb.indexing.segment.TombstoneOverlayManager.fileTypeFor(1L),
                    tmp, null);
            try {
                herddb.indexing.segment.TombstoneOverlayManager.loadOverlay(dsm, ts, idxUuid, segUuid, 1L);
                fail("expected IOException for over-cap count");
            } catch (IOException ok) {
                assertTrue("error must mention count: " + ok.getMessage(),
                        ok.getMessage().contains("implausible tombstone count"));
            }
        } finally {
            dsm.close();
            try (java.util.stream.Stream<java.nio.file.Path> walk =
                    java.nio.file.Files.walk(baseDir)) {
                walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                    try {
                        java.nio.file.Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                    }
                });
            }
        }
    }
}
