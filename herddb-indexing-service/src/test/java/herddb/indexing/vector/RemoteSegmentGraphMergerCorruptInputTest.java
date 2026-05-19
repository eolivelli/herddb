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
package herddb.indexing.vector;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.mem.MemoryDataStorageManager;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Validates that {@link RemoteSegmentGraphMerger} fails fast on corrupt or
 * dimension-mismatched input map files (issue #484, review items B.1#2 and
 * B.2#3 from the first pr-reviewer pass).
 *
 * <p>The merger validates every {@code int} read from the input file
 * against {@link RemoteSegmentGraphMerger#MAX_ENTRIES_PER_MAP_FILE} and
 * {@link RemoteSegmentGraphMerger#MAX_PK_LEN}, rejects negative ordinals,
 * and refuses any input whose vector dimension differs from the configured
 * one. Without these checks a corrupt or partially-uploaded multipart file
 * could either coerce the merger into a multi-GB allocation (OOM) or
 * silently corrupt the merged graph by writing mismatched-dim vectors.
 */
public class RemoteSegmentGraphMergerCorruptInputTest {

    private static final String TS_UUID = "ts-corrupt";
    private static final String IDX_UUID = "idx-corrupt";

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private MemoryDataStorageManager dsm;
    private Path tmpDir;

    @Before
    public void setUp() throws Exception {
        dsm = new MemoryDataStorageManager();
        tmpDir = tmp.newFolder("merger-tmp").toPath();
    }

    /** Writes a 2-input fixture where the second input is replaced by raw bytes. */
    private List<RemoteSegmentGraphMerger.RemoteSegmentInput> twoInputsWithCorruptSecond(
            byte[] corruptMapBytes) throws Exception {
        // First input is healthy: 5 vectors at dim=4.
        Path goodMap = Files.createTempFile(tmpDir, "good-", ".tmp");
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(goodMap.toFile()));
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(5);
            for (int i = 0; i < 5; i++) {
                dos.writeInt(i);
                byte[] pk = ("good-" + i).getBytes();
                dos.writeInt(pk.length);
                dos.write(pk);
                dos.writeInt(4);
                for (int j = 0; j < 4; j++) {
                    dos.writeInt(Float.floatToIntBits((float) (i + j)));
                }
            }
        }
        long goodSize = Files.size(goodMap);
        dsm.writeMultipartIndexFile(TS_UUID, IDX_UUID + "_seg1", "map", goodMap, null);
        Files.deleteIfExists(goodMap);

        // Second input: written as raw, possibly-malformed bytes.
        Path corruptMap = Files.createTempFile(tmpDir, "corrupt-", ".tmp");
        Files.write(corruptMap, corruptMapBytes);
        dsm.writeMultipartIndexFile(TS_UUID, IDX_UUID + "_seg2", "map", corruptMap, null);
        Files.deleteIfExists(corruptMap);

        return List.of(
                new RemoteSegmentGraphMerger.RemoteSegmentInput(
                        TS_UUID, IDX_UUID, "good-uuid", 1L, goodSize, 1L, new int[0]),
                new RemoteSegmentGraphMerger.RemoteSegmentInput(
                        TS_UUID, IDX_UUID, "corrupt-uuid", 2L, corruptMapBytes.length, 2L, new int[0]));
    }

    @Test
    public void garbageEntryCountThrowsIOException() throws Exception {
        // entryCount = Integer.MAX_VALUE — a sane reader must reject this rather
        // than try to allocate an int[Integer.MAX_VALUE] or loop billions of
        // times. The MAX_ENTRIES_PER_MAP_FILE cap (1<<28) is what stops us.
        byte[] bytes = new byte[]{
                (byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff
        };
        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpDir, /* M */ 8, /* beam */ 32, 1.2f, 1.4f,
                VectorSimilarityFunction.EUCLIDEAN);
        try {
            merger.merge(twoInputsWithCorruptSecond(bytes), TS_UUID, IDX_UUID,
                    /* output */ 999L, /* dim */ 4);
            fail("expected IOException for entryCount > MAX_ENTRIES_PER_MAP_FILE");
        } catch (IOException ok) {
            assertTrue("message should mention entryCount: " + ok.getMessage(),
                    ok.getMessage().contains("entryCount"));
        }
    }

    @Test
    public void negativeOrdinalThrowsIOException() throws Exception {
        // entryCount=1, ordinal=-1, then no further bytes (ok — read should
        // fail at the ordinal check before consuming pkLen).
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(1);
        dos.writeInt(-1); // negative ordinal
        // pad with bogus pkLen / floatCount to make sure parser doesn't get
        // confused before it hits the ordinal check
        dos.writeInt(0);
        dos.writeInt(0);
        dos.flush();

        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpDir, 8, 32, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);
        try {
            merger.merge(twoInputsWithCorruptSecond(baos.toByteArray()), TS_UUID, IDX_UUID,
                    999L, 4);
            fail("expected IOException for negative ordinal");
        } catch (IOException ok) {
            assertTrue("message should mention ordinal: " + ok.getMessage(),
                    ok.getMessage().contains("ordinal"));
        }
    }

    @Test
    public void oversizedPkLenThrowsIOException() throws Exception {
        // entryCount=1, ordinal=0, pkLen=Integer.MAX_VALUE — the cap
        // MAX_PK_LEN (1<<16) must reject this. Without the cap we'd allocate a
        // 2 GiB byte[] and OOM.
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(1);
        dos.writeInt(0);
        dos.writeInt(Integer.MAX_VALUE); // oversized pkLen
        dos.flush();

        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpDir, 8, 32, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);
        try {
            merger.merge(twoInputsWithCorruptSecond(baos.toByteArray()), TS_UUID, IDX_UUID,
                    999L, 4);
            fail("expected IOException for oversized pkLen");
        } catch (IOException ok) {
            assertTrue("message should mention pkLen: " + ok.getMessage(),
                    ok.getMessage().contains("pkLen"));
        }
    }

    @Test
    public void dimensionMismatchThrowsIOException() throws Exception {
        // entryCount=1, ordinal=0, pkLen=3, "abc", floatCount=8. The merger is
        // configured with dim=4, so the floatCount=8 mismatch must abort the
        // merge before any vector data is consumed (silently building a graph
        // with mismatched-dim vectors would corrupt the InlineVectors feature).
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(1);
        dos.writeInt(0);          // ordinal
        dos.writeInt(3);          // pkLen
        dos.write("abc".getBytes()); // pkBytes
        dos.writeInt(8);          // floatCount = 8 (mismatch with dim=4)
        // Do not write the floats — the merger should fail before consuming them.
        dos.flush();

        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpDir, 8, 32, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);
        try {
            merger.merge(twoInputsWithCorruptSecond(baos.toByteArray()), TS_UUID, IDX_UUID,
                    /* outputSegmentId */ 9999L, /* dim */ 4);
            fail("expected IOException for dimension mismatch");
        } catch (IOException ok) {
            assertTrue("message should mention dimension mismatch: " + ok.getMessage(),
                    ok.getMessage().contains("dimension mismatch"));
        }
    }
}
