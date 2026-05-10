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
package herddb.indexing.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentState;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Direct unit tests for {@link RemoteSegmentMerger#peekDimFromMapFile} (issue #516).
 *
 * <p>Writes synthetic map files with known well-formed and malformed headers
 * via {@link MemoryDataStorageManager}, then calls {@code peekDimFromMapFile}
 * directly (package-private visibility) and asserts the correct dimension is
 * returned or {@link IllegalStateException} is thrown with a useful message.
 */
public class RemoteSegmentMergerPeekDimTest {

    private static final String TS_UUID = "ts-pdim";
    private static final String IDX_UUID = "idx-pdim";

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private MemoryDataStorageManager dsm;
    private Path tmpDir;

    @Before
    public void setUp() throws Exception {
        dsm = new MemoryDataStorageManager();
        tmpDir = tmp.newFolder("pdim-tmp").toPath();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Writes a map file to the DSM and returns the corresponding SegmentMetadata. */
    private SegmentMetadata writeMapFile(long segmentId, byte[] mapFileBytes) throws Exception {
        Path f = Files.createTempFile(tmpDir, "map-", ".tmp");
        try {
            Files.write(f, mapFileBytes);
            String multipartUuid = IDX_UUID + "_seg" + segmentId;
            dsm.writeMultipartIndexFile(TS_UUID, multipartUuid, "map", f, null);
        } finally {
            Files.deleteIfExists(f);
        }
        return SegmentMetadata.builder()
                .segmentUuid("seg-" + segmentId)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .segmentId(segmentId)
                .graphPath("g/x")
                .mapPath("m/x")
                .baseLsn(new LogSequenceNumber(1L, 1L))
                .sizeBytes((long) mapFileBytes.length * 2)
                .mapFileSize(mapFileBytes.length)
                .vectorCount(1L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
    }

    /**
     * Builds a minimal well-formed map file with exactly one entry.
     * Format: int entryCount, int ordinal, int pkLen, byte[pkLen] pk,
     *         int floatCount, int[floatCount] floatBits.
     */
    private byte[] buildWellFormedMapFile(int dim) throws Exception {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(baos))) {
            dos.writeInt(1);           // entryCount
            dos.writeInt(0);           // ordinal
            byte[] pk = "pk0".getBytes();
            dos.writeInt(pk.length);   // pkLen
            dos.write(pk);
            dos.writeInt(dim);         // floatCount = dim
            for (int j = 0; j < dim; j++) {
                dos.writeInt(Float.floatToIntBits(j * 0.1f));
            }
        }
        return baos.toByteArray();
    }

    private RemoteSegmentMerger makeMerger() {
        return new RemoteSegmentMerger(
                dsm, tmpDir,
                new StaticIndexMergeConfigProvider(new IndexMergeConfig(
                        16, 64, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN)));
    }

    // -------------------------------------------------------------------------
    // Happy path
    // -------------------------------------------------------------------------

    @Test
    public void peeksDimCorrectly() throws Exception {
        int dim = 128;
        SegmentMetadata seg = writeMapFile(1L, buildWellFormedMapFile(dim));
        RemoteSegmentMerger merger = makeMerger();
        assertEquals(dim, merger.peekDimFromMapFile(seg));
    }

    @Test
    public void peeksDimCorrectlySmall() throws Exception {
        int dim = 4;
        SegmentMetadata seg = writeMapFile(2L, buildWellFormedMapFile(dim));
        RemoteSegmentMerger merger = makeMerger();
        assertEquals(dim, merger.peekDimFromMapFile(seg));
    }

    // -------------------------------------------------------------------------
    // Error cases: malformed map file headers
    // -------------------------------------------------------------------------

    @Test
    public void throwsWhenEntryCountIsZero() throws Exception {
        // Write a map file with entryCount = 0 (empty segment, but shouldn't reach merge).
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(baos))) {
            dos.writeInt(0); // entryCount = 0
        }
        SegmentMetadata seg = writeMapFile(3L, baos.toByteArray());
        RemoteSegmentMerger merger = makeMerger();
        try {
            merger.peekDimFromMapFile(seg);
            fail("expected IllegalStateException for entryCount=0");
        } catch (IllegalStateException e) {
            assertTrue("error message should mention empty or corrupt: " + e.getMessage(),
                    e.getMessage().contains("empty or corrupt"));
        }
    }

    @Test
    public void throwsWhenEntryCountIsNegative() throws Exception {
        // Negative entryCount — corrupt header.
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(baos))) {
            dos.writeInt(-1); // entryCount = -1
        }
        SegmentMetadata seg = writeMapFile(4L, baos.toByteArray());
        RemoteSegmentMerger merger = makeMerger();
        try {
            merger.peekDimFromMapFile(seg);
            fail("expected IllegalStateException for entryCount=-1");
        } catch (IllegalStateException e) {
            assertTrue("error message should mention empty or corrupt: " + e.getMessage(),
                    e.getMessage().contains("empty or corrupt"));
        }
    }

    @Test
    public void throwsWhenPkLenIsOutOfRange() throws Exception {
        // pkLen = 70_000 exceeds the 65_536 cap.
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(baos))) {
            dos.writeInt(1);        // entryCount
            dos.writeInt(0);        // ordinal
            dos.writeInt(70_000);   // pkLen > 65_536
        }
        SegmentMetadata seg = writeMapFile(5L, baos.toByteArray());
        RemoteSegmentMerger merger = makeMerger();
        try {
            merger.peekDimFromMapFile(seg);
            fail("expected IllegalStateException for pkLen out of range");
        } catch (IllegalStateException e) {
            assertTrue("error message should mention pkLen: " + e.getMessage(),
                    e.getMessage().contains("pkLen="));
        }
    }

    @Test
    public void throwsWhenFloatCountIsNonPositive() throws Exception {
        // floatCount = 0 is non-positive — corrupt or dim-less index.
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(baos))) {
            dos.writeInt(1);           // entryCount
            dos.writeInt(0);           // ordinal
            byte[] pk = "pk0".getBytes();
            dos.writeInt(pk.length);   // pkLen
            dos.write(pk);
            dos.writeInt(0);           // floatCount = 0 (non-positive)
        }
        SegmentMetadata seg = writeMapFile(6L, baos.toByteArray());
        RemoteSegmentMerger merger = makeMerger();
        try {
            merger.peekDimFromMapFile(seg);
            fail("expected IllegalStateException for floatCount=0");
        } catch (IllegalStateException e) {
            assertTrue("error message should mention floatCount: " + e.getMessage(),
                    e.getMessage().contains("floatCount="));
        }
    }

    @Test
    public void throwsWhenFloatCountExceedsLimit() throws Exception {
        // floatCount = 70_000 exceeds the 65_536 cap — implausible dimension.
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(baos))) {
            dos.writeInt(1);           // entryCount
            dos.writeInt(0);           // ordinal
            byte[] pk = "pk0".getBytes();
            dos.writeInt(pk.length);   // pkLen
            dos.write(pk);
            dos.writeInt(70_000);      // floatCount > 65_536
        }
        SegmentMetadata seg = writeMapFile(7L, baos.toByteArray());
        RemoteSegmentMerger merger = makeMerger();
        try {
            merger.peekDimFromMapFile(seg);
            fail("expected IllegalStateException for floatCount out of range");
        } catch (IllegalStateException e) {
            assertTrue("error message should mention floatCount: " + e.getMessage(),
                    e.getMessage().contains("floatCount="));
        }
    }
}
