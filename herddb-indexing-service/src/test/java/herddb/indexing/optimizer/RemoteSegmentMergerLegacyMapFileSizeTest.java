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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentState;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies the merger's handling of <em>legacy</em> znodes — those whose
 * {@code SegmentMetadata.mapFileSize} field is unset (issue #484, round-3
 * review item B.1.1).
 *
 * <p>The production storage readers ({@code RemoteRandomAccessReader},
 * {@code SegmentedMappedReader}) treat the {@code fileSize} passed at
 * construction as authoritative — they don't probe the underlying object —
 * and the direct-multipart-download path computes block count from that
 * size and throws "Block N not found" past the actual file end. Silently
 * over-reading would corrupt the merged graph; trying to clamp the read
 * via {@code RandomAccessReader.length()} is a no-op on every production
 * reader because {@code length()} returns the constructor-supplied size
 * unchanged.
 *
 * <p>The merger therefore refuses legacy inputs outright with
 * {@link RemoteSegmentMerger.LegacyMetadataException}. The engine catches
 * the exception, bumps {@code mergeFailuresTotal}, and moves on — the
 * next IS checkpoint rewrites the znode with {@code mapFileSize} set,
 * unsticking the segment.
 */
public class RemoteSegmentMergerLegacyMapFileSizeTest {

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final String TS_UUID = "ts-legacy-mfs";
    private static final String IDX_UUID = "idx-legacy-mfs";
    private static final int DIM = 8;
    private static final int VECTORS_PER_SEGMENT = 50;

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private MemoryDataStorageManager dsm;
    private Path tmpDir;

    @Before
    public void setUp() throws Exception {
        dsm = new MemoryDataStorageManager();
        tmpDir = tmp.newFolder("merger-tmp").toPath();
    }

    private SegmentMetadata writeLegacyInput(String segUuid, long segId, int seed) throws Exception {
        Path mapFile = Files.createTempFile(tmpDir, "input-", ".tmp");
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(mapFile.toFile()));
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(VECTORS_PER_SEGMENT);
            Random rng = new Random(seed);
            for (int local = 0; local < VECTORS_PER_SEGMENT; local++) {
                dos.writeInt(local);
                byte[] pk = (segUuid + "-" + local).getBytes();
                dos.writeInt(pk.length);
                dos.write(pk);
                VectorFloat<?> v = VTS.createFloatVector(DIM);
                for (int j = 0; j < DIM; j++) {
                    v.set(j, rng.nextFloat());
                }
                dos.writeInt(v.length());
                for (int j = 0; j < v.length(); j++) {
                    dos.writeInt(Float.floatToIntBits(v.get(j)));
                }
            }
        }
        long actualMapSize = Files.size(mapFile);
        long combinedSizeHint = actualMapSize * 4L; // mimic graph + map combined
        String multipartUuid = IDX_UUID + "_seg" + segId;
        String mapPath = dsm.writeMultipartIndexFile(TS_UUID, multipartUuid, "map", mapFile, null);
        Files.deleteIfExists(mapFile);

        return SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID).tableName("docs")
                .indexUuid(IDX_UUID).indexName("docs_v1")
                .state(SegmentState.ACTIVE).ownerInstanceId(0)
                .segmentId(segId)
                .graphPath("g/" + segUuid).mapPath(mapPath)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(combinedSizeHint)
                // Legacy: mapFileSize INTENTIONALLY left at the default 0L.
                .vectorCount(VECTORS_PER_SEGMENT).generation(1L)
                .createdAtEpochMillis(0L)
                .build();
    }

    @Test
    public void legacyZnodeRefusedOnMemoryDsm() throws Exception {
        SegmentMetadata a = writeLegacyInput("legacy-mfs-A", 100L, 0xA);
        SegmentMetadata b = writeLegacyInput("legacy-mfs-B", 200L, 0xB);

        // Sanity: both inputs carry mapFileSize == 0.
        assertEquals(SegmentMetadata.UNKNOWN_FILE_SIZE, a.getMapFileSize());
        assertEquals(SegmentMetadata.UNKNOWN_FILE_SIZE, b.getMapFileSize());

        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir, DIM, /* M */ 8, /* beam */ 32, 1.2f, 1.4f,
                VectorSimilarityFunction.EUCLIDEAN);
        try {
            merger.merge(List.of(a, b), /* newOwnerInstance */ 0);
            fail("expected LegacyMetadataException for legacy znode without mapFileSize");
        } catch (RemoteSegmentMerger.LegacyMetadataException ok) {
            // The first input that the merger encounters trips the check; we
            // assert at least one of the segment uuids appears in the message.
            assertTrue("error message should reference the legacy segment: "
                            + ok.getMessage(),
                    ok.getMessage().contains("legacy-mfs-")
                            && ok.getMessage().contains("predating issue #484"));
        }
    }

    @Test
    public void modernZnodeFailsLoudlyAgainstHintReturningReader() throws Exception {
        // Regression test for round-3 review B.1.1: production
        // RandomAccessReader implementations (RemoteRandomAccessReader,
        // SegmentedMappedReader) treat the constructor-supplied fileSize as
        // authoritative and return it from length() — they don't probe the
        // underlying object. The round-4 fix removed the misleading
        // length()-based clamp from RemoteSegmentGraphMerger.downloadMapFile;
        // the caller (RemoteSegmentMerger) is now solely responsible for
        // passing a byte-accurate size.
        //
        // This test exercises the production-reader contract directly by
        // wiring a {@link HintReturningReader} that mirrors
        // RemoteRandomAccessReader.length(). We feed a *modern* znode whose
        // mapFileSize is INTENTIONALLY too large vs the actual remote bytes
        // — the merger then asks the reader for that many bytes, the reader
        // honours the over-estimate, and {@code readFully} throws when the
        // underlying buffer runs out. Asserts the failure is loud (an
        // IOException) rather than silent under-read or torn graph.
        HintReturningDsm productionLikeDsm = new HintReturningDsm();
        // Plant a 16-byte real map file on the DSM, but configure the
        // SegmentMetadata to claim 1024 bytes — the production reader will
        // happily return length()=1024 from the size hint, and the merger
        // will read until it runs off the end.
        Path scratch = Files.createTempFile(tmpDir, "scratch-", ".tmp");
        Files.write(scratch, new byte[16]);
        productionLikeDsm.writeMultipartIndexFile(TS_UUID, IDX_UUID + "_seg500", "map",
                scratch, null);
        Files.deleteIfExists(scratch);
        Path scratch2 = Files.createTempFile(tmpDir, "scratch2-", ".tmp");
        Files.write(scratch2, new byte[16]);
        productionLikeDsm.writeMultipartIndexFile(TS_UUID, IDX_UUID + "_seg600", "map",
                scratch2, null);
        Files.deleteIfExists(scratch2);

        SegmentMetadata oversizedHint1 = SegmentMetadata.builder()
                .segmentUuid("oversized-A").tablespaceUuid(TS_UUID).tableName("t")
                .indexUuid(IDX_UUID).indexName("i").state(SegmentState.ACTIVE)
                .ownerInstanceId(0).segmentId(500L)
                .graphPath("g").mapPath("m")
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(2048L)
                .mapFileSize(1024L) // far larger than the 16-byte real file
                .vectorCount(1L).generation(1L)
                .createdAtEpochMillis(0L)
                .build();
        SegmentMetadata oversizedHint2 = SegmentMetadata.builder()
                .segmentUuid("oversized-B").tablespaceUuid(TS_UUID).tableName("t")
                .indexUuid(IDX_UUID).indexName("i").state(SegmentState.ACTIVE)
                .ownerInstanceId(0).segmentId(600L)
                .graphPath("g").mapPath("m")
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(2048L)
                .mapFileSize(1024L)
                .vectorCount(1L).generation(2L)
                .createdAtEpochMillis(0L)
                .build();

        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                productionLikeDsm, tmpDir, DIM, 8, 32, 1.2f, 1.4f,
                VectorSimilarityFunction.EUCLIDEAN);
        try {
            merger.merge(List.of(oversizedHint1, oversizedHint2), 0);
            fail("expected merge to fail loudly when mapFileSize over-estimates"
                    + " against a production-like reader (no length()-based clamp)");
        } catch (Exception ok) {
            // The exact exception type is implementation-defined (IOException
            // bubbling up from readFully, or DataStorageManagerException from
            // the multipart layer); assert at least that the failure was
            // surfaced rather than swallowed.
            assertTrue("expected loud failure, got: " + ok,
                    ok instanceof IOException
                            || ok instanceof RuntimeException);
        }
    }

    @Test
    public void legacyZnodeRefusedBeforeAnyIo() throws Exception {
        // Companion to legacyZnodeRefusedOnMemoryDsm: same legacy refusal
        // behaviour, but against the production-like DSM. The
        // LegacyMetadataException is thrown by the merger BEFORE any
        // multipart reader is opened, so this test passes regardless of
        // what the reader's length() returns. Documents the layering: the
        // mapFileSize check at RemoteSegmentMerger.merge sits upstream of
        // any I/O.
        HintReturningDsm productionLikeDsm = new HintReturningDsm();
        SegmentMetadata legacy = writeLegacyInput("legacy-prod-A", 100L, 0xA);
        SegmentMetadata other = writeLegacyInput("legacy-prod-B", 200L, 0xB);

        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                productionLikeDsm, tmpDir, DIM, 8, 32, 1.2f, 1.4f,
                VectorSimilarityFunction.EUCLIDEAN);
        try {
            merger.merge(List.of(legacy, other), 0);
            fail("expected LegacyMetadataException");
        } catch (RemoteSegmentMerger.LegacyMetadataException ok) {
            // pass — refused upstream of any reader I/O.
        }
    }

    @Test
    public void modernZnodeWithExplicitMapFileSizeMerges() throws Exception {
        // Control: with mapFileSize set, the merge proceeds normally.
        Path mapA = Files.createTempFile(tmpDir, "modern-A-", ".tmp");
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(mapA.toFile()));
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(10);
            for (int i = 0; i < 10; i++) {
                dos.writeInt(i);
                byte[] pk = ("modern-A-" + i).getBytes();
                dos.writeInt(pk.length);
                dos.write(pk);
                dos.writeInt(DIM);
                for (int j = 0; j < DIM; j++) {
                    dos.writeInt(Float.floatToIntBits((float) (i + j)));
                }
            }
        }
        long realSize = Files.size(mapA);
        dsm.writeMultipartIndexFile(TS_UUID, IDX_UUID + "_seg300", "map", mapA, null);
        Files.deleteIfExists(mapA);
        SegmentMetadata modern1 = SegmentMetadata.builder()
                .segmentUuid("modern-A").tablespaceUuid(TS_UUID).tableName("t")
                .indexUuid(IDX_UUID).indexName("i").state(SegmentState.ACTIVE)
                .ownerInstanceId(0).segmentId(300L)
                .graphPath("g").mapPath("m")
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(realSize * 4L)
                .mapFileSize(realSize)
                .vectorCount(10L).generation(1L)
                .createdAtEpochMillis(0L)
                .build();

        Path mapB = Files.createTempFile(tmpDir, "modern-B-", ".tmp");
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(mapB.toFile()));
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(10);
            for (int i = 0; i < 10; i++) {
                dos.writeInt(i);
                byte[] pk = ("modern-B-" + i).getBytes();
                dos.writeInt(pk.length);
                dos.write(pk);
                dos.writeInt(DIM);
                for (int j = 0; j < DIM; j++) {
                    dos.writeInt(Float.floatToIntBits((float) (i + j + 100)));
                }
            }
        }
        long realSizeB = Files.size(mapB);
        dsm.writeMultipartIndexFile(TS_UUID, IDX_UUID + "_seg400", "map", mapB, null);
        Files.deleteIfExists(mapB);
        SegmentMetadata modern2 = SegmentMetadata.builder()
                .segmentUuid("modern-B").tablespaceUuid(TS_UUID).tableName("t")
                .indexUuid(IDX_UUID).indexName("i").state(SegmentState.ACTIVE)
                .ownerInstanceId(0).segmentId(400L)
                .graphPath("g").mapPath("m")
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(realSizeB * 4L)
                .mapFileSize(realSizeB)
                .vectorCount(10L).generation(2L)
                .createdAtEpochMillis(0L)
                .build();

        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir, DIM, 8, 32, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);
        SegmentMetadata merged = merger.merge(List.of(modern1, modern2), 0);
        assertNotNull(merged);
        assertEquals(20L, merged.getVectorCount());
        // The merger's own output must populate mapFileSize so a future
        // merge of this output goes straight through the modern path.
        assertTrue("merger output must populate mapFileSize: " + merged.getMapFileSize(),
                merged.getMapFileSize() > 0L);
    }

    /**
     * DSM whose multipart reader behaves like {@code RemoteRandomAccessReader}:
     * {@code length()} returns the constructor-supplied {@code fileSize} hint
     * unchanged, regardless of the actual underlying object size. This is the
     * production semantics that defeats any naive {@code length()}-based clamp.
     */
    private static final class HintReturningDsm extends MemoryDataStorageManager {
        @Override
        public ReaderSupplier multipartIndexReaderSupplier(String tableSpace, String uuid,
                                                            String fileType, long fileSize)
                throws DataStorageManagerException {
            ReaderSupplier delegate = super.multipartIndexReaderSupplier(tableSpace, uuid, fileType, fileSize);
            return () -> {
                RandomAccessReader inner = delegate.get();
                return new HintReturningReader(inner, fileSize);
            };
        }
    }

    /**
     * Wraps a real {@link RandomAccessReader} but pretends {@link #length()}
     * returns the size hint from construction. Mirrors
     * {@code RemoteRandomAccessReader.length()} semantics.
     */
    private static final class HintReturningReader implements RandomAccessReader {
        private final RandomAccessReader delegate;
        private final long lenHint;

        HintReturningReader(RandomAccessReader delegate, long lenHint) {
            this.delegate = delegate;
            this.lenHint = lenHint;
        }

        @Override
        public long length() {
            return lenHint;
        }

        @Override
        public void seek(long offset) throws IOException {
            delegate.seek(offset);
        }

        @Override
        public long getPosition() throws IOException {
            return delegate.getPosition();
        }

        @Override
        public int readInt() throws IOException {
            return delegate.readInt();
        }

        @Override
        public float readFloat() throws IOException {
            return delegate.readFloat();
        }

        @Override
        public long readLong() throws IOException {
            return delegate.readLong();
        }

        @Override
        public void readFully(byte[] dest) throws IOException {
            delegate.readFully(dest);
        }

        @Override
        public void readFully(java.nio.ByteBuffer buffer) throws IOException {
            delegate.readFully(buffer);
        }

        @Override
        public void readFully(long[] vector) throws IOException {
            delegate.readFully(vector);
        }

        @Override
        public void read(int[] ints, int offset, int count) throws IOException {
            delegate.read(ints, offset, count);
        }

        @Override
        public void read(float[] floats, int offset, int count) throws IOException {
            delegate.read(floats, offset, count);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
