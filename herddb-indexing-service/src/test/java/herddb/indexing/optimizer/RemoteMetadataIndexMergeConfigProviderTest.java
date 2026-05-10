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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import herddb.index.vector.VectorIndexManager;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.server.RemoteFileClient;
import herddb.utils.ExtendedDataOutputStream;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Unit tests for {@link RemoteMetadataIndexMergeConfigProvider} (issue #516).
 *
 * <p>Uses a fake {@link RemoteFileClient} that returns pre-baked binary bytes
 * matching the checkpoint metadata binary format, so no real remote file server
 * is required.
 *
 * <p>The two checkpoint files exercised:
 * <ul>
 *   <li>{@code {tsUuid}/_metadata/latest.checkpoint} — LSN of the latest committed checkpoint.</li>
 *   <li>{@code {tsUuid}/_metadata/indexes.{ledger}.{offset}.tablesmetadata} — list of
 *       serialized {@link Index} objects at that LSN.</li>
 * </ul>
 */
public class RemoteMetadataIndexMergeConfigProviderTest {

    private static final String TS_UUID = "test-ts-uuid";
    private static final long LEDGER = 42L;
    private static final long OFFSET = 999L;

    // -------------------------------------------------------------------------
    // Binary format helpers
    // -------------------------------------------------------------------------

    /**
     * Builds the binary content of {@code latest.checkpoint} for the given LSN.
     * Format (mirrors SharedCheckpointMetadataManager):
     * <pre>
     *   VLong(1) — version
     *   VLong(0) — flags
     *   UTF(tablespace name)
     *   ZLong(ledgerId)
     *   ZLong(offset)
     * </pre>
     */
    private static byte[] buildLatestCheckpointBytes(String tsName, long ledgerId, long offset)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ExtendedDataOutputStream dos = new ExtendedDataOutputStream(baos)) {
            dos.writeVLong(1L); // version
            dos.writeVLong(0L); // flags
            dos.writeUTF(tsName);
            dos.writeZLong(ledgerId);
            dos.writeZLong(offset);
        }
        return baos.toByteArray();
    }

    /**
     * Builds the binary content of the index-definitions tablesmetadata file for the given LSN.
     * Format (mirrors SharedCheckpointMetadataManager):
     * <pre>
     *   VLong(1) — version
     *   VLong(0) — flags
     *   UTF(tablespace name)
     *   ZLong(ledgerId)
     *   ZLong(offset)
     *   Int(count)
     *   // for each index:
     *   Array(Index.serialize())
     * </pre>
     */
    private static byte[] buildIndexDefinitionsBytes(String tsName, long ledgerId, long offset,
                                                     List<Index> indexes) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ExtendedDataOutputStream dos = new ExtendedDataOutputStream(baos)) {
            dos.writeVLong(1L); // version
            dos.writeVLong(0L); // flags
            dos.writeUTF(tsName);
            dos.writeZLong(ledgerId);
            dos.writeZLong(offset);
            dos.writeInt(indexes.size());
            for (Index idx : indexes) {
                dos.writeArray(idx.serialize());
            }
        }
        return baos.toByteArray();
    }

    /** Builds a vector {@link Index} with all jvector build parameters set in {@code properties}. */
    private static Index buildVectorIndex(String tableName, String indexName, String uuid,
                                          int graphM, int beamWidth,
                                          float neighborOverflow, float alpha,
                                          VectorSimilarityFunction similarity) {
        return Index.builder()
                .table(tableName)
                .tablespace(TS_UUID)
                .name(indexName)
                .uuid(uuid)
                .type(Index.TYPE_VECTOR)
                .column("v", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_M, String.valueOf(graphM))
                .property(VectorIndexManager.PROP_BEAM_WIDTH, String.valueOf(beamWidth))
                .property(VectorIndexManager.PROP_NEIGHBOR_OVERFLOW, String.valueOf(neighborOverflow))
                .property(VectorIndexManager.PROP_ALPHA, String.valueOf(alpha))
                .property(VectorIndexManager.PROP_SIMILARITY, similarity.name())
                .build();
    }

    /** Builds a fake {@link RemoteFileClient} backed by a pre-populated path→bytes map. */
    private static RemoteFileClient fakeClient(Map<String, byte[]> files) {
        return new RemoteFileClient() {
            @Override
            public byte[] readFile(String path) {
                return files.get(path);
            }

            @Override
            public void writeFile(String path, byte[] content) {
                files.put(path, content);
            }

            @Override
            public void updateServers(List<String> servers) {
            }

            @Override
            public boolean awaitServersReady(long timeoutMs) {
                return true;
            }
        };
    }

    // -------------------------------------------------------------------------
    // Happy path
    // -------------------------------------------------------------------------

    @Test
    public void resolvesByUuid() throws Exception {
        Index idx = buildVectorIndex("docs", "docs_v1", "idx-uuid-1",
                16, 64, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);

        Map<String, byte[]> files = new HashMap<>();
        files.put(TS_UUID + "/_metadata/latest.checkpoint",
                buildLatestCheckpointBytes("my-ts", LEDGER, OFFSET));
        files.put(TS_UUID + "/_metadata/indexes." + LEDGER + "." + OFFSET + ".tablesmetadata",
                buildIndexDefinitionsBytes("my-ts", LEDGER, OFFSET, List.of(idx)));

        RemoteMetadataIndexMergeConfigProvider provider =
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(files), TS_UUID);

        IndexMergeConfig cfg = provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "idx-uuid-1");

        assertNotNull(cfg);
        assertEquals(16, cfg.graphM);
        assertEquals(64, cfg.beamWidth);
        assertEquals(1.2f, cfg.neighborOverflow, 1e-6f);
        assertEquals(1.4f, cfg.alpha, 1e-6f);
        assertEquals(VectorSimilarityFunction.EUCLIDEAN, cfg.similarity);
    }

    @Test
    public void resolvesByNameFallbackWhenUuidMismatch() throws Exception {
        // Store index with a different UUID in the metadata; provider must fall
        // back to matching by (table, indexName).
        Index idx = buildVectorIndex("docs", "docs_v1", "different-uuid",
                8, 32, 1.1f, 1.3f, VectorSimilarityFunction.DOT_PRODUCT);

        Map<String, byte[]> files = new HashMap<>();
        files.put(TS_UUID + "/_metadata/latest.checkpoint",
                buildLatestCheckpointBytes("my-ts", LEDGER, OFFSET));
        files.put(TS_UUID + "/_metadata/indexes." + LEDGER + "." + OFFSET + ".tablesmetadata",
                buildIndexDefinitionsBytes("my-ts", LEDGER, OFFSET, List.of(idx)));

        RemoteMetadataIndexMergeConfigProvider provider =
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(files), TS_UUID);

        // Query with a UUID that does NOT match — must fall back to (table, name) lookup.
        IndexMergeConfig cfg = provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "wanted-uuid");

        assertNotNull(cfg);
        assertEquals(8, cfg.graphM);
        assertEquals(32, cfg.beamWidth);
        assertEquals(VectorSimilarityFunction.DOT_PRODUCT, cfg.similarity);
    }

    @Test
    public void resultIsCachedPerIndexUuid() throws Exception {
        Index idx = buildVectorIndex("docs", "docs_v1", "cache-uuid",
                4, 20, 1.0f, 1.0f, VectorSimilarityFunction.COSINE);

        Map<String, byte[]> files = new HashMap<>();
        files.put(TS_UUID + "/_metadata/latest.checkpoint",
                buildLatestCheckpointBytes("my-ts", LEDGER, OFFSET));
        files.put(TS_UUID + "/_metadata/indexes." + LEDGER + "." + OFFSET + ".tablesmetadata",
                buildIndexDefinitionsBytes("my-ts", LEDGER, OFFSET, List.of(idx)));

        RemoteMetadataIndexMergeConfigProvider provider =
                // Use a 1-hour TTL so the cache entry does not expire during the test.
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(files), TS_UUID, 3_600_000L);

        IndexMergeConfig first  = provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "cache-uuid");
        IndexMergeConfig second = provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "cache-uuid");

        // Same config object must be returned from cache on second call.
        assertSame("second call must hit the cache", first, second);
    }

    @Test
    public void invalidateCacheForcesReFetch() throws Exception {
        Index idx = buildVectorIndex("docs", "docs_v1", "invalidate-uuid",
                16, 64, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);

        Map<String, byte[]> files = new HashMap<>();
        files.put(TS_UUID + "/_metadata/latest.checkpoint",
                buildLatestCheckpointBytes("my-ts", LEDGER, OFFSET));
        files.put(TS_UUID + "/_metadata/indexes." + LEDGER + "." + OFFSET + ".tablesmetadata",
                buildIndexDefinitionsBytes("my-ts", LEDGER, OFFSET, List.of(idx)));

        RemoteMetadataIndexMergeConfigProvider provider =
                // Use a 1-hour TTL so the cache entry does not expire during the test.
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(files), TS_UUID, 3_600_000L);

        IndexMergeConfig first = provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "invalidate-uuid");
        provider.invalidateCache();
        IndexMergeConfig second = provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "invalidate-uuid");

        // After invalidation the provider must re-fetch and return a NEW object
        // (not the cached reference). The values must be equal (same metadata).
        assertNotSame("invalidateCache must force a re-fetch (different object instance)", first, second);
        assertEquals(first.graphM,           second.graphM);
        assertEquals(first.beamWidth,        second.beamWidth);
        assertEquals(first.neighborOverflow, second.neighborOverflow, 1e-6f);
        assertEquals(first.alpha,            second.alpha,            1e-6f);
        assertEquals(first.similarity,       second.similarity);
    }

    @Test
    public void readLatestCheckpointLsnParsesBytes() throws Exception {
        // Unit test for readLatestCheckpointLsn() in isolation.
        byte[] checkpointBytes = buildLatestCheckpointBytes("my-ts", 77L, 123L);

        Map<String, byte[]> files = new HashMap<>();
        files.put(TS_UUID + "/_metadata/latest.checkpoint", checkpointBytes);

        RemoteMetadataIndexMergeConfigProvider provider =
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(files), TS_UUID);

        LogSequenceNumber lsn = provider.readLatestCheckpointLsn();
        assertEquals(77L, lsn.ledgerId);
        assertEquals(123L, lsn.offset);
    }

    @Test
    public void readLatestCheckpointLsnReturnsStartOfTimeWhenFileAbsent() throws Exception {
        // When latest.checkpoint is absent the remote file client returns null →
        // provider must return LogSequenceNumber.START_OF_TIME (no checkpoint yet).
        RemoteMetadataIndexMergeConfigProvider provider =
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(new HashMap<>()), TS_UUID);

        LogSequenceNumber lsn = provider.readLatestCheckpointLsn();
        assertEquals(LogSequenceNumber.START_OF_TIME.ledgerId, lsn.ledgerId);
        assertEquals(LogSequenceNumber.START_OF_TIME.offset,   lsn.offset);
    }

    // -------------------------------------------------------------------------
    // Error cases
    // -------------------------------------------------------------------------

    @Test
    public void throwsWhenNoCheckpointYet() {
        // No checkpoint file on the fake client → getMergeConfig must throw Exception
        // explaining that no checkpoint has been published yet.
        RemoteMetadataIndexMergeConfigProvider provider =
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(new HashMap<>()), TS_UUID);

        try {
            provider.getMergeConfig(TS_UUID, "t", "i", "u");
            fail("expected Exception when no checkpoint has been published");
        } catch (Exception e) {
            // Expected — verify the message is useful.
            if (e.getMessage() == null || !e.getMessage().contains("checkpoint")) {
                fail("error message should mention 'checkpoint': " + e.getMessage());
            }
        }
    }

    @Test
    public void throwsWhenIndexNotFoundByUuidOrName() throws Exception {
        // Metadata contains an unrelated index; provider must throw clearly.
        Index other = buildVectorIndex("other", "other_idx", "other-uuid",
                16, 64, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);

        Map<String, byte[]> files = new HashMap<>();
        files.put(TS_UUID + "/_metadata/latest.checkpoint",
                buildLatestCheckpointBytes("my-ts", LEDGER, OFFSET));
        files.put(TS_UUID + "/_metadata/indexes." + LEDGER + "." + OFFSET + ".tablesmetadata",
                buildIndexDefinitionsBytes("my-ts", LEDGER, OFFSET, List.of(other)));

        RemoteMetadataIndexMergeConfigProvider provider =
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(files), TS_UUID);

        try {
            provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "missing-uuid");
            fail("expected Exception for index not found in checkpoint metadata");
        } catch (Exception e) {
            if (e.getMessage() == null
                    || !e.getMessage().contains("docs_v1")
                    || !e.getMessage().contains("missing-uuid")) {
                fail("error message should reference indexName and uuid: " + e.getMessage());
            }
        }
    }

    @Test
    public void throwsWhenRequiredPropertyAbsent() throws Exception {
        // Index with no jvector properties at all → parseConfig must throw.
        Index noProps = Index.builder()
                .table("docs")
                .tablespace(TS_UUID)
                .name("docs_v1")
                .uuid("noprop-uuid")
                .type(Index.TYPE_VECTOR)
                .column("v", ColumnTypes.FLOATARRAY)
                .build();

        Map<String, byte[]> files = new HashMap<>();
        files.put(TS_UUID + "/_metadata/latest.checkpoint",
                buildLatestCheckpointBytes("my-ts", LEDGER, OFFSET));
        files.put(TS_UUID + "/_metadata/indexes." + LEDGER + "." + OFFSET + ".tablesmetadata",
                buildIndexDefinitionsBytes("my-ts", LEDGER, OFFSET, List.of(noProps)));

        RemoteMetadataIndexMergeConfigProvider provider =
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(files), TS_UUID);

        try {
            provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "noprop-uuid");
            fail("expected Exception when required jvector property is absent");
        } catch (Exception e) {
            // Expected — must mention the missing property key.
            if (e.getMessage() == null || !e.getMessage().contains(VectorIndexManager.PROP_M)) {
                fail("error message should reference the missing property '"
                        + VectorIndexManager.PROP_M + "': " + e.getMessage());
            }
        }
    }

    @Test
    public void throwsWhenSimilarityIsUnknown() throws Exception {
        // Index with an invalid similarity name → parseConfig must throw.
        Index badSim = Index.builder()
                .table("docs")
                .tablespace(TS_UUID)
                .name("docs_v1")
                .uuid("badsim-uuid")
                .type(Index.TYPE_VECTOR)
                .column("v", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_M, "16")
                .property(VectorIndexManager.PROP_BEAM_WIDTH, "64")
                .property(VectorIndexManager.PROP_NEIGHBOR_OVERFLOW, "1.2")
                .property(VectorIndexManager.PROP_ALPHA, "1.4")
                .property(VectorIndexManager.PROP_SIMILARITY, "NOT_A_SIMILARITY")
                .build();

        Map<String, byte[]> files = new HashMap<>();
        files.put(TS_UUID + "/_metadata/latest.checkpoint",
                buildLatestCheckpointBytes("my-ts", LEDGER, OFFSET));
        files.put(TS_UUID + "/_metadata/indexes." + LEDGER + "." + OFFSET + ".tablesmetadata",
                buildIndexDefinitionsBytes("my-ts", LEDGER, OFFSET, List.of(badSim)));

        RemoteMetadataIndexMergeConfigProvider provider =
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(files), TS_UUID);

        try {
            provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "badsim-uuid");
            fail("expected Exception for unknown similarity function");
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains("NOT_A_SIMILARITY")) {
                fail("error message should reference the bad similarity: " + e.getMessage());
            }
        }
    }

    @Test
    public void throwsWhenPropertyIsNonPositive() throws Exception {
        // m=0 is a non-positive integer — requirePositiveInt must throw.
        Index idx = Index.builder()
                .table("docs")
                .tablespace(TS_UUID)
                .name("docs_v1")
                .uuid("nonpos-uuid")
                .type(Index.TYPE_VECTOR)
                .column("v", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_M, "0")  // <= 0: invalid
                .property(VectorIndexManager.PROP_BEAM_WIDTH, "64")
                .property(VectorIndexManager.PROP_NEIGHBOR_OVERFLOW, "1.2")
                .property(VectorIndexManager.PROP_ALPHA, "1.4")
                .property(VectorIndexManager.PROP_SIMILARITY, "EUCLIDEAN")
                .build();

        Map<String, byte[]> files = new HashMap<>();
        files.put(TS_UUID + "/_metadata/latest.checkpoint",
                buildLatestCheckpointBytes("my-ts", LEDGER, OFFSET));
        files.put(TS_UUID + "/_metadata/indexes." + LEDGER + "." + OFFSET + ".tablesmetadata",
                buildIndexDefinitionsBytes("my-ts", LEDGER, OFFSET, List.of(idx)));

        RemoteMetadataIndexMergeConfigProvider provider =
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(files), TS_UUID);

        try {
            provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "nonpos-uuid");
            fail("expected Exception for non-positive property value");
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains(VectorIndexManager.PROP_M)) {
                fail("error message should reference the property key: " + e.getMessage());
            }
        }
    }

    @Test
    public void throwsWhenPropertyIsNonNumeric() throws Exception {
        // m=abc is not a valid integer — requirePositiveInt must throw.
        Index idx = Index.builder()
                .table("docs")
                .tablespace(TS_UUID)
                .name("docs_v1")
                .uuid("nonnumeric-uuid")
                .type(Index.TYPE_VECTOR)
                .column("v", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_M, "abc")  // not a number
                .property(VectorIndexManager.PROP_BEAM_WIDTH, "64")
                .property(VectorIndexManager.PROP_NEIGHBOR_OVERFLOW, "1.2")
                .property(VectorIndexManager.PROP_ALPHA, "1.4")
                .property(VectorIndexManager.PROP_SIMILARITY, "EUCLIDEAN")
                .build();

        Map<String, byte[]> files = new HashMap<>();
        files.put(TS_UUID + "/_metadata/latest.checkpoint",
                buildLatestCheckpointBytes("my-ts", LEDGER, OFFSET));
        files.put(TS_UUID + "/_metadata/indexes." + LEDGER + "." + OFFSET + ".tablesmetadata",
                buildIndexDefinitionsBytes("my-ts", LEDGER, OFFSET, List.of(idx)));

        RemoteMetadataIndexMergeConfigProvider provider =
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(files), TS_UUID);

        try {
            provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "nonnumeric-uuid");
            fail("expected Exception for non-numeric property value");
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains(VectorIndexManager.PROP_M)) {
                fail("error message should reference the property key: " + e.getMessage());
            }
        }
    }

    @Test
    public void throwsOnCorruptCheckpointHeader() throws Exception {
        // Write a latest.checkpoint with version=2 (only 1 is valid) — must throw.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ExtendedDataOutputStream dos = new ExtendedDataOutputStream(baos)) {
            dos.writeVLong(2L); // bad version
            dos.writeVLong(0L);
            dos.writeUTF("my-ts");
            dos.writeZLong(LEDGER);
            dos.writeZLong(OFFSET);
        }

        Map<String, byte[]> files = new HashMap<>();
        files.put(TS_UUID + "/_metadata/latest.checkpoint", baos.toByteArray());

        RemoteMetadataIndexMergeConfigProvider provider =
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(files), TS_UUID);

        try {
            provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "any-uuid");
            fail("expected Exception for corrupt checkpoint header");
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains("Corrupted")) {
                fail("error message should indicate corruption: " + e.getMessage());
            }
        }
    }

    @Test
    public void throwsOnCorruptIndexDefinitionsHeader() throws Exception {
        // Write latest.checkpoint correctly, but index definitions with version=99.
        ByteArrayOutputStream idxBaos = new ByteArrayOutputStream();
        try (ExtendedDataOutputStream dos = new ExtendedDataOutputStream(idxBaos)) {
            dos.writeVLong(99L); // bad version
            dos.writeVLong(0L);
            dos.writeUTF("my-ts");
            dos.writeZLong(LEDGER);
            dos.writeZLong(OFFSET);
            dos.writeInt(0);
        }

        Map<String, byte[]> files = new HashMap<>();
        files.put(TS_UUID + "/_metadata/latest.checkpoint",
                buildLatestCheckpointBytes("my-ts", LEDGER, OFFSET));
        files.put(TS_UUID + "/_metadata/indexes." + LEDGER + "." + OFFSET + ".tablesmetadata",
                idxBaos.toByteArray());

        RemoteMetadataIndexMergeConfigProvider provider =
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(files), TS_UUID);

        try {
            provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "any-uuid");
            fail("expected Exception for corrupt index definitions header");
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains("Corrupted")) {
                fail("error message should indicate corruption: " + e.getMessage());
            }
        }
    }

    @Test
    public void throwsWhenIndexDefinitionsFileMissing() throws Exception {
        // latest.checkpoint points at a non-START_OF_TIME LSN, but the
        // indexes file is absent → must throw clearly.
        Map<String, byte[]> files = new HashMap<>();
        files.put(TS_UUID + "/_metadata/latest.checkpoint",
                buildLatestCheckpointBytes("my-ts", LEDGER, OFFSET));
        // Do NOT add the indexes file.

        RemoteMetadataIndexMergeConfigProvider provider =
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(files), TS_UUID);

        try {
            provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "any-uuid");
            fail("expected Exception when index definitions file is absent");
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains("Index definitions file not found")) {
                fail("error message should mention missing file: " + e.getMessage());
            }
        }
    }

    @Test
    public void cacheExpiresAfterTtl() throws Exception {
        // Use a tiny TTL (1 ms). Sleep 10 ms to ensure expiry, then verify a
        // second getMergeConfig call re-fetches (returns a different object).
        Index idx = buildVectorIndex("docs", "docs_v1", "ttl-uuid",
                16, 64, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);

        Map<String, byte[]> files = new HashMap<>();
        files.put(TS_UUID + "/_metadata/latest.checkpoint",
                buildLatestCheckpointBytes("my-ts", LEDGER, OFFSET));
        files.put(TS_UUID + "/_metadata/indexes." + LEDGER + "." + OFFSET + ".tablesmetadata",
                buildIndexDefinitionsBytes("my-ts", LEDGER, OFFSET, List.of(idx)));

        // 1 ms TTL — guaranteed to expire even under heavy system load within 10 ms.
        RemoteMetadataIndexMergeConfigProvider provider =
                new RemoteMetadataIndexMergeConfigProvider(fakeClient(files), TS_UUID, 1L);

        IndexMergeConfig first = provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "ttl-uuid");
        Thread.sleep(10L); // ensure TTL has passed
        IndexMergeConfig second = provider.getMergeConfig(TS_UUID, "docs", "docs_v1", "ttl-uuid");

        // The cache entry has expired; the provider must allocate a fresh IndexMergeConfig.
        assertNotSame("expired cache must produce a new IndexMergeConfig instance", first, second);
        assertEquals(first.graphM, second.graphM);
        assertEquals(first.similarity, second.similarity);
    }
}
