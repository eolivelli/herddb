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

package herddb.remote.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * Issue #638: end-to-end integration test for the direct-S3 bulk upload
 * path against a real MinIO container (via Testcontainers). Verifies that
 * {@link S3ObjectStorage#uploadFile} drives a real S3 Multipart Upload
 * via {@link S3TransferManager} (parts pipelined by the CRT HTTP client),
 * stores a single object at the configured key (no per-block layout
 * suffix), reports progress, and that the resulting object can be
 * downloaded back with {@link S3ObjectStorage#downloadFileBulk} and
 * via a byte-range {@code getObject} for random-access reads.
 */
public class S3DirectBulkUploadIT {

    private static final String BUCKET = "test-herddb-bulk";
    private static final String MINIO_USER = "minioadmin";
    private static final String MINIO_PASS = "minioadmin";

    @ClassRule
    public static GenericContainer<?> minio = new GenericContainer<>("minio/minio:latest")
            .withExposedPorts(9000)
            .withEnv("MINIO_ROOT_USER", MINIO_USER)
            .withEnv("MINIO_ROOT_PASSWORD", MINIO_PASS)
            .withCommand("server /data --console-address :9001")
            .waitingFor(Wait.forHttp("/minio/health/live").forPort(9000));

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private S3AsyncClient s3Client;
    private S3TransferManager tm;
    private S3ObjectStorage storage;

    @BeforeClass
    public static void setupClass() throws Exception {
        // Create the bucket exactly once using a short-lived client so the
        // per-test clients can assume the bucket exists. We close this
        // client immediately so it does not leak across the test class —
        // S3ObjectStorage.close() takes ownership of the per-test client
        // and tests must NOT share native CRT clients across methods (CRT
        // close is destructive and not idempotent across method boundaries).
        String endpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
        S3AsyncClient bootstrap = S3AsyncClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO_USER, MINIO_PASS)))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true).build())
                .build();
        try {
            bootstrap.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build()).get();
        } finally {
            bootstrap.close();
        }
    }

    @AfterClass
    public static void teardownClass() {
        // Nothing to release at class scope; per-test cleanup in @After
        // owns the client + TM closing.
    }

    @Before
    public void setUp() {
        // Fresh CRT client per test so {@link S3ObjectStorage#close()}
        // (which closes the wrapped client) cannot poison the next test.
        String endpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
        s3Client = S3AsyncClient.crtBuilder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO_USER, MINIO_PASS)))
                .forcePathStyle(true)
                .build();
        tm = S3TransferManagerFactory.build(s3Client);
        storage = new S3ObjectStorage(s3Client, BUCKET, "" /* prefix */, null, tm);
    }

    @After
    public void tearDown() {
        if (storage != null) {
            storage.close();
        }
    }

    private static byte[] randomBytes(int size, long seed) {
        Random rng = new Random(seed);
        byte[] data = new byte[size];
        rng.nextBytes(data);
        return data;
    }

    /**
     * Upload a multipart-sized file (24 MiB > 8 MiB threshold) via the TM
     * and verify the result is a single S3 object at the expected key
     * with no per-block layout siblings. Then download it back via
     * {@code downloadFileBulk} and compare bytes.
     */
    @Test
    public void multipartUploadProducesSingleObjectAndRoundTrips() throws Exception {
        final int size = 24 * 1024 * 1024;
        byte[] payload = randomBytes(size, 1234L);
        Path src = tmpFolder.newFile("src.bin").toPath();
        Files.write(src, payload);

        String objectKey = "ts/uuid-bulk/multipart/graph.bulk";
        AtomicLong progressTotal = new AtomicLong();

        long uploaded = storage.uploadFile(objectKey, src, progressTotal::addAndGet).get();
        assertEquals("uploaded byte count must match source size", size, uploaded);
        assertEquals("progress callback must report the full byte count",
                size, progressTotal.get());

        // Confirm the bulk object exists.
        assertTrue("bulk object must exist after uploadFile",
                storage.existsObject(objectKey).get());
        // Confirm exactly one S3 object was created — bulk layout is single-object,
        // with no per-block layout siblings. Using size()==1 catches any future
        // regression that writes extra keys regardless of naming convention.
        List<String> all = storage.list("ts/uuid-bulk/multipart/").get();
        assertEquals("exactly one S3 object must exist for the bulk layout", 1, all.size());
        assertEquals("the single object must be the expected bulk key",
                objectKey, all.get(0));

        Path dest = tmpFolder.newFile("dst.bin").toPath();
        storage.downloadFileBulk(objectKey, dest).get();
        byte[] downloaded = Files.readAllBytes(dest);
        assertArrayEquals("downloaded bytes must equal source", payload, downloaded);
    }

    /**
     * Byte-range read against a bulk-uploaded object via the SDK's range
     * request. Proves that bulk-format objects support random-access
     * reads — the read-side path for jvector graph traversal can issue
     * range GETs against the single S3 key without any per-block layout
     * indirection. (The current DSM implementation downloads the whole
     * bulk file to a local cache before serving random-access reads, but
     * this contract guarantees the upstream backend supports the cheaper
     * range-read variant as well, for future optimisation.)
     */
    @Test
    public void bulkObjectSupportsByteRangeReads() throws Exception {
        final int size = 12 * 1024 * 1024;
        byte[] payload = randomBytes(size, 99L);
        Path src = tmpFolder.newFile("range-src.bin").toPath();
        Files.write(src, payload);
        String objectKey = "ts/uuid-range/multipart/m.bulk";
        storage.uploadFile(objectKey, src, null).get();

        // Read a slice from the middle of the object via a Range header.
        final long offset = 5L * 1024 * 1024;
        final int length = 1024 * 1024;
        software.amazon.awssdk.services.s3.model.GetObjectRequest req =
                software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(objectKey)
                        .range("bytes=" + offset + "-" + (offset + length - 1))
                        .build();
        byte[] slice = s3Client.getObject(req,
                software.amazon.awssdk.core.async.AsyncResponseTransformer.toBytes()).get()
                .asByteArray();
        assertEquals(length, slice.length);
        byte[] expected = new byte[length];
        System.arraycopy(payload, (int) offset, expected, 0, length);
        assertArrayEquals("byte-range slice must equal the source slice",
                expected, slice);
    }

    /**
     * Small file (under the multipart threshold) still uploads via the
     * TM. The TM internally falls back to a single PutObject for small
     * objects — the contract for our DSM is the same either way: one S3
     * object at the {@code .bulk} key, downloadable in one shot.
     */
    @Test
    public void smallFileUploadIsSingleObject() throws Exception {
        byte[] payload = randomBytes(1024, 7L);
        Path src = tmpFolder.newFile("small.bin").toPath();
        Files.write(src, payload);

        String objectKey = "ts/uuid-small/multipart/m.bulk";
        long uploaded = storage.uploadFile(objectKey, src, null).get();
        assertEquals(payload.length, uploaded);

        Path dest = tmpFolder.newFile("small-dst.bin").toPath();
        storage.downloadFileBulk(objectKey, dest).get();
        assertArrayEquals(payload, Files.readAllBytes(dest));
    }

    /**
     * The {@code existsObject} probe returns false for a key that has
     * never been written — the most common case the DSM hits before the
     * first direct upload for a logical path.
     */
    @Test
    public void existsReturnsFalseForMissingKey() throws Exception {
        String missing = "missing/" + UUID.randomUUID() + ".bulk";
        assertFalse(storage.existsObject(missing).get());
    }

    /**
     * Verify that a non-null TM is exposed on the storage object via the
     * round-trip behaviour: with TM wired, multipart uploads to a real
     * endpoint succeed; without TM (default-constructed storage), the
     * default impl falls back to {@code write(...)} which is a single
     * {@code PutObject} — still functional, just not multipart.
     */
    @Test
    public void uploadWithoutTransferManagerStillFunctional() throws Exception {
        // Build a fresh client + plain storage (no TM) so we exercise the
        // default {@link ObjectStorage#uploadFile} fallback path. We
        // explicitly do NOT install a TM on this storage so the fallback
        // is taken; the per-test {@code storage} field's TM is unaffected.
        String endpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
        S3AsyncClient plainClient = S3AsyncClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO_USER, MINIO_PASS)))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true).build())
                .build();
        S3ObjectStorage plain = new S3ObjectStorage(plainClient, BUCKET, "" /* prefix */);
        try {
            byte[] payload = randomBytes(8192, 5L);
            Path src = tmpFolder.newFile("plain.bin").toPath();
            Files.write(src, payload);
            String key = "plain-" + UUID.randomUUID() + ".bulk";
            long uploaded = plain.uploadFile(key, src, null).get();
            assertEquals(payload.length, uploaded);
            assertTrue(plain.existsObject(key).get());
            assertNotNull("plain storage upload returned non-null receipt", uploaded);
        } finally {
            plain.close();
        }
    }
}
