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
import static org.junit.Assert.assertTrue;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class S3ObjectStorageIT {

    private static final String BUCKET = "test-herddb";
    private static final String MINIO_USER = "minioadmin";
    private static final String MINIO_PASS = "minioadmin";

    @ClassRule
    public static GenericContainer<?> minio = new GenericContainer<>("minio/minio:latest")
            .withExposedPorts(9000)
            .withEnv("MINIO_ROOT_USER", MINIO_USER)
            .withEnv("MINIO_ROOT_PASSWORD", MINIO_PASS)
            .withCommand("server /data --console-address :9001")
            .waitingFor(Wait.forHttp("/minio/health/live").forPort(9000));

    private static S3AsyncClient s3Client;

    @BeforeClass
    public static void setupClass() throws Exception {
        String endpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
        s3Client = S3AsyncClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO_USER, MINIO_PASS)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build()).get();
    }

    @AfterClass
    public static void teardownClass() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    private S3ObjectStorage storage;

    @Before
    public void setUp() {
        String prefix = "test-" + UUID.randomUUID() + "/";
        storage = new S3ObjectStorage(s3Client, BUCKET, prefix);
    }

    @After
    public void tearDown() throws Exception {
        // Clean up test data but do NOT close shared s3Client
        storage.deleteByPrefix("").get();
    }

    @Test
    public void testWriteRead() throws Exception {
        byte[] data = "hello s3".getBytes();
        storage.write("ts1/uuid1/1.page", data).get();

        ReadResult result = storage.read("ts1/uuid1/1.page").get();
        assertEquals(ReadResult.Status.FOUND, result.status());
        assertArrayEquals(data, result.content());
    }

    @Test
    public void testReadMissing() throws Exception {
        ReadResult result = storage.read("nonexistent/key.page").get();
        assertEquals(ReadResult.Status.NOT_FOUND, result.status());
    }

    @Test
    public void testDelete() throws Exception {
        byte[] data = "to delete".getBytes();
        storage.write("del/1.page", data).get();

        // S3 delete always returns true (idempotent)
        assertTrue(storage.delete("del/1.page").get());

        ReadResult result = storage.read("del/1.page").get();
        assertEquals(ReadResult.Status.NOT_FOUND, result.status());
    }

    @Test
    public void testList() throws Exception {
        storage.write("ns1/a.page", "a".getBytes()).get();
        storage.write("ns1/b.page", "b".getBytes()).get();
        storage.write("ns2/c.page", "c".getBytes()).get();

        List<String> paths = storage.list("ns1/").get();
        assertEquals(2, paths.size());
        assertTrue(paths.stream().allMatch(p -> p.startsWith("ns1/")));
    }

    @Test
    public void testDeleteByPrefix() throws Exception {
        storage.write("pfx/a.page", "a".getBytes()).get();
        storage.write("pfx/b.page", "b".getBytes()).get();
        storage.write("other/c.page", "c".getBytes()).get();

        int deleted = storage.deleteByPrefix("pfx/").get();
        assertEquals(2, deleted);

        List<String> remaining = storage.list("").get();
        assertEquals(1, remaining.size());
        assertTrue(remaining.get(0).startsWith("other/"));
    }

    @Test
    public void testListPagination() throws Exception {
        int count = 1005;
        for (int i = 0; i < count; i++) {
            storage.write(String.format("page/%05d.page", i), ("data" + i).getBytes()).get();
        }
        List<String> all = storage.list("page/").get();
        assertEquals(count, all.size());
    }

    @Test
    public void testPrefixIsolation() throws Exception {
        String prefix1 = "ns1-" + UUID.randomUUID() + "/";
        String prefix2 = "ns2-" + UUID.randomUUID() + "/";
        S3ObjectStorage storage1 = new S3ObjectStorage(s3Client, BUCKET, prefix1);
        S3ObjectStorage storage2 = new S3ObjectStorage(s3Client, BUCKET, prefix2);

        storage1.write("shared/key.page", "from-ns1".getBytes()).get();
        storage2.write("shared/key.page", "from-ns2".getBytes()).get();

        List<String> list1 = storage1.list("").get();
        List<String> list2 = storage2.list("").get();

        assertEquals(1, list1.size());
        assertEquals(1, list2.size());

        // Each namespace only sees its own key
        assertFalse(list1.stream().anyMatch(k -> k.startsWith(prefix2)));
        assertFalse(list2.stream().anyMatch(k -> k.startsWith(prefix1)));

        storage1.deleteByPrefix("").get();
        storage2.deleteByPrefix("").get();
    }
}
