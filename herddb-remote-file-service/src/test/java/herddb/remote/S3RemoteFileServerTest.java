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

package herddb.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class S3RemoteFileServerTest {

    private static final String BUCKET = "herddb-it";
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
    public TemporaryFolder folder = new TemporaryFolder();

    private String minioEndpoint;
    private String testPrefix;

    @Before
    public void setUp() throws Exception {
        minioEndpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
        testPrefix = "it-" + UUID.randomUUID() + "/";

        // Create bucket once per test (idempotent via try/catch)
        try (S3AsyncClient client = buildS3Client()) {
            client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build()).get();
        } catch (Exception e) {
            // Bucket may already exist from a previous test
        }
    }

    @After
    public void tearDown() throws Exception {
        // Clean up test data
        try (S3AsyncClient client = buildS3Client()) {
            herddb.remote.storage.S3ObjectStorage cleanup =
                    new herddb.remote.storage.S3ObjectStorage(client, BUCKET, testPrefix);
            cleanup.deleteByPrefix("").get();
        }
    }

    private S3AsyncClient buildS3Client() {
        return S3AsyncClient.builder()
                .endpointOverride(URI.create(minioEndpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO_USER, MINIO_PASS)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                // Use AWS CRT async HTTP client for better stability and non-blocking shutdown
                .httpClientBuilder(AwsCrtAsyncHttpClient.builder())
                .build();
    }

    private Properties buildConfig(Path dataDir) {
        Properties config = new Properties();
        config.setProperty("storage.mode", "s3");
        config.setProperty("s3.endpoint", minioEndpoint);
        config.setProperty("s3.bucket", BUCKET);
        config.setProperty("s3.region", "us-east-1");
        config.setProperty("s3.access.key", MINIO_USER);
        config.setProperty("s3.secret.key", MINIO_PASS);
        config.setProperty("s3.prefix", testPrefix);
        config.setProperty("cache.dir", dataDir.resolve("s3-cache").toString());
        config.setProperty("cache.max.bytes", String.valueOf(512 * 1024 * 1024));
        return config;
    }

    @Test
    public void testGrpcRoundTrip() throws Exception {
        Path dataDir = folder.newFolder("data1").toPath();
        Properties config = buildConfig(dataDir);

        try (RemoteFileServer server = new RemoteFileServer("0.0.0.0", 0, dataDir, 4, config)) {
            server.start();
            try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                    List.of("localhost:" + server.getPort()))) {

                byte[] data = "grpc round trip".getBytes(StandardCharsets.UTF_8);
                client.writeFileAsync("ts1/1.page", data).get();

                byte[] read = client.readFileAsync("ts1/1.page").get();
                assertNotNull(read);
                assertEquals("grpc round trip", new String(read, StandardCharsets.UTF_8));

                assertTrue(client.deleteFileAsync("ts1/1.page").get());
                assertNull(client.readFileAsync("ts1/1.page").get());

                client.writeFileAsync("list/a.page", "a".getBytes()).get();
                client.writeFileAsync("list/b.page", "b".getBytes()).get();
                List<String> listed = client.listFilesAsync("list/").get();
                assertEquals(2, listed.size());

                int deleted = client.deleteByPrefixAsync("list/").get();
                assertEquals(2, deleted);
            }
        }
    }

    @Test
    public void testCacheClearedOnRestart() throws Exception {
        Path dataDir = folder.newFolder("data2").toPath();
        Properties config = buildConfig(dataDir);

        byte[] data = "persistent in s3".getBytes(StandardCharsets.UTF_8);

        // Write via first server instance
        try (RemoteFileServer server = new RemoteFileServer("0.0.0.0", 0, dataDir, 4, config)) {
            server.start();
            try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                    List.of("localhost:" + server.getPort()))) {
                client.writeFileAsync("persist/1.page", data).get();
            }
        }

        // Restart server (clears local cache), read via gRPC: data should come from S3
        try (RemoteFileServer server = new RemoteFileServer("0.0.0.0", 0, dataDir, 4, config)) {
            server.start();
            try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                    List.of("localhost:" + server.getPort()))) {
                byte[] read = client.readFileAsync("persist/1.page").get();
                assertNotNull("data should be re-fetched from S3 after restart", read);
                assertEquals("persistent in s3", new String(read, StandardCharsets.UTF_8));
            }
        }
    }

    @Test(expected = IOException.class)
    public void testFailFastOnNonexistentBucket() throws Exception {
        Path dataDir = folder.newFolder("data3").toPath();
        Properties config = buildConfig(dataDir);
        config.setProperty("s3.bucket", "nonexistent-bucket-" + UUID.randomUUID());
        // Set a short timeout for testing nonexistent bucket
        config.setProperty("s3.bucket.wait.timeout.ms", "500");

        RemoteFileServer server = new RemoteFileServer("0.0.0.0", 0, dataDir, 4, config);
        server.start(); // should throw IOException
    }
}
