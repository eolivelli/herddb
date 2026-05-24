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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Unit tests for {@link S3ObjectStorage} that cover the GCS S3-compatibility quirk where
 * DELETE on a missing key returns {@code 404 NoSuchKey} instead of succeeding silently.
 */
public class S3ObjectStorageTest {

    /**
     * Regression test for issue #199. GCS reports 404 when deleting an object that is not
     * present (native S3 is silent). The storage layer must treat this as the desired
     * end-state and return successfully, otherwise the higher-level retry wrapper in
     * {@code RemoteFileServiceClient} will retry up to 10× with exponential back-off and
     * stall post-checkpoint cleanup for ~11 minutes per file.
     */
    @Test
    public void deleteReturnsTrueWhenS3ReturnsNoSuchKey() throws Exception {
        S3AsyncClient client = fakeClient(req -> CompletableFuture.failedFuture(
                NoSuchKeyException.builder().message("The specified key does not exist.").build()));
        S3ObjectStorage storage = new S3ObjectStorage(client, "bucket", "prefix/");

        Boolean result = storage.delete("missing.page").get();

        assertTrue(result);
    }

    /**
     * Non-404 errors must still propagate so the generic retry wrapper can handle
     * transient failures. This guards against accidentally widening the catch.
     */
    @Test
    public void deletePropagatesNonNoSuchKeyFailure() throws Exception {
        S3Exception failure = (S3Exception) S3Exception.builder()
                .statusCode(503).message("slow down").build();
        S3AsyncClient client = fakeClient(req -> CompletableFuture.failedFuture(failure));
        S3ObjectStorage storage = new S3ObjectStorage(client, "bucket", "prefix/");

        try {
            storage.delete("transient.page").get();
            fail("expected S3Exception to propagate");
        } catch (ExecutionException e) {
            assertSame(failure, e.getCause());
        }
    }

    /**
     * Single-object layout: delete() on a missing key on GCS returns 404 NoSuchKey;
     * the future must still complete successfully (idempotent).
     */
    @Test
    public void deleteSwallowsNoSuchKeyFromSingleObjectDelete() throws Exception {
        S3AsyncClient client = (S3AsyncClient) Proxy.newProxyInstance(
                S3AsyncClient.class.getClassLoader(),
                new Class<?>[] {S3AsyncClient.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "deleteObject":
                            return CompletableFuture.failedFuture(
                                    NoSuchKeyException.builder().message("missing").build());
                        case "listObjectsV2":
                            return CompletableFuture.completedFuture(
                                    ListObjectsV2Response.builder()
                                            .contents(Collections.emptyList())
                                            .isTruncated(false)
                                            .build());
                        case "close":
                            return null;
                        default:
                            throw new UnsupportedOperationException(
                                    "fake client does not implement " + method.getName());
                    }
                });
        S3ObjectStorage storage = new S3ObjectStorage(client, "bucket", "prefix/");

        Boolean deleted = storage.delete("ghost.page").get();

        // delete() returns TRUE on 404 to stay consistent with native S3 (idempotent).
        assertEquals(Boolean.TRUE, deleted);
    }

    private static S3AsyncClient fakeClient(Function<DeleteObjectRequest, CompletableFuture<DeleteObjectResponse>> deleteHandler) {
        InvocationHandler handler = (proxy, method, args) -> {
            if ("deleteObject".equals(method.getName()) && args != null && args.length == 1
                    && args[0] instanceof DeleteObjectRequest) {
                return deleteHandler.apply((DeleteObjectRequest) args[0]);
            }
            if ("listObjectsV2".equals(method.getName()) && args != null && args.length == 1
                    && args[0] instanceof ListObjectsV2Request) {
                return CompletableFuture.completedFuture(
                        ListObjectsV2Response.builder()
                                .contents(Collections.emptyList())
                                .isTruncated(false)
                                .build());
            }
            if ("close".equals(method.getName())) {
                return null;
            }
            throw new UnsupportedOperationException(
                    "fake client does not implement " + method.getName());
        };
        return (S3AsyncClient) Proxy.newProxyInstance(
                S3AsyncClient.class.getClassLoader(),
                new Class<?>[] {S3AsyncClient.class},
                handler);
    }
}
