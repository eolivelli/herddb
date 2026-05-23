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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.lang.reflect.Proxy;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Issue #645: unit tests for {@link S3ObjectStorage#existsObject(String)}.
 *
 * <p>The existence probe is the decision-point that routes a multipart
 * file read either through the bulk-layout fast path (memory-mapped local
 * cache file) or through the legacy gRPC per-block path. Before issue
 * #645, every exception thrown by {@code S3AsyncClient.headObject} was
 * collapsed into a {@code false} answer — which silently misrouted reads
 * of direct-S3-uploaded objects through the gRPC file-server on restart
 * and produced the {@code Block not found} crash-loop in Run 9.
 *
 * <p>The new tri-state contract (see {@link ObjectStorage#existsObject}):
 *
 * <ul>
 *   <li>HEAD 200 → {@code true};</li>
 *   <li>{@link NoSuchKeyException} or generic {@link AwsServiceException}
 *       with HTTP status 404 → {@code false} (definitively absent);</li>
 *   <li>anything else (5xx, 403, throttling,
 *       {@link SdkClientException}, generic runtime errors,
 *       miscellaneous I/O errors) → completes exceptionally.</li>
 * </ul>
 *
 * <p>The tests use a reflective {@link Proxy} that intercepts
 * {@code headObject} and returns a configured future, mirroring the
 * pattern established by
 * {@code S3ObjectStorageDownloadToFileTest#proxyClient}. No real S3
 * client is required.
 */
public class S3ObjectStorageExistsObjectErrorTest {

    /**
     * HEAD 200 must map to {@code true}.
     */
    @Test
    public void presentObjectReturnsTrue() throws Exception {
        S3AsyncClient client = headObjectProxy(
                () -> CompletableFuture.completedFuture(
                        HeadObjectResponse.builder().contentLength(0L).build()));
        S3ObjectStorage storage = new S3ObjectStorage(client, "bucket", "prefix/");

        Boolean result = storage.existsObject("segment/graph.bulk").get();
        assertEquals(Boolean.TRUE, result);
    }

    /**
     * {@link NoSuchKeyException} — the canonical "object missing" surface
     * from the AWS SDK — must map to {@code false} (definitively absent).
     */
    @Test
    public void missingObjectViaNoSuchKeyReturnsFalse() throws Exception {
        NoSuchKeyException notFound = (NoSuchKeyException)
                NoSuchKeyException.builder().message("absent").build();
        S3AsyncClient client = headObjectProxy(
                () -> failedFuture(notFound));
        S3ObjectStorage storage = new S3ObjectStorage(client, "bucket", "prefix/");

        Boolean result = storage.existsObject("segment/graph.bulk").get();
        assertEquals(Boolean.FALSE, result);
    }

    /**
     * Some S3-compatible backends (CRT, MinIO under some configurations)
     * surface "not found" as a generic {@link AwsServiceException} with
     * HTTP status 404 rather than a typed {@link NoSuchKeyException}.
     * That branch must also map to {@code false}.
     */
    @Test
    public void missingObjectViaGenericServiceException404ReturnsFalse() throws Exception {
        AwsServiceException ex = AwsServiceException.builder()
                .message("404 from MinIO")
                .statusCode(404)
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("404")
                        .errorMessage("Not Found")
                        .serviceName("S3")
                        .build())
                .build();
        S3AsyncClient client = headObjectProxy(() -> failedFuture(ex));
        S3ObjectStorage storage = new S3ObjectStorage(client, "bucket", "prefix/");

        Boolean result = storage.existsObject("segment/graph.bulk").get();
        assertEquals(Boolean.FALSE, result);
    }

    /**
     * Issue #645 — regression test for the failure mode that produced
     * the bench crash-loop: a generic HTTP 5xx
     * ({@link AwsServiceException} with status != 404) must propagate as
     * an exceptional future completion. Before the fix this branch
     * returned {@code false} and silently misrouted the read through
     * gRPC.
     */
    @Test
    public void serverError503PropagatesAsException() throws Exception {
        AwsServiceException ex = AwsServiceException.builder()
                .message("503 Service Unavailable")
                .statusCode(503)
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("ServiceUnavailable")
                        .errorMessage("Service Unavailable")
                        .serviceName("S3")
                        .build())
                .build();
        S3AsyncClient client = headObjectProxy(() -> failedFuture(ex));
        S3ObjectStorage storage = new S3ObjectStorage(client, "bucket", "prefix/");

        try {
            storage.existsObject("segment/graph.bulk").get();
            fail("503 must propagate as exceptional completion");
        } catch (ExecutionException expected) {
            Throwable cause = expected.getCause();
            assertTrue("cause must mention 503 / ServiceUnavailable; got: " + cause,
                    cause.getMessage() != null
                            && (cause.getMessage().contains("503")
                                    || cause.getMessage().contains("Service Unavailable")));
        }
    }

    /**
     * HTTP 403 (Forbidden) is NOT a "missing object" answer — propagating
     * loudly lets the operator see and fix the permission problem
     * instead of silently degrading to the wrong read path.
     */
    @Test
    public void forbidden403PropagatesAsException() throws Exception {
        AwsServiceException ex = AwsServiceException.builder()
                .message("403 Forbidden")
                .statusCode(403)
                .build();
        S3AsyncClient client = headObjectProxy(() -> failedFuture(ex));
        S3ObjectStorage storage = new S3ObjectStorage(client, "bucket", "prefix/");

        try {
            storage.existsObject("segment/graph.bulk").get();
            fail("403 must propagate as exceptional completion");
        } catch (ExecutionException expected) {
            // expected
        }
    }

    /**
     * A network-level failure (DNS, connect timeout, socket close) is
     * surfaced as {@link SdkClientException} — a transient condition
     * that MUST propagate. This is the most common shape of the
     * "MinIO blip" that crash-looped the IS in Run 9.
     */
    @Test
    public void sdkClientExceptionPropagatesAsException() throws Exception {
        SdkClientException ex = SdkClientException.builder()
                .message("Unable to execute HTTP request: connect timed out")
                .cause(new SocketTimeoutException("connect timed out"))
                .build();
        S3AsyncClient client = headObjectProxy(() -> failedFuture(ex));
        S3ObjectStorage storage = new S3ObjectStorage(client, "bucket", "prefix/");

        try {
            storage.existsObject("segment/graph.bulk").get();
            fail("SdkClientException must propagate as exceptional completion");
        } catch (ExecutionException expected) {
            Throwable cause = expected.getCause();
            assertTrue("cause must mention the network failure; got: " + cause,
                    cause.getMessage() != null
                            && cause.getMessage().contains("connect timed out"));
        }
    }

    /**
     * Any other {@link RuntimeException} thrown by the SDK or by
     * unrelated downstream code (e.g. an interceptor) must also propagate
     * — never silently collapse to {@code false}.
     */
    @Test
    public void genericRuntimeExceptionPropagates() throws Exception {
        RuntimeException ex = new IllegalStateException(
                "unexpected interceptor failure");
        S3AsyncClient client = headObjectProxy(() -> failedFuture(ex));
        S3ObjectStorage storage = new S3ObjectStorage(client, "bucket", "prefix/");

        try {
            storage.existsObject("segment/graph.bulk").get();
            fail("RuntimeException must propagate as exceptional completion");
        } catch (ExecutionException expected) {
            Throwable cause = expected.getCause();
            assertTrue("cause must be the original IllegalStateException; got: " + cause,
                    cause instanceof IllegalStateException);
        }
    }

    // ------------------------------------------------------------------
    // Helpers — mirror the proxy pattern used by
    // S3ObjectStorageDownloadToFileTest.
    // ------------------------------------------------------------------

    private static <T> CompletableFuture<T> failedFuture(Throwable t) {
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(t);
        return f;
    }

    /**
     * Returns a reflective {@link S3AsyncClient} proxy whose
     * {@code headObject} method returns whatever future the supplied
     * factory produces. Any other invocation (apart from {@code close})
     * triggers {@link UnsupportedOperationException} so unexpected SDK
     * calls fail loudly.
     */
    @SuppressWarnings("unchecked")
    private static S3AsyncClient headObjectProxy(
            Supplier<CompletableFuture<HeadObjectResponse>> responseFactory) {
        return (S3AsyncClient) Proxy.newProxyInstance(
                S3AsyncClient.class.getClassLoader(),
                new Class<?>[]{S3AsyncClient.class},
                (proxy, method, args) -> {
                    if ("headObject".equals(method.getName()) && args != null
                            && args.length == 1
                            && args[0] instanceof HeadObjectRequest) {
                        return responseFactory.get();
                    }
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    throw new UnsupportedOperationException(
                            "fake S3 client does not implement " + method.getName());
                });
    }
}
