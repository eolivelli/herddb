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
import static org.junit.Assert.assertTrue;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Unit tests for {@link S3ObjectStorage#downloadToFile(String, Path, boolean)}.
 *
 * <p>Verifies that the streaming download path correctly creates the destination
 * file for block 0 (append=false) and appends subsequent blocks (append=true),
 * producing a byte-for-byte identical result to what the S3 object contained.
 *
 * <p>Uses a reflective proxy that intercepts {@code getObject} and writes
 * a fixed payload via the {@link AsyncResponseTransformer} so no real S3
 * server is required.
 */
public class S3ObjectStorageDownloadToFileTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final byte[] BLOCK_0 = {1, 2, 3, 4, 5};
    private static final byte[] BLOCK_1 = {6, 7, 8, 9, 10};

    /**
     * Block 0 with append=false: destination file is created (or replaced) and
     * contains exactly the bytes returned by S3.
     */
    @Test
    public void downloadToFileCreatesFileForFirstBlock() throws Exception {
        S3AsyncClient client = proxyClient(BLOCK_0);
        S3ObjectStorage storage = new S3ObjectStorage(client, "bucket", "prefix/");

        Path dest = folder.newFile("block0.bin").toPath();
        storage.downloadToFile("segment/graph.multipart/0", dest, false).get();

        byte[] written = Files.readAllBytes(dest);
        assertArrayEquals(BLOCK_0, written);
    }

    /**
     * Block 1 with append=true: content is appended to block 0 so the file
     * contains exactly BLOCK_0 + BLOCK_1 concatenated.
     */
    @Test
    public void downloadToFileAppendsSubsequentBlocks() throws Exception {
        // Write block 0 first.
        S3AsyncClient client0 = proxyClient(BLOCK_0);
        S3ObjectStorage storage0 = new S3ObjectStorage(client0, "bucket", "prefix/");
        Path dest = folder.newFile("multipart.bin").toPath();
        storage0.downloadToFile("segment/graph.multipart/0", dest, false).get();

        // Append block 1.
        S3AsyncClient client1 = proxyClient(BLOCK_1);
        S3ObjectStorage storage1 = new S3ObjectStorage(client1, "bucket", "prefix/");
        storage1.downloadToFile("segment/graph.multipart/1", dest, true).get();

        byte[] written = Files.readAllBytes(dest);
        assertEquals(BLOCK_0.length + BLOCK_1.length, written.length);
        // First half = BLOCK_0, second half = BLOCK_1
        for (int i = 0; i < BLOCK_0.length; i++) {
            assertEquals("mismatch at block-0 position " + i, BLOCK_0[i], written[i]);
        }
        for (int i = 0; i < BLOCK_1.length; i++) {
            assertEquals("mismatch at block-1 position " + i, BLOCK_1[i],
                    written[BLOCK_0.length + i]);
        }
    }

    /**
     * append=false on an already-existing file replaces the content (create-or-replace
     * semantics), ensuring that a retry of block 0 produces a correct file.
     */
    @Test
    public void downloadToFileReplacesExistingFileWhenAppendFalse() throws Exception {
        // Pre-populate the file with stale bytes.
        Path dest = folder.newFile("stale.bin").toPath();
        Files.write(dest, new byte[]{99, 99, 99, 99, 99, 99, 99, 99});

        S3AsyncClient client = proxyClient(BLOCK_0);
        S3ObjectStorage storage = new S3ObjectStorage(client, "bucket", "prefix/");
        storage.downloadToFile("segment/graph.multipart/0", dest, false).get();

        byte[] written = Files.readAllBytes(dest);
        assertArrayEquals("stale content must be replaced by block 0", BLOCK_0, written);
    }

    /**
     * A mid-stream S3 error (e.g. {@code NoSuchKeyException}) must complete the
     * returned future exceptionally. When the error occurs with {@code append=true},
     * any pre-existing content in the destination file must remain intact (the SDK's
     * {@code FileTransformerConfiguration.defaultCreateOrAppend()} uses
     * {@code FailureBehavior.LEAVE}, so the partial-append behaviour is
     * backend-determined; at minimum the file must not be deleted).
     */
    @Test
    public void downloadToFilePropagatesS3Failure() throws Exception {
        byte[] existing = {77, 88, 99};
        Path dest = folder.newFile("existing.bin").toPath();
        Files.write(dest, existing);

        S3AsyncClient failingClient = failingProxyClient(
                NoSuchKeyException.builder().message("simulated not found").build());
        S3ObjectStorage storage = new S3ObjectStorage(failingClient, "bucket", "prefix/");

        CompletableFuture<Void> future = storage.downloadToFile(
                "segment/graph.multipart/1", dest, true);

        ExecutionException ex = null;
        try {
            future.get();
        } catch (ExecutionException e) {
            ex = e;
        }

        assertTrue("future must complete exceptionally on S3 failure", ex != null);
        // The existing file must not be deleted.
        assertTrue("destination file must still exist after S3 error", Files.exists(dest));
    }

    /**
     * Issue #636 — deterministic regression test for the
     * {@link #singleBufferPublisher} idempotency contract that the other tests
     * in this file rely on. Drives a hand-rolled {@link Subscriber} that
     * synchronously calls {@code request(1)} three times inside its
     * {@code onSubscribe}, then asserts the payload is delivered exactly once.
     *
     * <p>Prior to the fix, {@code onNext} was called three times — which is
     * what the SDK's {@code AsyncResponseTransformer.toFile()} occasionally
     * triggered during {@link #downloadToFileAppendsSubsequentBlocks} (one
     * extra request → one extra block written → file length 15 instead of 10).
     */
    @Test
    public void requestIsIdempotentUnderMultipleRequestCalls() {
        byte[] payload = {1, 2, 3, 4, 5};
        AtomicInteger onNextCalls = new AtomicInteger();
        AtomicInteger onCompleteCalls = new AtomicInteger();
        AtomicInteger onErrorCalls = new AtomicInteger();
        List<byte[]> received = new ArrayList<>();

        Subscriber<ByteBuffer> subscriber = new Subscriber<ByteBuffer>() {
            @Override
            public void onSubscribe(Subscription s) {
                // Three back-to-back request(n) calls — the helper must
                // deliver the payload at most once. This is what the SDK's
                // file transformer can do internally; we model that worst
                // case explicitly here so the regression cannot recur.
                s.request(1);
                s.request(1);
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer b) {
                onNextCalls.incrementAndGet();
                byte[] copy = new byte[b.remaining()];
                b.get(copy);
                received.add(copy);
            }

            @Override
            public void onError(Throwable t) {
                onErrorCalls.incrementAndGet();
            }

            @Override
            public void onComplete() {
                onCompleteCalls.incrementAndGet();
            }
        };

        singleBufferPublisher(payload).subscribe(subscriber);

        assertEquals("onNext must fire exactly once regardless of request count",
                1, onNextCalls.get());
        assertEquals("onComplete must fire exactly once", 1, onCompleteCalls.get());
        assertEquals("onError must not fire on the happy path",
                0, onErrorCalls.get());
        assertEquals("exactly one buffer must have been delivered",
                1, received.size());
        assertArrayEquals("delivered bytes must match the payload",
                payload, received.get(0));
    }

    /**
     * Issue #636 — cancel must also flip the terminal flag, so a
     * {@code request(n)} that arrives after {@code cancel()} is a no-op.
     * Pins the post-cancel-state path of the Reactive Streams contract.
     */
    @Test
    public void requestAfterCancelIsNoOp() {
        byte[] payload = {1, 2, 3};
        AtomicInteger onNextCalls = new AtomicInteger();
        AtomicInteger onCompleteCalls = new AtomicInteger();

        Subscriber<ByteBuffer> subscriber = new Subscriber<ByteBuffer>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.cancel();
                s.request(1);
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer b) {
                onNextCalls.incrementAndGet();
            }

            @Override
            public void onError(Throwable t) {
                // No-op for this test.
            }

            @Override
            public void onComplete() {
                onCompleteCalls.incrementAndGet();
            }
        };

        singleBufferPublisher(payload).subscribe(subscriber);

        assertEquals("onNext must not fire after cancel", 0, onNextCalls.get());
        assertEquals("onComplete must not fire after cancel",
                0, onCompleteCalls.get());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Returns a reflective {@link S3AsyncClient} proxy whose {@code getObject}
     * method writes {@code payload} to the destination file via the provided
     * {@link AsyncResponseTransformer}, exactly as the real SDK does.
     */
    @SuppressWarnings("unchecked")
    private static S3AsyncClient proxyClient(byte[] payload) {
        return (S3AsyncClient) Proxy.newProxyInstance(
                S3AsyncClient.class.getClassLoader(),
                new Class<?>[]{S3AsyncClient.class},
                (proxy, method, args) -> {
                    if ("getObject".equals(method.getName()) && args != null && args.length == 2
                            && args[0] instanceof GetObjectRequest
                            && args[1] instanceof AsyncResponseTransformer) {
                        AsyncResponseTransformer<GetObjectResponse, ?> transformer =
                                (AsyncResponseTransformer<GetObjectResponse, ?>) args[1];
                        // Simulate how the real SDK feeds data into the transformer.
                        CompletableFuture<Object> future =
                                (CompletableFuture<Object>) transformer.prepare();
                        transformer.onResponse(GetObjectResponse.builder().build());
                        transformer.onStream(singleBufferPublisher(payload));
                        return future;
                    }
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    throw new UnsupportedOperationException(
                            "fake S3 client does not implement " + method.getName());
                });
    }

    /**
     * Returns an {@link SdkPublisher} that delivers {@code payload} as a single
     * {@link ByteBuffer} element and then completes — once and only once,
     * regardless of how many times the subscriber calls
     * {@link Subscription#request(long)}.
     *
     * <p>This is the issue #636 fix: previously the inline {@code Subscription}
     * inside {@link #proxyClient} re-delivered the payload on every
     * {@code request(n)} call. The SDK's {@code AsyncResponseTransformer.toFile()}
     * is free to issue {@code request} more than once per subscription (especially
     * in the append-mode {@code FileTransformerConfiguration.defaultCreateOrAppend()}
     * path), so any second request silently doubled the bytes written to disk
     * and produced the {@code expected:&lt;10&gt; but was:&lt;15&gt;} flake in
     * {@link #downloadToFileAppendsSubsequentBlocks}.
     *
     * <p>The Reactive Streams contract requires a publisher to be a state
     * machine: after {@code onComplete} / {@code onError} / {@code cancel},
     * further {@code request} calls must be no-ops. The
     * {@link AtomicBoolean#compareAndSet} below pins exactly that contract.
     * Kept as a single-buffer-only helper — any future test that needs
     * multi-chunk delivery should introduce its own helper rather than reuse
     * this flag.
     */
    static SdkPublisher<ByteBuffer> singleBufferPublisher(byte[] payload) {
        return subscriber -> {
            AtomicBoolean delivered = new AtomicBoolean(false);
            subscriber.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    if (delivered.compareAndSet(false, true)) {
                        subscriber.onNext(ByteBuffer.wrap(payload));
                        subscriber.onComplete();
                    }
                }

                @Override
                public void cancel() {
                    // Idempotent terminal — flip the flag so any post-cancel
                    // request(n) is also a no-op, matching the spec.
                    delivered.set(true);
                }
            });
        };
    }

    /**
     * Returns a reflective {@link S3AsyncClient} proxy whose {@code getObject}
     * method signals a failure via {@link AsyncResponseTransformer#exceptionOccurred(Throwable)},
     * simulating a mid-stream S3 error such as {@link NoSuchKeyException}.
     */
    @SuppressWarnings("unchecked")
    private static S3AsyncClient failingProxyClient(Throwable error) {
        return (S3AsyncClient) Proxy.newProxyInstance(
                S3AsyncClient.class.getClassLoader(),
                new Class<?>[]{S3AsyncClient.class},
                (proxy, method, args) -> {
                    if ("getObject".equals(method.getName()) && args != null && args.length == 2
                            && args[0] instanceof GetObjectRequest
                            && args[1] instanceof AsyncResponseTransformer) {
                        AsyncResponseTransformer<GetObjectResponse, ?> transformer =
                                (AsyncResponseTransformer<GetObjectResponse, ?>) args[1];
                        CompletableFuture<Object> future =
                                (CompletableFuture<Object>) transformer.prepare();
                        // Signal error before any data arrives — simulates NoSuchKey.
                        transformer.exceptionOccurred(error);
                        return future;
                    }
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    throw new UnsupportedOperationException(
                            "fake S3 client does not implement " + method.getName());
                });
    }
}
