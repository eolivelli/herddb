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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import java.util.LinkedHashSet;

/**
 * S3-backed implementation of {@link ObjectStorage} using the AWS SDK v2 async client.
 * Compatible with MinIO (forcePathStyle must be enabled on the client).
 *
 * @author enrico.olivelli
 */
public class S3ObjectStorage implements ObjectStorage {

    private static final Logger LOGGER = Logger.getLogger(S3ObjectStorage.class.getName());
    private static final int MAX_BATCH_SIZE = 1000;

    private final S3AsyncClient client;
    private final String bucket;
    private final String keyPrefix;

    public S3ObjectStorage(S3AsyncClient client, String bucket, String prefix) {
        this.client = client;
        this.bucket = bucket;
        this.keyPrefix = prefix == null ? "" : prefix;
    }

    private String toKey(String path) {
        return keyPrefix + path;
    }

    private String fromKey(String key) {
        return key.substring(keyPrefix.length());
    }

    @Override
    public CompletableFuture<Void> write(String path, byte[] content) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(toKey(path))
                .build();
        return client.putObject(request, AsyncRequestBody.fromBytes(content))
                .thenApply(resp -> (Void) null);
    }

    @Override
    public CompletableFuture<ReadResult> read(String path) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(toKey(path))
                .build();
        return client.getObject(request, AsyncResponseTransformer.toBytes())
                .thenApply(response -> ReadResult.found(response.asByteArray()))
                .exceptionally(t -> {
                    Throwable cause = (t instanceof CompletionException) ? t.getCause() : t;
                    if (cause instanceof NoSuchKeyException) {
                        return ReadResult.notFound();
                    }
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new RuntimeException(cause);
                });
    }

    @Override
    public CompletableFuture<Void> writeBlock(String path, long blockIndex, byte[] content) {
        String blockKey = toKey(path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex);
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(blockKey)
                .build();
        return client.putObject(request, AsyncRequestBody.fromBytes(content))
                .thenApply(resp -> (Void) null);
    }

    @Override
    public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
        long blockIndex = offset / blockSize;
        int offsetInBlock = (int) (offset % blockSize);
        // Each block is a separate S3 object starting at byte 0.
        // Download the whole block object and return the requested slice.
        return read(path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex)
                .thenApply(result -> {
                    if (result.status() == ReadResult.Status.NOT_FOUND) {
                        return ReadResult.notFound();
                    }
                    byte[] blockBytes = result.content();
                    int from = offsetInBlock;
                    int to = Math.min(from + length, blockBytes.length);
                    if (from >= blockBytes.length) {
                        return ReadResult.notFound();
                    }
                    byte[] slice = new byte[to - from];
                    System.arraycopy(blockBytes, from, slice, 0, slice.length);
                    return ReadResult.found(slice);
                });
    }

    @Override
    public CompletableFuture<Boolean> deleteLogical(String path) {
        // Delete multipart blocks and the single-part file in parallel
        String multipartPrefix = path + ObjectStorage.MULTIPART_SUFFIX + "/";
        CompletableFuture<Integer> deletedBlocks = deleteByPrefix(multipartPrefix);
        CompletableFuture<Boolean> deletedSingle = delete(path);
        return deletedBlocks.thenCombine(deletedSingle, (blocks, single) -> blocks > 0 || single);
    }

    @Override
    public CompletableFuture<List<String>> listLogical(String prefix) {
        return list(prefix).thenApply(paths -> {
            LinkedHashSet<String> logical = new LinkedHashSet<>();
            for (String p : paths) {
                int mpIdx = p.indexOf(ObjectStorage.MULTIPART_SUFFIX + "/");
                if (mpIdx >= 0) {
                    String logicalPath = p.substring(0, mpIdx);
                    if (logicalPath.startsWith(prefix)) {
                        logical.add(logicalPath);
                    }
                } else {
                    logical.add(p);
                }
            }
            return new ArrayList<>(logical);
        });
    }

    @Override
    public CompletableFuture<Boolean> delete(String path) {
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(toKey(path))
                .build();
        // S3 DELETE is idempotent; always returns true
        return client.deleteObject(request).thenApply(resp -> Boolean.TRUE);
    }

    @Override
    public CompletableFuture<List<String>> list(String prefix) {
        List<String> results = new ArrayList<>();
        return listPage(prefix, null, results);
    }

    private CompletableFuture<List<String>> listPage(String prefix, String continuationToken, List<String> results) {
        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(keyPrefix + prefix);
        if (continuationToken != null) {
            builder.continuationToken(continuationToken);
        }
        return client.listObjectsV2(builder.build()).thenCompose(resp -> {
            for (S3Object obj : resp.contents()) {
                results.add(fromKey(obj.key()));
            }
            if (Boolean.TRUE.equals(resp.isTruncated())) {
                return listPage(prefix, resp.nextContinuationToken(), results);
            }
            return CompletableFuture.completedFuture(results);
        });
    }

    @Override
    public CompletableFuture<Integer> deleteByPrefix(String prefix) {
        return list(prefix).thenCompose(paths -> {
            if (paths.isEmpty()) {
                return CompletableFuture.completedFuture(0);
            }
            List<String> fullKeys = paths.stream()
                    .map(this::toKey)
                    .collect(Collectors.toList());
            int[] totalDeleted = {0};
            return deleteBatches(fullKeys, 0, totalDeleted);
        });
    }

    private CompletableFuture<Integer> deleteBatches(List<String> keys, int offset, int[] totalDeleted) {
        if (offset >= keys.size()) {
            return CompletableFuture.completedFuture(totalDeleted[0]);
        }
        int end = Math.min(offset + MAX_BATCH_SIZE, keys.size());
        List<ObjectIdentifier> identifiers = keys.subList(offset, end).stream()
                .map(k -> ObjectIdentifier.builder().key(k).build())
                .collect(Collectors.toList());
        DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(Delete.builder().objects(identifiers).build())
                .build();
        return client.deleteObjects(request).thenCompose(resp -> {
            totalDeleted[0] += resp.deleted().size();
            return deleteBatches(keys, end, totalDeleted);
        });
    }

    @Override
    public void close() {
        client.close();
    }
}
