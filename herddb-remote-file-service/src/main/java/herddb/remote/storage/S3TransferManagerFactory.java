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

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * Builds an {@link S3TransferManager} that reuses an existing CRT-backed
 * {@link S3AsyncClient}. The Transfer Manager is the AWS SDK v2 canonical
 * high-throughput bulk-transfer API: it drives real S3 Multipart Upload
 * with parallel parts pipelined by the CRT HTTP client.
 *
 * <p>Issue #638: used by the indexing service to upload Phase B checkpoint /
 * compaction segment files directly to S3 or MinIO, bypassing the gRPC
 * file-server hop. Reads use the same underlying CRT pool as the existing
 * direct-S3 read path (issue #381).
 *
 * <p>This is a thin factory because we deliberately want the indexing
 * service (or any caller) to own the underlying {@link S3AsyncClient}
 * lifecycle. The TM does <em>not</em> close the wrapped client when
 * {@link S3TransferManager#close()} is called as long as it was built via
 * {@code .s3Client(...)} — that contract is exercised by
 * {@code S3TransferManagerFactoryTest} so the {@link S3AsyncClient} can be
 * closed exactly once by its owner.
 */
public final class S3TransferManagerFactory {

    private S3TransferManagerFactory() {
        // utility class
    }

    /**
     * Builds an {@link S3TransferManager} backed by {@code crtClient}.
     *
     * @param crtClient CRT-backed {@link S3AsyncClient} already configured
     *                  with credentials, endpoint, and region. Must not be
     *                  {@code null}. Its lifecycle remains owned by the
     *                  caller — closing the returned TM does not close the
     *                  client.
     * @return a ready-to-use {@link S3TransferManager}.
     */
    public static S3TransferManager build(S3AsyncClient crtClient) {
        if (crtClient == null) {
            throw new IllegalArgumentException("crtClient must not be null");
        }
        return S3TransferManager.builder()
                .s3Client(crtClient)
                .build();
    }
}
