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

import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;

/**
 * Central factory for the AWS CRT async HTTP client used by all HerdDB
 * services that access S3/GCS (file server, indexing service, index optimizer).
 *
 * <p>Keeps the configuration property names and defaults in a single place so
 * every service uses the same key in its own properties file:
 * <ul>
 *   <li>{@value #PROPERTY_CRT_MAX_CONCURRENCY} — maximum concurrent connections
 *       (default {@value #PROPERTY_CRT_MAX_CONCURRENCY_DEFAULT})</li>
 *   <li>{@value #PROPERTY_CRT_READ_BUFFER_SIZE} — per-connection native read-buffer
 *       size in bytes (default 1 GiB)</li>
 * </ul>
 *
 * <p>Native memory tracked by {@code CRT.nativeMemory()} requires
 * {@code -Daws.crt.memory.tracing=1} to be set <em>before</em> the CRT library
 * static initialiser runs. {@code setenv.sh} injects this flag by default via
 * {@code AWS_CRT_MEMORY_TRACING_OPTS} so no Java code needs to call
 * {@code System.setProperty}.
 *
 * @see <a href="https://github.com/eolivelli/herddb/issues/612">issue #612</a>
 */
public final class CrtS3HttpClientFactory {

    private static final Logger LOGGER = Logger.getLogger(CrtS3HttpClientFactory.class.getName());

    // -------------------------------------------------------------------------
    // Shared configuration property names (used identically in every service)
    // -------------------------------------------------------------------------

    /**
     * Maximum number of concurrent S3 connections the
     * {@code AwsCrtAsyncHttpClient} may keep open.
     *
     * <p>Property key: {@code "s3.crt.max.concurrency"}.
     * Default: {@value #PROPERTY_CRT_MAX_CONCURRENCY_DEFAULT}.
     *
     * <p>Lowering this from the CRT default (50) to 4 caps native memory
     * consumed by in-flight downloads during large index compactions. Each
     * connection buffers up to {@link #PROPERTY_CRT_READ_BUFFER_SIZE} bytes
     * in native memory, so the worst-case footprint is
     * {@code maxConcurrency × readBufferSize}.
     */
    public static final String PROPERTY_CRT_MAX_CONCURRENCY = "s3.crt.max.concurrency";
    public static final int PROPERTY_CRT_MAX_CONCURRENCY_DEFAULT = 4;

    /**
     * Per-connection native read-buffer size in bytes for the
     * {@code AwsCrtAsyncHttpClient}.
     *
     * <p>Property key: {@code "s3.crt.read.buffer.size"}.
     * Default: 1 GiB ({@value #PROPERTY_CRT_READ_BUFFER_SIZE_DEFAULT} bytes).
     *
     * <p>With the default concurrency of 4 this bounds CRT native memory to
     * 4 GiB worst-case during a compaction that simultaneously downloads 4
     * large segment files.
     */
    public static final String PROPERTY_CRT_READ_BUFFER_SIZE = "s3.crt.read.buffer.size";
    public static final long PROPERTY_CRT_READ_BUFFER_SIZE_DEFAULT = 1024L * 1024 * 1024;

    // -------------------------------------------------------------------------
    // Factory method
    // -------------------------------------------------------------------------

    /**
     * Returns a pre-configured {@link AwsCrtAsyncHttpClient.Builder} with the
     * supplied concurrency and read-buffer settings applied. The builder is
     * returned (not built) so the caller can attach it to an
     * {@code S3AsyncClient.Builder} via
     * {@code s3Builder.httpClientBuilder(...)}.
     *
     * @param maxConcurrency   maximum number of concurrent connections
     * @param readBufferSize   per-connection native read-buffer size in bytes
     * @return configured, un-built {@link AwsCrtAsyncHttpClient.Builder}
     */
    public static AwsCrtAsyncHttpClient.Builder builder(int maxConcurrency, long readBufferSize) {
        LOGGER.log(Level.INFO,
                "CRT HTTP client: maxConcurrency={0}, readBufferSizeInBytes={1}",
                new Object[]{maxConcurrency, readBufferSize});
        return AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(maxConcurrency)
                .readBufferSizeInBytes(readBufferSize);
    }

    private CrtS3HttpClientFactory() {
        // utility class — no instances
    }
}
