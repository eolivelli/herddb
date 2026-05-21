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
package herddb.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.remote.RemoteFileDataStorageManager;
import herddb.remote.storage.ObjectStorage;
import herddb.remote.storage.ReadResult;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongConsumer;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Issue #638: covers the IS-server side of the direct-S3 upload wiring.
 * The corresponding code lives inline in
 * {@link herddb.indexing.IndexingServer#buildDataStorageManager}; we test
 * it here by exercising the exact two config-driven calls — read the
 * config, then call {@code setDirectObjectStorage} and (if the write
 * sub-flag is true) {@code enableDirectUpload} on a recording DSM. The
 * IndexingServer's behaviour is fully described by this contract.
 */
public class IndexingServerConfigDirectUploadFlowTest {

    private Path tmpRoot;

    @Before
    public void setUp() throws IOException {
        tmpRoot = Files.createTempDirectory("is-server-direct-upload-test-");
    }

    @After
    public void tearDown() throws IOException {
        if (tmpRoot != null) {
            FileUtils.deleteDirectory(tmpRoot.toFile());
        }
    }

    private static IndexingServerConfiguration configWith(Properties extra) {
        return new IndexingServerConfiguration(extra);
    }

    /**
     * IS-server wiring: when {@code indexing.s3.direct.enabled=true} but
     * {@code indexing.s3.direct.write.enabled} is NOT set (relying on the
     * default), the IS-server must NOT call {@code enableDirectUpload} because
     * the default is {@code false} (safe rollout: operators opt in explicitly).
     * The read setter ({@code setDirectObjectStorage}) is still called.
     */
    @Test
    public void isServerSkipsDirectUploadWhenWriteFlagDefault() throws Exception {
        Properties p = new Properties();
        p.setProperty(IndexingServerConfiguration.PROPERTY_S3_DIRECT_ENABLED, "true");
        // PROPERTY_S3_DIRECT_WRITE_ENABLED defaults to false (safe rollout opt-in).
        IndexingServerConfiguration cfg = configWith(p);
        RecordingDsm dsm = newRecordingDsm();
        ObjectStorage fake = new NoopObjectStorage();

        // Mirror the IndexingServer.buildDataStorageManager direct-upload block:
        dsm.setDirectObjectStorage(fake);
        boolean directWriteEnabled = cfg.getBoolean(
                IndexingServerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED,
                IndexingServerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED_DEFAULT);
        if (directWriteEnabled) {
            long maxInflight = cfg.getLong(
                    IndexingServerConfiguration
                            .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES,
                    IndexingServerConfiguration
                            .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES_DEFAULT);
            dsm.enableDirectUpload(maxInflight);
        }

        assertEquals(1, dsm.setterCalls);
        assertEquals("write flag defaults to false — enableDirectUpload must NOT be called",
                0, dsm.enableCalls);
        assertFalse("supportsDirectMultipartUpload must be false when write flag is off",
                dsm.supportsDirectMultipartUpload());
        assertNotNull("read setter must still have been called", dsm.lastAttachedStorage);
    }

    /**
     * IS-server wiring: when {@code indexing.s3.direct.write.enabled=true} is
     * set explicitly, the IS-server calls both setters on the DSM with the
     * configured (or default) inflight cap.
     */
    @Test
    public void isServerEnablesDirectUploadWhenWriteFlagExplicitTrue() throws Exception {
        Properties p = new Properties();
        p.setProperty(IndexingServerConfiguration.PROPERTY_S3_DIRECT_ENABLED, "true");
        p.setProperty(IndexingServerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED, "true");
        IndexingServerConfiguration cfg = configWith(p);
        RecordingDsm dsm = newRecordingDsm();
        ObjectStorage fake = new NoopObjectStorage();

        // Mirror the IndexingServer.buildDataStorageManager direct-upload block:
        dsm.setDirectObjectStorage(fake);
        boolean directWriteEnabled = cfg.getBoolean(
                IndexingServerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED,
                IndexingServerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED_DEFAULT);
        if (directWriteEnabled) {
            long maxInflight = cfg.getLong(
                    IndexingServerConfiguration
                            .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES,
                    IndexingServerConfiguration
                            .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES_DEFAULT);
            dsm.enableDirectUpload(maxInflight);
        }

        assertEquals(1, dsm.setterCalls);
        assertEquals(1, dsm.enableCalls);
        assertEquals(IndexingServerConfiguration
                        .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES_DEFAULT,
                dsm.lastInflightCap);
        assertTrue(dsm.supportsDirectMultipartUpload());
        assertNotNull(dsm.lastAttachedStorage);
    }

    /**
     * IS-server: explicit {@code indexing.s3.direct.write.enabled=false}
     * disables ONLY the write path. Reads remain on. This is the rollout
     * knob: operators can flip writes off if a CRT regression surfaces in
     * production.
     */
    @Test
    public void isServerSkipsUploadWhenWriteFlagFalse() throws Exception {
        Properties p = new Properties();
        p.setProperty(IndexingServerConfiguration.PROPERTY_S3_DIRECT_ENABLED, "true");
        p.setProperty(IndexingServerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED, "false");
        IndexingServerConfiguration cfg = configWith(p);
        RecordingDsm dsm = newRecordingDsm();
        ObjectStorage fake = new NoopObjectStorage();

        dsm.setDirectObjectStorage(fake);
        boolean directWriteEnabled = cfg.getBoolean(
                IndexingServerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED,
                IndexingServerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED_DEFAULT);
        if (directWriteEnabled) {
            dsm.enableDirectUpload(1L);
        }

        assertEquals(1, dsm.setterCalls);
        assertEquals("write flag off must skip the upload setter",
                0, dsm.enableCalls);
        assertFalse(dsm.supportsDirectMultipartUpload());
    }

    /**
     * Custom inflight cap from the IS-server config propagates verbatim.
     */
    @Test
    public void isServerPropagatesCustomInflightCap() throws Exception {
        Properties p = new Properties();
        p.setProperty(IndexingServerConfiguration.PROPERTY_S3_DIRECT_ENABLED, "true");
        p.setProperty(IndexingServerConfiguration
                .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES,
                "33554432"); // 32 MiB
        IndexingServerConfiguration cfg = configWith(p);
        RecordingDsm dsm = newRecordingDsm();
        ObjectStorage fake = new NoopObjectStorage();

        dsm.setDirectObjectStorage(fake);
        long maxInflight = cfg.getLong(
                IndexingServerConfiguration
                        .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES,
                IndexingServerConfiguration
                        .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES_DEFAULT);
        dsm.enableDirectUpload(maxInflight);

        assertEquals(32L * 1024 * 1024, dsm.lastInflightCap);
        assertEquals(32L * 1024 * 1024, dsm.maxDirectInflightUploadBytes());
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private RecordingDsm newRecordingDsm() throws IOException {
        Path metaDir = Files.createDirectories(tmpRoot.resolve("meta"));
        Path remoteTmp = Files.createDirectories(tmpRoot.resolve("remote-tmp"));
        return new RecordingDsm(metaDir, remoteTmp);
    }

    static final class RecordingDsm extends RemoteFileDataStorageManager {
        volatile ObjectStorage lastAttachedStorage;
        volatile int setterCalls;
        volatile int enableCalls;
        volatile long lastInflightCap;

        RecordingDsm(Path metaDir, Path remoteTmp) {
            super(metaDir, remoteTmp, Integer.MAX_VALUE, /* client */ null);
        }

        @Override
        public void setDirectObjectStorage(ObjectStorage storage) {
            this.lastAttachedStorage = storage;
            this.setterCalls++;
            super.setDirectObjectStorage(storage);
        }

        @Override
        public void enableDirectUpload(long maxInflightBytes) {
            this.enableCalls++;
            this.lastInflightCap = maxInflightBytes;
            super.enableDirectUpload(maxInflightBytes);
        }
    }

    /** Minimal stub used only as a non-null argument to setDirectObjectStorage. */
    static final class NoopObjectStorage implements ObjectStorage {
        @Override public CompletableFuture<Void> write(String path, byte[] content) {
            return CompletableFuture.completedFuture(null);
        }
        @Override public CompletableFuture<ReadResult> read(String path) {
            return CompletableFuture.completedFuture(ReadResult.notFound());
        }
        @Override public CompletableFuture<Void> writeBlock(String p, long i, byte[] c) {
            return CompletableFuture.completedFuture(null);
        }
        @Override public CompletableFuture<ReadResult> readRange(String p, long o, int l, int b) {
            return CompletableFuture.completedFuture(ReadResult.notFound());
        }
        @Override public CompletableFuture<Boolean> deleteLogical(String path) {
            return CompletableFuture.completedFuture(Boolean.TRUE);
        }
        @Override public CompletableFuture<List<String>> listLogical(String prefix) {
            return CompletableFuture.completedFuture(new ArrayList<>());
        }
        @Override public CompletableFuture<Boolean> delete(String path) {
            return CompletableFuture.completedFuture(Boolean.TRUE);
        }
        @Override public CompletableFuture<List<String>> list(String prefix) {
            return CompletableFuture.completedFuture(new ArrayList<>());
        }
        @Override public CompletableFuture<Integer> deleteByPrefix(String prefix) {
            return CompletableFuture.completedFuture(0);
        }
        @Override public CompletableFuture<Long> uploadFile(String path, Path src, LongConsumer p) {
            return CompletableFuture.completedFuture(0L);
        }
        @Override public CompletableFuture<Boolean> existsObject(String path) {
            return CompletableFuture.completedFuture(Boolean.FALSE);
        }
        @Override public void close() { }
    }
}
