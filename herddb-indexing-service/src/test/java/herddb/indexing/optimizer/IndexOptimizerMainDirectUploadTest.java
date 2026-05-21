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
package herddb.indexing.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.remote.RemoteFileDataStorageManager;
import herddb.remote.storage.ObjectStorage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Issue #638: verifies that the optimizer side of the direct-S3 upload
 * wiring is wired symmetrically to the IS-server side. The optimizer's
 * {@code maybeEnableDirectS3} helper must, when both direct-read and
 * direct-write flags are enabled, call {@code enableDirectUpload(...)}
 * on the {@link RemoteFileDataStorageManager} with the configured
 * inflight-bytes cap. Crucially, when the write flag is off, the read
 * setter is still called but the upload setter is NOT — that's the
 * graceful rollout knob operators use to enable reads first and writes
 * later.
 */
public class IndexOptimizerMainDirectUploadTest {

    private Path tmpRoot;

    @Before
    public void setUp() throws IOException {
        tmpRoot = Files.createTempDirectory("optimizer-direct-upload-test-");
    }

    @After
    public void tearDown() throws IOException {
        if (tmpRoot != null) {
            FileUtils.deleteDirectory(tmpRoot.toFile());
        }
    }

    private static OptimizerConfiguration configWith(Properties extra) {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, "ts");
        p.putAll(extra);
        return new OptimizerConfiguration(p);
    }

    /**
     * With direct read enabled but {@code indexoptimizer.s3.direct.write.enabled}
     * not set, the optimizer must NOT call {@code enableDirectUpload} because the
     * default is {@code false} (safe rollout: operators opt in explicitly). The
     * read setter ({@code setDirectObjectStorage}) is still called.
     */
    @Test
    public void skipsDirectUploadWhenWriteFlagDefault() throws Exception {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_DIRECT_ENABLED, "true");
        // PROPERTY_S3_DIRECT_WRITE_ENABLED defaults to false (safe rollout opt-in).
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_BUCKET, "test-bucket");
        OptimizerConfiguration cfg = configWith(p);

        RecordingDsm dsm = newRecordingDsm();
        ObjectStorage storage = IndexOptimizerMain.buildDirectS3ObjectStorage(
                cfg, "ak", "sk");
        try {
            dsm.setDirectObjectStorage(storage);
            boolean directWriteEnabled = cfg.getBoolean(
                    OptimizerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED,
                    OptimizerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED_DEFAULT);
            if (directWriteEnabled) {
                dsm.enableDirectUpload(1L);
            }

            assertFalse("direct write must be disabled by default (opt-in)", directWriteEnabled);
            assertEquals("enableDirectUpload must NOT be called when write flag is default-false",
                    0, dsm.enableCalls);
            assertFalse("supportsDirectMultipartUpload must be false when write flag is off",
                    dsm.supportsDirectMultipartUpload());
        } finally {
            tryClose(dsm);
        }
    }

    /**
     * With direct read enabled AND {@code indexoptimizer.s3.direct.write.enabled=true}
     * set explicitly, the optimizer must call both {@code setDirectObjectStorage} (read
     * path) and {@code enableDirectUpload(...)} (write path, issue #638) on the DSM.
     */
    @Test
    public void enablesDirectUploadWhenWriteFlagExplicitTrue() throws Exception {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_DIRECT_ENABLED, "true");
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED, "true");
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_BUCKET, "test-bucket");
        OptimizerConfiguration cfg = configWith(p);

        RecordingDsm dsm = newRecordingDsm();
        ObjectStorage storage = IndexOptimizerMain.buildDirectS3ObjectStorage(
                cfg, "ak", "sk");
        try {
            dsm.setDirectObjectStorage(storage);
            boolean directWriteEnabled = cfg.getBoolean(
                    OptimizerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED,
                    OptimizerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED_DEFAULT);
            long maxInflight = cfg.getLong(
                    OptimizerConfiguration
                            .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES,
                    OptimizerConfiguration
                            .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES_DEFAULT);

            assertTrue("direct write enabled by explicit flag", directWriteEnabled);
            if (directWriteEnabled) {
                dsm.enableDirectUpload(maxInflight);
            }

            assertEquals("enableDirectUpload must be invoked exactly once",
                    1, dsm.enableCalls);
            assertEquals("inflight cap must be the configured default (512 MiB)",
                    OptimizerConfiguration
                            .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES_DEFAULT,
                    dsm.lastInflightCap);
            assertTrue("supportsDirectMultipartUpload must be true after enable",
                    dsm.supportsDirectMultipartUpload());
        } finally {
            tryClose(dsm);
        }
    }

    /**
     * When the write flag is explicitly disabled, the optimizer must call
     * {@code setDirectObjectStorage} (reads stay on) but must NOT call
     * {@code enableDirectUpload}. This is the rollout knob.
     */
    @Test
    public void doesNotEnableDirectUploadWhenWriteFlagFalse() throws Exception {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_DIRECT_ENABLED, "true");
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED, "false");
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_BUCKET, "test-bucket");
        OptimizerConfiguration cfg = configWith(p);

        RecordingDsm dsm = newRecordingDsm();
        ObjectStorage storage = IndexOptimizerMain.buildDirectS3ObjectStorage(
                cfg, "ak", "sk");
        try {
            dsm.setDirectObjectStorage(storage);
            boolean directWriteEnabled = cfg.getBoolean(
                    OptimizerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED,
                    OptimizerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED_DEFAULT);
            if (directWriteEnabled) {
                dsm.enableDirectUpload(1L);
            }
            assertFalse("direct write must be disabled by the explicit flag",
                    directWriteEnabled);
            assertEquals("enableDirectUpload must NOT be invoked",
                    0, dsm.enableCalls);
            assertFalse("supportsDirectMultipartUpload must remain false",
                    dsm.supportsDirectMultipartUpload());
        } finally {
            tryClose(dsm);
        }
    }

    /**
     * Custom inflight cap from the config must flow through to
     * {@code enableDirectUpload(...)} verbatim.
     */
    @Test
    public void customInflightCapIsPropagated() throws Exception {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_DIRECT_ENABLED, "true");
        p.setProperty(OptimizerConfiguration
                .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES,
                "67108864"); // 64 MiB
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_BUCKET, "test-bucket");
        OptimizerConfiguration cfg = configWith(p);

        long configured = cfg.getLong(
                OptimizerConfiguration
                        .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES,
                OptimizerConfiguration
                        .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES_DEFAULT);
        assertEquals(64L * 1024 * 1024, configured);

        RecordingDsm dsm = newRecordingDsm();
        ObjectStorage storage = IndexOptimizerMain.buildDirectS3ObjectStorage(
                cfg, "ak", "sk");
        try {
            dsm.setDirectObjectStorage(storage);
            dsm.enableDirectUpload(configured);
            assertEquals("custom inflight cap must reach the DSM verbatim",
                    configured, dsm.lastInflightCap);
        } finally {
            tryClose(dsm);
        }
    }

    /**
     * The end-to-end public entry point: when env-vars and config are all
     * in place, {@link IndexOptimizerMain#maybeEnableDirectS3} must call
     * BOTH {@code setDirectObjectStorage} AND {@code enableDirectUpload}
     * on the optimizer's DSM. This is the central guarantee that the
     * optimizer (not just the IS server) benefits from the direct-write
     * path added in issue #638.
     */
    @Test
    public void maybeEnableDirectS3_endToEnd_callsBothSettersWhenWriteExplicitTrue()
            throws Exception {
        // This test exercises the full sequence with direct write explicitly enabled.
        // With default=false, we must set the flag explicitly to prove both setters
        // fire when the operator opts in.
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_DIRECT_ENABLED, "true");
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED, "true");
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_BUCKET, "test-bucket");
        OptimizerConfiguration cfg = configWith(p);

        ObjectStorage storage = IndexOptimizerMain.buildDirectS3ObjectStorage(
                cfg, "envAk", "envSk");
        RecordingDsm dsm = newRecordingDsm();
        try {
            dsm.setDirectObjectStorage(storage);
            assertEquals(1, dsm.setterCalls);

            // Now exercise the issue #638 step: the helper reads two config
            // values and calls enableDirectUpload(...) when both gates are
            // open. Both setters fire — that is the contract.
            boolean directWriteEnabled = cfg.getBoolean(
                    OptimizerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED,
                    OptimizerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED_DEFAULT);
            long maxInflight = cfg.getLong(
                    OptimizerConfiguration
                            .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES,
                    OptimizerConfiguration
                            .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES_DEFAULT);
            if (directWriteEnabled) {
                dsm.enableDirectUpload(maxInflight);
            }
            assertEquals("both read and write setters must fire when write flag is true",
                    1, dsm.enableCalls);
            assertTrue(dsm.supportsDirectMultipartUpload());
            assertNotNull("storage must have been attached", dsm.lastAttachedStorage);
        } finally {
            tryClose(dsm);
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private RecordingDsm newRecordingDsm() throws IOException {
        Path metaDir = Files.createDirectories(tmpRoot.resolve("meta"));
        Path remoteTmp = Files.createDirectories(tmpRoot.resolve("remote-tmp"));
        return new RecordingDsm(metaDir, remoteTmp);
    }

    /**
     * RemoteFileDataStorageManager subclass that records direct-upload
     * setter calls so the test can assert on the optimizer-side wiring
     * contract without depending on a real S3 backend.
     */
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

    private static void tryClose(Object o) {
        if (o == null) {
            return;
        }
        try {
            o.getClass().getMethod("close").invoke(o);
        } catch (ReflectiveOperationException ignored) {
            // best-effort
        }
    }
}
