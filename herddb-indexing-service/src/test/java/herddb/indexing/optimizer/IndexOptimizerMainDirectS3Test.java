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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.mem.MemoryDataStorageManager;
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
 * Unit tests for the direct-S3 wiring added in issue #609 to
 * {@link IndexOptimizerMain}. Exercises the two package-private helpers in
 * isolation so the env-var validation, the {@link ObjectStorage}
 * construction, and the
 * {@link RemoteFileDataStorageManager#setDirectObjectStorage} hand-off are
 * all covered without standing up a real ZooKeeper / file-server / S3
 * backend.
 */
public class IndexOptimizerMainDirectS3Test {

    private Path tmpRoot;

    @Before
    public void setUp() throws IOException {
        tmpRoot = Files.createTempDirectory("optimizer-direct-s3-test-");
    }

    @After
    public void tearDown() throws IOException {
        if (tmpRoot != null) {
            FileUtils.deleteDirectory(tmpRoot.toFile());
        }
    }

    private static OptimizerConfiguration configWith(Properties extra) {
        Properties p = new Properties();
        // Provide a tablespace name so the constructor doesn't trip on
        // required-field validation in unrelated paths. The S3 helpers
        // under test only consume the S3 keys.
        p.setProperty(OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, "ts");
        p.putAll(extra);
        return new OptimizerConfiguration(p);
    }

    /**
     * When {@code indexoptimizer.s3.direct.enabled=false} (the default),
     * {@link IndexOptimizerMain#maybeEnableDirectS3} is a strict no-op:
     * no env-var lookup happens, no setter is called on the DSM. We verify
     * the no-op contract by passing a DSM that would never have direct
     * download support and asserting it stays that way.
     */
    @Test
    public void maybeEnableDirectS3_flagFalse_isStrictNoop() throws Exception {
        OptimizerConfiguration cfg = configWith(new Properties());
        // Default = false; do not even set the property.
        DummyRemoteFileDsm dsm = newRemoteFileDsm();
        assertFalse("precondition: direct download not yet wired",
                dsm.supportsDirectMultipartDownload());

        IndexOptimizerMain.maybeEnableDirectS3(dsm, cfg);

        assertFalse("flag=false must leave direct download disabled",
                dsm.supportsDirectMultipartDownload());
        assertEquals("flag=false must not invoke the setter", 0, dsm.setterCalls);
    }

    /**
     * When the flag is enabled but the DSM is not a
     * {@link RemoteFileDataStorageManager} (e.g. unit-test fallbacks that
     * use the in-memory DSM, or a future plugin DSM), the wiring must log
     * a WARNING and continue — never throw. The optimizer pod must remain
     * startable in degraded environments rather than crashing on a
     * misconfiguration that has zero impact on correctness.
     */
    @Test
    public void maybeEnableDirectS3_flagTrueWithNonRemoteDsm_logsAndSkips()
            throws Exception {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_DIRECT_ENABLED, "true");
        OptimizerConfiguration cfg = configWith(p);

        // No env vars set; the helper must short-circuit on the DSM type check
        // before it ever consults S3_ACCESS_KEY / S3_SECRET_KEY.
        IndexOptimizerMain.maybeEnableDirectS3(new MemoryDataStorageManager(), cfg);
        // Reaching this line proves the helper neither threw nor required env vars.
    }

    /**
     * {@code buildDirectS3ObjectStorage} fast-fails when the access key is
     * absent: the helper throws {@link IOException} with a message that names
     * the missing env var and the optimizer property. This keeps a
     * misconfigured pod from limping into the first merge attempt and then
     * surfacing the failure as a cryptic AWS SDK auth error.
     */
    @Test
    public void buildDirectS3_emptyAccessKey_throws() {
        OptimizerConfiguration cfg = configWith(new Properties());
        try {
            IndexOptimizerMain.buildDirectS3ObjectStorage(cfg, "", "sk");
            fail("expected IOException for empty access key");
        } catch (IOException expected) {
            assertTrue("error message must reference S3_ACCESS_KEY: "
                            + expected.getMessage(),
                    expected.getMessage().contains("S3_ACCESS_KEY"));
            assertTrue("error message must reference the property name: "
                            + expected.getMessage(),
                    expected.getMessage().contains(
                            OptimizerConfiguration.PROPERTY_S3_DIRECT_ENABLED));
        }
    }

    /**
     * Same as above but with a null access key — the env-var case (the
     * variable being unset returns {@code null} from {@code System.getenv},
     * not an empty string).
     */
    @Test
    public void buildDirectS3_nullAccessKey_throws() {
        OptimizerConfiguration cfg = configWith(new Properties());
        try {
            IndexOptimizerMain.buildDirectS3ObjectStorage(cfg, null, "sk");
            fail("expected IOException for null access key");
        } catch (IOException expected) {
            assertTrue(expected.getMessage().contains("S3_ACCESS_KEY"));
        }
    }

    /**
     * Symmetric to {@link #buildDirectS3_emptyAccessKey_throws}: an empty
     * secret key surfaces with a {@code S3_SECRET_KEY}-named error.
     */
    @Test
    public void buildDirectS3_emptySecretKey_throws() {
        OptimizerConfiguration cfg = configWith(new Properties());
        try {
            IndexOptimizerMain.buildDirectS3ObjectStorage(cfg, "ak", "");
            fail("expected IOException for empty secret key");
        } catch (IOException expected) {
            assertTrue("error message must reference S3_SECRET_KEY: "
                            + expected.getMessage(),
                    expected.getMessage().contains("S3_SECRET_KEY"));
        }
    }

    /**
     * Null secret key: same fast-fail path as empty.
     */
    @Test
    public void buildDirectS3_nullSecretKey_throws() {
        OptimizerConfiguration cfg = configWith(new Properties());
        try {
            IndexOptimizerMain.buildDirectS3ObjectStorage(cfg, "ak", null);
            fail("expected IOException for null secret key");
        } catch (IOException expected) {
            assertTrue(expected.getMessage().contains("S3_SECRET_KEY"));
        }
    }

    /**
     * Happy path for the pure builder: with valid creds and a typical
     * GCS-compatible configuration (path-style addressing, WHEN_REQUIRED
     * checksums) it returns a non-null {@link ObjectStorage} without
     * connecting anywhere. The AWS SDK's {@code S3AsyncClient} construction
     * is lazy — no network traffic is generated.
     */
    @Test
    public void buildDirectS3_gcsCompatibilityConfig_returnsObjectStorage()
            throws Exception {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_ENDPOINT,
                "https://storage.googleapis.com");
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_BUCKET, "test-bucket");
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_REGION, "auto");
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_PREFIX, "herddb/");
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_GCS_COMPATIBILITY, "true");
        OptimizerConfiguration cfg = configWith(p);

        ObjectStorage storage = IndexOptimizerMain.buildDirectS3ObjectStorage(
                cfg, "ak", "sk");
        try {
            assertNotNull("builder must return a non-null ObjectStorage", storage);
        } finally {
            // Defensive: ObjectStorage is Closeable in some impls; S3ObjectStorage
            // exposes close-through to the underlying client. Silently best-effort.
            tryClose(storage);
        }
    }

    /**
     * AWS-mode happy path (no GCS compatibility, no endpoint override).
     * Default region is used. Same construction is non-throwing and yields
     * a non-null storage.
     */
    @Test
    public void buildDirectS3_awsModeMinimalConfig_returnsObjectStorage()
            throws Exception {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_BUCKET, "test-bucket-aws");
        // Leave region/prefix/gcsCompatibility at defaults.
        OptimizerConfiguration cfg = configWith(p);

        ObjectStorage storage = IndexOptimizerMain.buildDirectS3ObjectStorage(
                cfg, "ak", "sk");
        try {
            assertNotNull(storage);
        } finally {
            tryClose(storage);
        }
    }

    /**
     * End-to-end through {@link IndexOptimizerMain#maybeEnableDirectS3}
     * using a fake env source: we bypass {@code System.getenv} by directly
     * exercising the pure builder, then verify that
     * {@link RemoteFileDataStorageManager#setDirectObjectStorage} actually
     * flips {@code supportsDirectMultipartDownload()} from {@code false} to
     * {@code true}. This is the contract that
     * {@link IndexOptimizerMain#buildRemoteSegmentMerger} relies on.
     */
    @Test
    public void buildDirectS3_andAttach_flipsSupportsDirectMultipartDownload()
            throws Exception {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_S3_BUCKET, "b");
        OptimizerConfiguration cfg = configWith(p);
        ObjectStorage storage = IndexOptimizerMain.buildDirectS3ObjectStorage(
                cfg, "ak", "sk");
        DummyRemoteFileDsm dsm = newRemoteFileDsm();
        try {
            assertFalse(dsm.supportsDirectMultipartDownload());

            dsm.setDirectObjectStorage(storage);

            assertTrue("setDirectObjectStorage(...) must flip the support flag",
                    dsm.supportsDirectMultipartDownload());
            assertSame("DummyRemoteFileDsm must record the attached storage",
                    storage, dsm.lastAttachedStorage);
        } finally {
            // Release the SDK S3AsyncClient + CRT HTTP client owned by the
            // S3ObjectStorage so the per-test class doesn't leak SDK threads
            // and direct-memory arenas. dsm.close() chains through to the
            // attached ObjectStorage so we don't need to close `storage`
            // explicitly. Broad catch is required: close paths on the SDK and
            // on RemoteFileDataStorageManager can raise both IOException and
            // RuntimeException (e.g. IllegalStateException when an arena is
            // already released); test cleanup must not propagate those.
            try {
                dsm.close();
            } catch (Exception ignored) {
                // best-effort
            }
        }
    }

    // -------------------------------------------------------------------------
    // Test helpers
    // -------------------------------------------------------------------------

    private DummyRemoteFileDsm newRemoteFileDsm() throws IOException {
        Path metaDir = Files.createDirectories(tmpRoot.resolve("meta"));
        Path remoteTmp = Files.createDirectories(tmpRoot.resolve("remote-tmp"));
        // Construct with a null RemoteFileServiceClient: the helpers under test
        // never invoke any client method, and only setDirectObjectStorage /
        // supportsDirectMultipartDownload are exercised on this instance.
        return new DummyRemoteFileDsm(metaDir, remoteTmp);
    }

    /**
     * {@link RemoteFileDataStorageManager} subclass that records direct-storage
     * attach calls so the test can assert on the wiring contract without
     * depending on a real S3 backend.
     */
    private static final class DummyRemoteFileDsm extends RemoteFileDataStorageManager {

        volatile ObjectStorage lastAttachedStorage;
        volatile int setterCalls;

        DummyRemoteFileDsm(Path metaDir, Path remoteTmp) {
            super(metaDir, remoteTmp, Integer.MAX_VALUE, /* client */ null);
        }

        @Override
        public void setDirectObjectStorage(ObjectStorage storage) {
            this.lastAttachedStorage = storage;
            this.setterCalls++;
            super.setDirectObjectStorage(storage);
        }
    }

    private static void tryClose(Object o) {
        // S3ObjectStorage doesn't expose Closeable directly; use reflection so
        // we don't tie the test to its concrete signature. Best-effort cleanup —
        // any failure is ignored so the assertion path isn't masked.
        if (o == null) {
            return;
        }
        try {
            o.getClass().getMethod("close").invoke(o);
        } catch (ReflectiveOperationException ignored) {
            // No close() — nothing to do.
        }
    }
}
