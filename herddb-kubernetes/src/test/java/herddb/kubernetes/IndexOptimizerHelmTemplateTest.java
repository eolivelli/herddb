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
package herddb.kubernetes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Test;

/**
 * Renders the HerdDB Helm chart with various {@code indexOptimizer.*} settings
 * via {@code helm template} and asserts the generated YAML contains the right
 * resources (StatefulSet + headless Service + ConfigMap) — and the right
 * mounts/PVCs for the optimizer's local temp directory.
 *
 * <p>Skipped automatically when the {@code helm} CLI is not on the PATH; CI
 * environments that ship without helm can still run the rest of the suite.
 */
public class IndexOptimizerHelmTemplateTest {

    private Path chartDir;

    @Before
    public void setUp() {
        chartDir = Paths.get("src", "main", "helm", "herddb").toAbsolutePath();
        assumeTrue("chart directory not found at " + chartDir,
                Files.isDirectory(chartDir));
        assumeTrue("helm CLI not available", helmAvailable());
    }

    @Test
    public void chartLintsClean() throws Exception {
        ProcessResult r = runHelm("lint", chartDir.toString());
        assertEquals("helm lint failed:\n" + r.stdout + "\n" + r.stderr, 0, r.exitCode);
        assertTrue(r.stdout.contains("0 chart(s) failed"));
    }

    @Test
    public void disabledOptimizerProducesNoOptimizerResources() throws Exception {
        ProcessResult r = runHelm("template", "test", chartDir.toString(),
                "--set", "indexOptimizer.enabled=false");
        assertEquals(0, r.exitCode);
        assertFalse("StatefulSet should not be rendered when optimizer is disabled",
                r.stdout.contains("test-herddb-index-optimizer"));
    }

    @Test
    public void enabledOptimizerWithoutTablespaceNameFailsLoud() throws Exception {
        ProcessResult r = runHelm("template", "test", chartDir.toString(),
                "--set", "indexOptimizer.enabled=true");
        assertFalse("expected helm template to fail without tablespaceName", r.exitCode == 0);
        assertTrue("error message must mention the required value:\n" + r.stderr,
                r.stderr.contains("tablespaceName"));
    }

    @Test
    public void enabledOptimizerRendersStatefulSetConfigMapAndService() throws Exception {
        ProcessResult r = runHelm("template", "test", chartDir.toString(),
                "--set", "indexOptimizer.enabled=true",
                "--set", "indexOptimizer.tablespaceName=herd");
        assertEquals("helm template failed:\n" + r.stderr, 0, r.exitCode);

        String yaml = r.stdout;
        // ConfigMap with our properties.
        assertTrue("ConfigMap must be rendered",
                yaml.contains("name: test-herddb-index-optimizer-config"));
        assertTrue("ConfigMap must contain the tablespaceName",
                yaml.contains("indexoptimizer.tablespace.name=herd"));
        assertTrue("ConfigMap must contain the interval",
                yaml.contains("indexoptimizer.interval.ms=300000"));

        // StatefulSet (NOT Deployment) — singleton with persistent temp PVC.
        assertTrue("StatefulSet must be rendered (per user requirement)",
                yaml.contains("kind: StatefulSet")
                        && yaml.contains("name: test-herddb-index-optimizer\n"));
        assertTrue("StatefulSet must declare replicas: 1",
                yaml.matches("(?s).*name: test-herddb-index-optimizer.*?replicas: 1.*"));
        assertTrue("StatefulSet must declare a volumeClaimTemplate named tmp",
                yaml.matches("(?s).*name: test-herddb-index-optimizer.*?volumeClaimTemplates:.*?name: tmp.*"));
        assertTrue("Container must mount /opt/herddb/optimizer-tmp",
                yaml.contains("mountPath: /opt/herddb/optimizer-tmp"));

        // Headless service for the StatefulSet.
        assertTrue("headless Service must be rendered",
                yaml.contains("kind: Service\n")
                        && yaml.contains("name: test-herddb-index-optimizer\n")
                        && yaml.contains("clusterIP: None"));

        // Container args must reference the bin/service launcher.
        assertTrue("Container must invoke bin/service index-optimizer console",
                yaml.contains("/opt/herddb/bin/service index-optimizer console"));
    }

    @Test
    public void customStorageSizeAndStorageClassFlowToVolumeClaimTemplate() throws Exception {
        ProcessResult r = runHelm("template", "test", chartDir.toString(),
                "--set", "indexOptimizer.enabled=true",
                "--set", "indexOptimizer.tablespaceName=herd",
                "--set", "indexOptimizer.storage.tmp.size=50Gi",
                "--set", "indexOptimizer.storage.tmp.storageClass=fast-ssd");
        assertEquals(0, r.exitCode);
        assertTrue(r.stdout.contains("storage: 50Gi"));
        assertTrue(r.stdout.contains("storageClassName: \"fast-ssd\""));
    }

    /**
     * Issue #609: with the chart's default {@code indexOptimizer.s3.directEnabled=false}
     * the configmap must NOT emit {@code indexoptimizer.s3.direct.enabled=true},
     * and the statefulset must NOT mount {@code S3_ACCESS_KEY} /
     * {@code S3_SECRET_KEY} env vars. This guards against an accidental regression
     * where the helm template would unconditionally render those keys and force
     * every deployment to ship a credentials Secret.
     */
    @Test
    public void s3DirectDisabledByDefault_noS3KeysOrEnvVars() throws Exception {
        ProcessResult r = runHelm("template", "test", chartDir.toString(),
                "--set", "indexOptimizer.enabled=true",
                "--set", "indexOptimizer.tablespaceName=herd");
        assertEquals("helm template failed:\n" + r.stderr, 0, r.exitCode);
        String yaml = r.stdout;
        assertFalse("default render must not enable direct S3:\n" + yaml,
                yaml.contains("indexoptimizer.s3.direct.enabled=true"));
        assertFalse("default render must not inject S3_ACCESS_KEY env var:\n" + yaml,
                yaml.contains("name: S3_ACCESS_KEY"));
        assertFalse("default render must not inject S3_SECRET_KEY env var:\n" + yaml,
                yaml.contains("name: S3_SECRET_KEY"));
    }

    /**
     * Issue #609: with {@code indexOptimizer.s3.directEnabled=true} plus a
     * complete GCS-style configuration (endpoint + bucket + gcsCompatibility +
     * credentialsSecret), the configmap must emit every
     * {@code indexoptimizer.s3.*} property and the statefulset must inject the
     * {@code S3_ACCESS_KEY} / {@code S3_SECRET_KEY} env vars sourced from the
     * configured Secret. Mirrors the indexing-service direct-S3 contract from
     * issue #381.
     */
    @Test
    public void s3DirectEnabled_rendersPropertiesAndCredentialEnvVars()
            throws Exception {
        ProcessResult r = runHelm("template", "test", chartDir.toString(),
                "--set", "indexOptimizer.enabled=true",
                "--set", "indexOptimizer.tablespaceName=herd",
                "--set", "indexOptimizer.s3.directEnabled=true",
                "--set", "indexOptimizer.s3.endpoint=https://storage.googleapis.com",
                "--set", "indexOptimizer.s3.bucket=test-bucket",
                "--set", "indexOptimizer.s3.region=auto",
                "--set", "indexOptimizer.s3.prefix=herddb/",
                "--set", "indexOptimizer.s3.gcsCompatibility=true",
                "--set", "indexOptimizer.s3.credentialsSecret=test-gcs-creds");
        assertEquals("helm template failed:\n" + r.stderr, 0, r.exitCode);
        String yaml = r.stdout;

        // ConfigMap rendering.
        assertTrue("missing indexoptimizer.s3.direct.enabled=true:\n" + yaml,
                yaml.contains("indexoptimizer.s3.direct.enabled=true"));
        assertTrue("missing indexoptimizer.s3.endpoint:\n" + yaml,
                yaml.contains("indexoptimizer.s3.endpoint=https://storage.googleapis.com"));
        assertTrue("missing indexoptimizer.s3.bucket:\n" + yaml,
                yaml.contains("indexoptimizer.s3.bucket=test-bucket"));
        assertTrue("missing indexoptimizer.s3.region:\n" + yaml,
                yaml.contains("indexoptimizer.s3.region=auto"));
        assertTrue("missing indexoptimizer.s3.prefix:\n" + yaml,
                yaml.contains("indexoptimizer.s3.prefix=herddb/"));
        assertTrue("missing indexoptimizer.s3.gcs.compatibility=true:\n" + yaml,
                yaml.contains("indexoptimizer.s3.gcs.compatibility=true"));

        // StatefulSet env-var injection from the Secret.
        assertTrue("S3_ACCESS_KEY env var must be injected:\n" + yaml,
                yaml.contains("name: S3_ACCESS_KEY"));
        assertTrue("S3_SECRET_KEY env var must be injected:\n" + yaml,
                yaml.contains("name: S3_SECRET_KEY"));
        assertTrue("S3 env vars must reference the credentials Secret name:\n" + yaml,
                yaml.contains("name: test-gcs-creds"));
    }

    /* ------------------- helpers ------------------- */

    private static boolean helmAvailable() {
        try {
            ProcessResult r = runHelm("version", "--short");
            return r.exitCode == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private static ProcessResult runHelm(String... args) throws Exception {
        java.util.List<String> cmd = new java.util.ArrayList<>();
        cmd.add("helm");
        for (String a : args) {
            cmd.add(a);
        }
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(false);
        Process p = pb.start();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        Thread to = new Thread(() -> drain(p.getInputStream(), out));
        Thread te = new Thread(() -> drain(p.getErrorStream(), err));
        to.start();
        te.start();
        int code = p.waitFor();
        to.join();
        te.join();
        return new ProcessResult(code, out.toString(java.nio.charset.StandardCharsets.UTF_8),
                err.toString(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static void drain(InputStream in, ByteArrayOutputStream out) {
        try {
            byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) >= 0) {
                out.write(buf, 0, n);
            }
        } catch (Exception ignored) {
        }
    }

    private static final class ProcessResult {
        final int exitCode;
        final String stdout;
        final String stderr;

        ProcessResult(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }
    }
}
