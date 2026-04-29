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
    public void enabledOptimizerWithoutTablespaceUuidFailsLoud() throws Exception {
        ProcessResult r = runHelm("template", "test", chartDir.toString(),
                "--set", "indexOptimizer.enabled=true");
        assertFalse("expected helm template to fail without tablespaceUuid", r.exitCode == 0);
        assertTrue("error message must mention the required value:\n" + r.stderr,
                r.stderr.contains("tablespaceUuid"));
    }

    @Test
    public void enabledOptimizerRendersStatefulSetConfigMapAndService() throws Exception {
        ProcessResult r = runHelm("template", "test", chartDir.toString(),
                "--set", "indexOptimizer.enabled=true",
                "--set", "indexOptimizer.tablespaceUuid=fake-uuid-123");
        assertEquals("helm template failed:\n" + r.stderr, 0, r.exitCode);

        String yaml = r.stdout;
        // ConfigMap with our properties.
        assertTrue("ConfigMap must be rendered",
                yaml.contains("name: test-herddb-index-optimizer-config"));
        assertTrue("ConfigMap must contain the tablespaceUuid",
                yaml.contains("indexoptimizer.tablespace.uuid=\"fake-uuid-123\""));
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
                "--set", "indexOptimizer.tablespaceUuid=t",
                "--set", "indexOptimizer.storage.tmp.size=50Gi",
                "--set", "indexOptimizer.storage.tmp.storageClass=fast-ssd");
        assertEquals(0, r.exitCode);
        assertTrue(r.stdout.contains("storage: 50Gi"));
        assertTrue(r.stdout.contains("storageClassName: \"fast-ssd\""));
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
