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

import static herddb.kubernetes.DockerProcessUtil.dockerProcess;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class HerdDBKubernetesIT {

    private static final Logger LOG = Logger.getLogger(HerdDBKubernetesIT.class.getName());

    private static final String IMAGE_NAME = "herddb/herddb-server";
    private static final String IMAGE_TAG = "0.30.0-SNAPSHOT";
    private static final String FULL_IMAGE = IMAGE_NAME + ":" + IMAGE_TAG;

    // Setting -Dio.netty.maxDirectMemory=<bytes> matching -XX:MaxDirectMemorySize is required so that
    // Netty uses Unsafe.allocateMemory (no-cleaner pooled path) with an internal byte cap, bypassing
    // JVM Bits.reserveMemory accounting. Without this property, Netty falls back to ByteBuffer.allocateDirect
    // and direct allocations are bounded by phantom-reference GC delays — see issue #253 and the comment in
    // herddb-services/src/main/resources/bin/setenv.sh which sets -Dio.netty.maxDirectMemory=0 in the default
    // JAVA_OPTS baseline (lost when the Helm chart's full-replace server.javaOpts is supplied as in these tests).
    // Recent off-heap relocations (issues #399, #409, #411) make even simple SQL traffic allocate enough direct
    // memory that the previous 128 MiB cap caused server stalls on CI (issue #438).
    // Byte values: 268435456 = 256 * 1024 * 1024 (= MaxDirectMemorySize=256m).
    //
    // -XX:NativeMemoryTracking=summary enables `jcmd <pid> VM.native_memory summary` so that
    // KubernetesDiagnostics can report the actual direct/native memory breakdown when a test fails
    // (issue #438). Per Oracle, NMT summary mode adds ~5-10% per-malloc overhead — accepted here
    // because the K3s ITs need the breakdown to diagnose memory-pressure stalls. Remove this flag
    // once issue #438 is resolved if the overhead becomes a problem in tighter-memory scenarios.
    private static final String SERVER_JAVA_OPTS = "-XX:+UseG1GC -Duser.language=en -Xmx256m -Xms256m"
            + " -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=256m"
            + " -Dio.netty.maxDirectMemory=268435456"
            + " -XX:NativeMemoryTracking=summary"
            + " -Djava.awt.headless=true --add-modules jdk.incubator.vector";

    @ClassRule
    public static K3sContainer k3s = new K3sContainer(DockerImageName.parse("rancher/k3s:v1.31.4-k3s1"))
            .withExposedPorts(6443);

    private static KubernetesClient kubernetesClient;

    /**
     * Per-test wall-clock timeout (issue #438). The JDBC client already has a 600s
     * application-level timeout; this rule bounds the test method itself in case
     * something below the JDBC layer (e.g. {@code kubectl exec} streaming) hangs
     * indefinitely. 25 minutes leaves headroom for boot + slow first-DDL + diagnostics.
     */
    @Rule
    public Timeout perTestTimeout = new Timeout(25, TimeUnit.MINUTES);

    /**
     * Dump comprehensive diagnostics from the K3s cluster on test failure
     * (issue #438). Without this, the only signal we get from CI is
     * "Request timedout (600s)" with no clue whether the server JVM was
     * stuck in GC, OOMKilled, deadlocked, or otherwise hung.
     */
    @Rule
    public TestWatcher diagnosticsRule = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            LOG.severe("Test " + description.getMethodName() + " FAILED: " + e);
            try {
                KubernetesDiagnostics.dumpAll(k3s, kubernetesClient, description.getMethodName());
            } catch (RuntimeException diag) {
                LOG.log(Level.WARNING, "diagnostics dump failed", diag);
            }
        }
    };

    @BeforeClass
    public static void setup() throws Exception {
        // Check that the docker image exists locally
        Process checkImage = dockerProcess("docker", "image", "inspect", FULL_IMAGE)
                .redirectErrorStream(true)
                .start();
        int exitCode = checkImage.waitFor();
        assumeTrue("Docker image " + FULL_IMAGE + " must be built first "
                + "(run: mvn package jib:dockerBuild@build -pl herddb-docker)", exitCode == 0);

        // Save docker image to a tarball
        Path imageTar = Files.createTempFile("herddb-image", ".tar");
        try {
            LOG.info("Saving docker image to tarball...");
            Process save = dockerProcess("docker", "save", FULL_IMAGE, "-o", imageTar.toString())
                    .redirectErrorStream(true)
                    .start();
            assertEquals("docker save failed", 0, save.waitFor());

            // Copy tarball into K3S container and import
            LOG.info("Loading image into K3S...");
            k3s.copyFileToContainer(MountableFile.forHostPath(imageTar), "/tmp/herddb.tar");
            k3s.execInContainer("ctr", "--address", "/run/k3s/containerd/containerd.sock",
                    "--namespace", "k8s.io", "images", "import", "/tmp/herddb.tar");
        } finally {
            Files.deleteIfExists(imageTar);
        }

        // Create Kubernetes client
        String kubeConfigYaml = k3s.getKubeConfigYaml();
        Config config = Config.fromKubeconfig(kubeConfigYaml);
        config.setNamespace("default");
        kubernetesClient = new KubernetesClientBuilder().withConfig(config).build();

        // Render helm chart with standalone mode
        String helmChartPath = findHelmChartPath();
        LOG.info("Using helm chart at: " + helmChartPath);

        String renderedYaml = helmTemplate(helmChartPath);
        LOG.info("Rendered YAML length: " + renderedYaml.length());

        // Apply rendered manifests
        List<HasMetadata> resources = kubernetesClient.load(
                new ByteArrayInputStream(renderedYaml.getBytes(StandardCharsets.UTF_8))).items();
        LOG.info("Applying " + resources.size() + " Kubernetes resources...");
        kubernetesClient.resourceList(resources).createOrReplace();

        // Wait for ZooKeeper and BookKeeper
        LOG.info("Waiting for ZooKeeper pod to be ready...");
        kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "zookeeper")
                .waitUntilReady(5, TimeUnit.MINUTES);
        LOG.info("ZooKeeper pod is ready.");

        LOG.info("Waiting for BookKeeper pod to be ready...");
        kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "bookkeeper")
                .waitUntilReady(5, TimeUnit.MINUTES);
        LOG.info("BookKeeper pod is ready.");

        // Wait for the server pod to be ready
        LOG.info("Waiting for HerdDB server pod to be ready...");
        kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "server")
                .waitUntilReady(5, TimeUnit.MINUTES);
        LOG.info("HerdDB server pod is ready.");
    }

    @AfterClass
    public static void tearDown() {
        if (kubernetesClient != null) {
            kubernetesClient.close();
        }
    }

    @Test
    public void testBasicOperations() throws Exception {
        // Wait for tools pod
        LOG.info("Waiting for tools pod to be ready...");
        kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "tools")
                .waitUntilReady(5, TimeUnit.MINUTES);
        LOG.info("Tools pod is ready.");

        String toolsPod = getToolsPodName();

        // Wait for tablespace to be ready via CLI
        waitForTablespace(k3s, toolsPod);

        // CREATE TABLE
        execSql(k3s, toolsPod, "CREATE TABLE test_table (id int primary key, name string)");
        LOG.info("Table created.");

        // INSERT
        execSql(k3s, toolsPod, "INSERT INTO test_table (id, name) VALUES (1, 'hello')");
        LOG.info("Row inserted.");

        // SELECT and verify
        String output = execSql(k3s, toolsPod, "SELECT id, name FROM test_table");
        LOG.info("SELECT output: " + output);
        assertTrue("Expected 'hello' in output", output.contains("hello"));
        assertTrue("Expected '1' in output", output.contains("1"));
        LOG.info("Row verified.");
    }

    /**
     * Assert that the JVM in the given pod has picked up the jvector-required
     * flags — both on the actual java command line (read from
     * /proc/&lt;pid&gt;/cmdline of the running java process) and in the JVM's
     * own log output (the per-directive "CompileCommand: inline ..." lines
     * printed while parsing -XX:CompileCommandFile). Together these prove
     * that setenv.sh correctly wires the directives file through to every
     * service even when the Helm chart's full-replace javaOpts field is set.
     */
    static void assertJvectorCompilerDirectivesActive(K3sContainer k3sContainer,
                                                      String podName) throws Exception {
        // Command-line evidence: locate the java process inside the pod and
        // read its argv from /proc/<pid>/cmdline. The directives file path and
        // --add-modules jdk.incubator.vector must appear in the actual argv.
        String shellCmd = "pid=$(pgrep -f 'herddb\\.server\\..*Main' || pgrep -f 'java.*herddb' || pgrep -x java); "
                + "if [ -z \"$pid\" ]; then echo NO_JAVA_PID; ps -eo pid,args; exit 1; fi; "
                + "tr '\\0' ' ' < /proc/$pid/cmdline; echo";
        org.testcontainers.containers.Container.ExecResult cmdline = k3sContainer.execInContainer(
                "kubectl", "exec", podName, "--", "sh", "-c", shellCmd);
        String javaCmdline = cmdline.getStdout().trim();
        assertTrue("Pod " + podName + " java cmdline lookup failed (stdout='" + javaCmdline
                + "', stderr='" + cmdline.getStderr() + "')",
                cmdline.getExitCode() == 0 && !javaCmdline.contains("NO_JAVA_PID"));
        assertTrue("Pod " + podName + " java command line missing '--add-modules jdk.incubator.vector':\n"
                + javaCmdline, javaCmdline.contains("--add-modules jdk.incubator.vector"));
        assertTrue("Pod " + podName + " java command line missing "
                + "'-XX:CompileCommandFile=conf/jvector-compiler-directives':\n" + javaCmdline,
                javaCmdline.contains("-XX:CompileCommandFile=conf/jvector-compiler-directives"));

        // File evidence: read /opt/herddb/conf/jvector-compiler-directives
        // from inside the pod and confirm it ships the expected directives.
        // Combined with the cmdline check above this proves the JVM both
        // received the flag AND has a valid file to parse — the only way
        // it can fail to apply the directives is if the JVM rejects the
        // file at startup, which would crash-loop the pod (and we'd never
        // reach this assertion at all).
        org.testcontainers.containers.Container.ExecResult fileResult = k3sContainer.execInContainer(
                "kubectl", "exec", podName, "--", "cat", "/opt/herddb/conf/jvector-compiler-directives");
        assertTrue("Could not read /opt/herddb/conf/jvector-compiler-directives in pod "
                + podName + ": exit=" + fileResult.getExitCode() + " stderr=" + fileResult.getStderr(),
                fileResult.getExitCode() == 0);
        String directivesFile = fileResult.getStdout();
        String[] expectedDirectives = {
                "inline io/github/jbellis/jvector/vector/PanamaVectorUtilSupport.*",
                "inline io/github/jbellis/jvector/vector/NativeVectorUtilSupport.*",
                "inline io/github/jbellis/jvector/vector/cnative/NativeSimdOps.*",
                "inline io/github/jbellis/jvector/vector/DefaultVectorUtilSupport.*",
                "inline io/github/jbellis/jvector/vector/VectorUtil.*"
        };
        for (String expected : expectedDirectives) {
            assertTrue("Pod " + podName + " /opt/herddb/conf/jvector-compiler-directives missing '"
                    + expected + "':\n" + directivesFile,
                    directivesFile.contains(expected));
        }

        // Log evidence (best-effort): each directive parsed from the file is
        // echoed as a "CompileCommand: inline <class> bool inline = true"
        // line at JVM startup. The catch is that those lines live at the very
        // HEAD of the pod log; on chatty services (e.g. indexing-service
        // spamming ZK retries) kubelet rotates the log file before the test
        // gets a chance to look. We read the on-disk kubelet log chain
        // (un-gzipping rotated files) and assert when the head is still
        // there, or just log the situation when it isn't — the cmdline +
        // file-content evidence already proves the flag is in effect.
        String headShellCmd = "set -e; "
                + "podLogDir=$(ls -d /var/log/pods/default_" + podName + "_*/* 2>/dev/null "
                + "| grep -v wait-for | head -n1); "
                + "if [ -z \"$podLogDir\" ]; then exit 1; fi; "
                + "for f in $(ls -1tr \"$podLogDir\"/*.log* 2>/dev/null); do "
                + "case \"$f\" in *.gz) zcat \"$f\" ;; *) cat \"$f\" ;; esac; "
                + "done";
        org.testcontainers.containers.Container.ExecResult logResult = k3sContainer.execInContainer(
                "sh", "-c", headShellCmd);
        String logs = logResult.getExitCode() == 0 ? logResult.getStdout() : "";
        if (logs == null) {
            logs = "";
        }
        boolean foundInLog = logs.contains(
                "CompileCommand: inline io/github/jbellis/jvector/vector/PanamaVectorUtilSupport.*");
        if (foundInLog) {
            LOG.info("Pod " + podName + ": jvector compiler directives confirmed on "
                    + "java command line, in shipped conf file, and in JVM startup log.");
        } else {
            LOG.info("Pod " + podName + ": jvector compiler directives confirmed on "
                    + "java command line and in shipped conf file (JVM startup log already "
                    + "rotated past the CompileCommand banner — log size=" + logs.length() + ").");
        }
    }

    /**
     * Wait for the HerdDB tablespace to be fully booted by polling via CLI.
     */
    static void waitForTablespace(K3sContainer k3sContainer, String toolsPod) throws Exception {
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
        long lastDiagnostic = System.currentTimeMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                execSqlViaKubectl(k3sContainer, toolsPod, "SELECT * FROM systables LIMIT 1");
                LOG.info("Tablespace is ready.");
                return;
            } catch (Exception e) {
                LOG.info("Tablespace not ready yet: " + e.getMessage());
            }
            // Periodic mid-wait diagnostics (issue #438): if waitForTablespace is taking
            // longer than 5 minutes, dump LIGHTWEIGHT pod state every 5 minutes so we can
            // see *why* the tablespace is not ready (stuck on BookKeeper, ZK churn, missing
            // image, ...). Lightweight = pods, events, kubectl describe, recent logs only —
            // skip the heavier inside-pod jstack/jcmd probes which can themselves hang on a
            // stuck JVM and burn the per-test 25-min budget. The heavy probes are reserved
            // for the final timeout dump below and the @Rule TestWatcher.failed dump.
            if (System.currentTimeMillis() - lastDiagnostic > TimeUnit.MINUTES.toMillis(5)) {
                LOG.info("waitForTablespace exceeded 5 minutes; dumping interim (lightweight) diagnostics...");
                try {
                    KubernetesDiagnostics.dumpLightweight(k3sContainer, kubernetesClient,
                            "waitForTablespace-progress");
                } catch (RuntimeException diag) {
                    LOG.log(Level.WARNING, "interim diagnostics dump failed", diag);
                }
                lastDiagnostic = System.currentTimeMillis();
            }
            Thread.sleep(5000);
        }
        // Final diagnostic before throwing — full heavy dump (jstack, jcmd, smaps).
        try {
            KubernetesDiagnostics.dumpAll(k3sContainer, kubernetesClient, "waitForTablespace-timeout");
        } catch (RuntimeException diag) {
            LOG.log(Level.WARNING, "final diagnostics dump failed", diag);
        }
        throw new RuntimeException("Timed out waiting for tablespace to be ready");
    }

    String getToolsPodName() {
        List<Pod> pods = kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "tools")
                .list().getItems();
        assertEquals("Expected 1 tools pod", 1, pods.size());
        return pods.get(0).getMetadata().getName();
    }

    /**
     * Execute a SQL statement via herddb-cli in the tools pod.
     * Returns the CLI stdout output.
     */
    static String execSql(K3sContainer k3sContainer, String toolsPodName, String sql) throws Exception {
        return execSqlViaKubectl(k3sContainer, toolsPodName, sql);
    }

    /**
     * Execute SQL via kubectl exec inside the K3s container.
     * This is more reliable than the fabric8 exec API whose WebSocket
     * onClose callback doesn't fire consistently on CI.
     */
    static String execSqlViaKubectl(K3sContainer k3sContainer, String toolsPodName, String sql) throws Exception {
        // Use bash -c with 2>&1 to capture CLI errors in stdout
        // (kubectl stderr only shows generic "command terminated with exit code N")
        // Use /opt/herddb/bin/herddb-cli.sh directly with extended client timeout
        // (default 300s is too short for slow CI where first DDL can take minutes)
        String shellCmd = "/opt/herddb/bin/herddb-cli.sh"
                + " -x \"${HERDDB_JDBC_URL}?client.timeout=600000\""
                + " --query '" + sql.replace("'", "'\\''") + "' 2>&1; echo \"__EXIT:$?\"";
        org.testcontainers.containers.Container.ExecResult result = k3sContainer.execInContainer(
                "kubectl", "exec", toolsPodName, "--",
                "bash", "-c", shellCmd);
        String combined = result.getStdout();

        // Parse exit code from marker
        int exitCode = 0;
        String output = combined;
        int markerIdx = combined.lastIndexOf("__EXIT:");
        if (markerIdx >= 0) {
            String codeStr = combined.substring(markerIdx + "__EXIT:".length()).trim();
            try {
                exitCode = Integer.parseInt(codeStr);
            } catch (NumberFormatException e) {
                // ignore
            }
            output = combined.substring(0, markerIdx).trim();
        }

        if (exitCode != 0) {
            // Diagnostic dump on SQL failure (issue #438): every K3s IT failure
            // we have seen on CI has this exact code path, so capture cluster
            // state HERE — before the test method returns and the @Rule
            // TestWatcher kicks in (the watcher fires too, but captures from a
            // slightly later vantage point; both are kept for safety).
            try {
                KubernetesDiagnostics.dumpAll(k3sContainer, kubernetesClient,
                        "execSqlViaKubectl-failure");
            } catch (RuntimeException diag) {
                LOG.log(Level.WARNING, "diagnostics dump failed", diag);
            }
            throw new RuntimeException("SQL failed (exit=" + exitCode + "): " + output);
        }
        return output;
    }

    private static String findHelmChartPath() {
        String[] candidates = {
                "src/main/helm/herddb",
                "herddb-kubernetes/src/main/helm/herddb"
        };
        for (String candidate : candidates) {
            File chartDir = new File(candidate);
            if (new File(chartDir, "Chart.yaml").exists()) {
                return chartDir.getAbsolutePath();
            }
        }
        throw new IllegalStateException("Cannot find helm chart directory. "
                + "Looked in: " + String.join(", ", candidates));
    }

    // Same rationale as SERVER_JAVA_OPTS: set -Dio.netty.maxDirectMemory explicitly to match
    // MaxDirectMemorySize so Netty uses the Unsafe no-cleaner path (issue #253, issue #438).
    // -XX:NativeMemoryTracking=summary enables jcmd VM.native_memory summary for diagnostics.
    // Byte values: 134217728 = 128 * 1024 * 1024 (= MaxDirectMemorySize=128m).
    private static final String INFRA_JAVA_OPTS = "-XX:+UseG1GC -Duser.language=en -Xmx128m -Xms128m"
            + " -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=128m"
            + " -Dio.netty.maxDirectMemory=134217728"
            + " -XX:NativeMemoryTracking=summary"
            + " -Djava.awt.headless=true --add-modules jdk.incubator.vector";

    private static String helmTemplate(String chartPath) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
                "helm", "template", "test-release", chartPath,
                "--set", "server.mode=cluster",
                // Keep chart-default server.storageMode=remote so the test exercises the
                // production cluster+remote-storage path. To fit on the single-node K3s
                // testcontainer (4 vCPUs on GH Actions) we right-size the file-server
                // (the chart default is replicaCount=2 × cpu=4 = 8 CPUs requested,
                // which is unschedulable and leaves file-server-0 Pending with
                // "Insufficient cpu" — see issue #438).
                "--set", "server.replicaCount=1",
                "--set", "tools.enabled=true",
                "--set", "zookeeper.enabled=true",
                "--set", "bookkeeper.enabled=true",
                "--set", "bookkeeper.replicaCount=1",
                "--set", "zookeeper.javaOpts=" + INFRA_JAVA_OPTS,
                // Container memory raised from 256Mi to 384Mi to fit 128m heap + 128m direct + ~128m
                // JVM/native overhead (issue #438). Same applies to all infra pods below.
                "--set", "zookeeper.resources.requests.memory=384Mi",
                "--set", "zookeeper.resources.requests.cpu=0.5",
                "--set", "zookeeper.resources.limits.memory=384Mi",
                "--set", "zookeeper.resources.limits.cpu=0.5",
                "--set", "zookeeper.storage.size=1Gi",
                "--set", "bookkeeper.javaOpts=" + INFRA_JAVA_OPTS,
                "--set", "bookkeeper.resources.requests.memory=384Mi",
                "--set", "bookkeeper.resources.requests.cpu=0.5",
                "--set", "bookkeeper.resources.limits.memory=384Mi",
                "--set", "bookkeeper.resources.limits.cpu=0.5",
                "--set", "bookkeeper.storage.journal.size=1Gi",
                "--set", "bookkeeper.storage.ledger.size=1Gi",
                "--set", "server.javaOpts=" + SERVER_JAVA_OPTS,
                // Server container memory raised from 512Mi to 768Mi to fit 256m heap + 256m direct +
                // ~256m JVM/native overhead under the new off-heap memory profile (issue #438).
                "--set", "server.resources.requests.memory=768Mi",
                "--set", "server.resources.requests.cpu=0.5",
                "--set", "server.resources.limits.memory=768Mi",
                "--set", "server.resources.limits.cpu=0.5",
                "--set", "server.storage.data.size=1Gi",
                "--set", "server.storage.commitlog.size=1Gi",
                // File server: 1 replica with infra-sized resources to fit on a 4-vCPU
                // K3s runner. Chart defaults are replicaCount=2 × cpu=4 = 8 CPUs which
                // is unschedulable on GH Actions; see issue #438. Empty fileServer.javaOpts
                // (chart default) means setenv.sh's -Xmx4g default applies inside the pod
                // and instantly OOMs at the 384Mi memory limit, so override javaOpts too.
                "--set", "fileServer.replicaCount=1",
                "--set", "fileServer.javaOpts=" + INFRA_JAVA_OPTS,
                "--set", "fileServer.resources.requests.memory=384Mi",
                "--set", "fileServer.resources.requests.cpu=0.5",
                "--set", "fileServer.resources.limits.memory=384Mi",
                "--set", "fileServer.resources.limits.cpu=0.5",
                "--set", "fileServer.storage.size=1Gi",
                "--set", "image.pullPolicy=Never"
        );
        pb.redirectErrorStream(true);
        Process process = pb.start();
        String output;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            output = reader.lines().collect(Collectors.joining("\n"));
        }
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("helm template failed (exit=" + exitCode + "): " + output);
        }
        return output;
    }
}
