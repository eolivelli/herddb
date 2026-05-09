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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

/**
 * Kubernetes integration test for the index-optimizer service.
 *
 * <p>Validates:
 * <ul>
 *   <li>The optimizer pod starts successfully when configured with
 *       {@code indexOptimizer.tablespaceName=herd} — i.e. it resolves the tablespace
 *       UUID from ZooKeeper at startup (issue #481) without the operator having to
 *       manually look up the UUID.</li>
 *   <li>The optimizer pod reaches {@code Ready} state (its {@code /health} readiness
 *       probe passes, confirming no {@code NumberFormatException} on startup — issue #480).</li>
 *   <li>The optimizer engine ticks at least once after a vector table and index are
 *       created, confirming the segment registry scan loop is functional.</li>
 * </ul>
 */
public class IndexOptimizerKubernetesIT {

    private static final Logger LOG = Logger.getLogger(IndexOptimizerKubernetesIT.class.getName());

    private static final String IMAGE_NAME = "herddb/herddb-server";
    private static final String IMAGE_TAG = "0.30.0-SNAPSHOT";
    private static final String FULL_IMAGE = IMAGE_NAME + ":" + IMAGE_TAG;

    /**
     * JVM options shared by ZooKeeper, BookKeeper, and the indexing service pods.
     * Keeping heap small so the test runs on a single-node k3s-in-docker setup.
     * -Dio.netty.maxDirectMemory must match MaxDirectMemorySize so Netty uses the
     * Unsafe no-cleaner path (see issue #253).
     */
    private static final String INFRA_JAVA_OPTS =
            "-XX:+UseG1GC -Duser.language=en -Xmx128m -Xms128m"
            + " -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=128m"
            + " -Dio.netty.maxDirectMemory=134217728"
            + " -XX:NativeMemoryTracking=summary"
            + " -Djava.awt.headless=true --add-modules jdk.incubator.vector";

    private static final String SERVER_JAVA_OPTS =
            "-XX:+UseG1GC -Duser.language=en -Xmx256m -Xms256m"
            + " -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=256m"
            + " -Dio.netty.maxDirectMemory=268435456"
            + " -XX:NativeMemoryTracking=summary"
            + " -Djava.awt.headless=true --add-modules jdk.incubator.vector";

    @ClassRule
    public static K3sContainer k3s = new K3sContainer(DockerImageName.parse("rancher/k3s:v1.31.4-k3s1"))
            .withExposedPorts(6443);

    private static KubernetesClient kubernetesClient;
    private static String helmChartPath;

    @Rule
    public Timeout perTestTimeout = new Timeout(25, TimeUnit.MINUTES);

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
        Process checkImage = dockerProcess("docker", "image", "inspect", FULL_IMAGE)
                .redirectErrorStream(true)
                .start();
        int exitCode = checkImage.waitFor();
        assumeTrue("Docker image " + FULL_IMAGE + " must be built first "
                + "(run: mvn package jib:dockerBuild@build -pl herddb-docker)", exitCode == 0);

        Path imageTar = Files.createTempFile("herddb-image", ".tar");
        try {
            LOG.info("Saving docker image to tarball...");
            Process save = dockerProcess("docker", "save", FULL_IMAGE, "-o", imageTar.toString())
                    .redirectErrorStream(true)
                    .start();
            assertEquals("docker save failed", 0, save.waitFor());

            LOG.info("Loading image into K3S...");
            k3s.copyFileToContainer(MountableFile.forHostPath(imageTar), "/tmp/herddb.tar");
            k3s.execInContainer("ctr", "--address", "/run/k3s/containerd/containerd.sock",
                    "--namespace", "k8s.io", "images", "import", "/tmp/herddb.tar");
        } finally {
            Files.deleteIfExists(imageTar);
        }

        String kubeConfigYaml = k3s.getKubeConfigYaml();
        Config config = Config.fromKubeconfig(kubeConfigYaml);
        config.setNamespace("default");
        kubernetesClient = new KubernetesClientBuilder().withConfig(config).build();

        helmChartPath = findHelmChartPath();
        LOG.info("Using helm chart at: " + helmChartPath);
    }

    @AfterClass
    public static void tearDown() {
        if (kubernetesClient != null) {
            kubernetesClient.close();
        }
    }

    @Test
    public void testIndexOptimizerStartsAndTicks() throws Exception {
        LOG.info("=== Test: Index Optimizer starts, resolves tablespace name, and ticks ===");

        Map<String, String> values = new LinkedHashMap<>();
        values.put("image.pullPolicy", "Never");

        // ZooKeeper
        values.put("zookeeper.enabled", "true");
        values.put("zookeeper.javaOpts", INFRA_JAVA_OPTS);
        values.put("zookeeper.resources.requests.memory", "384Mi");
        values.put("zookeeper.resources.requests.cpu", "0.5");
        values.put("zookeeper.resources.limits.memory", "384Mi");
        values.put("zookeeper.resources.limits.cpu", "0.5");
        values.put("zookeeper.storage.size", "1Gi");

        // BookKeeper
        values.put("bookkeeper.enabled", "true");
        values.put("bookkeeper.replicaCount", "1");
        values.put("bookkeeper.javaOpts", INFRA_JAVA_OPTS);
        values.put("bookkeeper.resources.requests.memory", "384Mi");
        values.put("bookkeeper.resources.requests.cpu", "0.5");
        values.put("bookkeeper.resources.limits.memory", "384Mi");
        values.put("bookkeeper.resources.limits.cpu", "0.5");
        values.put("bookkeeper.storage.journal.size", "1Gi");
        values.put("bookkeeper.storage.ledger.size", "1Gi");

        // HerdDB server: cluster mode, local storage
        values.put("server.mode", "cluster");
        values.put("server.storageMode", "local");
        values.put("server.replicaCount", "1");
        values.put("server.javaOpts", SERVER_JAVA_OPTS);
        values.put("server.resources.requests.memory", "768Mi");
        values.put("server.resources.requests.cpu", "0.5");
        values.put("server.resources.limits.memory", "768Mi");
        values.put("server.resources.limits.cpu", "0.5");
        values.put("server.storage.data.size", "1Gi");
        values.put("server.storage.commitlog.size", "1Gi");

        // IndexingService: 1 replica (needed by server for vector index)
        values.put("indexingService.enabled", "true");
        values.put("indexingService.replicaCount", "1");
        values.put("indexingService.javaOpts", INFRA_JAVA_OPTS);
        values.put("indexingService.resources.requests.memory", "384Mi");
        values.put("indexingService.resources.requests.cpu", "0.5");
        values.put("indexingService.resources.limits.memory", "384Mi");
        values.put("indexingService.resources.limits.cpu", "0.5");
        values.put("indexingService.storage.data.size", "1Gi");
        values.put("indexingService.storage.log.size", "1Gi");

        // Index Optimizer: enabled, configured by name (issue #481); short tick interval.
        values.put("indexOptimizer.enabled", "true");
        values.put("indexOptimizer.tablespaceName", "herd");
        // 10 seconds so we see ticks quickly during the test
        values.put("indexOptimizer.intervalMs", "10000");
        // Issue #484: configure the merger explicitly so the optimizer
        // constructs a real RemoteSegmentMerger (not the NoopMerger
        // fallback) and the production wiring is exercised end-to-end. The
        // dim is arbitrary because this IT doesn't ingest data — the merger
        // never actually runs against a non-empty registry.
        values.put("indexOptimizer.merger.dim", "64");
        values.put("indexOptimizer.javaOpts", INFRA_JAVA_OPTS);
        values.put("indexOptimizer.resources.requests.memory", "384Mi");
        values.put("indexOptimizer.resources.requests.cpu", "0.5");
        values.put("indexOptimizer.resources.limits.memory", "384Mi");
        values.put("indexOptimizer.resources.limits.cpu", "0.5");
        values.put("indexOptimizer.storage.tmp.size", "1Gi");

        // Tools pod for SQL execution
        values.put("tools.enabled", "true");
        values.put("tools.resources.requests.memory", "256Mi");
        values.put("tools.resources.requests.cpu", "0.5");
        values.put("tools.resources.limits.memory", "256Mi");
        values.put("tools.resources.limits.cpu", "0.5");

        String renderedYaml = helmTemplate(helmChartPath, values);
        LOG.info("Rendered YAML length: " + renderedYaml.length());

        List<HasMetadata> resources = kubernetesClient.load(
                new ByteArrayInputStream(renderedYaml.getBytes(StandardCharsets.UTF_8))).items();
        LOG.info("Applying " + resources.size() + " Kubernetes resources...");
        kubernetesClient.resourceList(resources).createOrReplace();

        LOG.info("Waiting for ZooKeeper...");
        waitForComponent("zookeeper", 5, TimeUnit.MINUTES);
        LOG.info("Waiting for BookKeeper...");
        waitForComponent("bookkeeper", 5, TimeUnit.MINUTES);
        LOG.info("Waiting for IndexingService...");
        waitForComponent("indexing-service", 5, TimeUnit.MINUTES);
        LOG.info("Waiting for HerdDB server...");
        waitForComponent("server", 5, TimeUnit.MINUTES);
        LOG.info("Waiting for tools pod...");
        kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "tools")
                .waitUntilReady(5, TimeUnit.MINUTES);

        String toolsPod = getComponentPodName("tools");

        // Wait until the cluster is ready (default tablespace 'herd' exists in ZK).
        HerdDBKubernetesIT.waitForTablespace(k3s, toolsPod);

        // Now wait for the optimizer pod. By the time ZK + BK + server are up, the
        // 'herd' tablespace exists, so the optimizer can resolve its UUID on startup.
        LOG.info("Waiting for index-optimizer pod...");
        waitForComponent("index-optimizer", 5, TimeUnit.MINUTES);
        LOG.info("Index-optimizer pod is Ready — health endpoint returned 200.");

        // Create a vector table and index. The optimizer's runs counter increments at
        // the start of every runOnce() tick regardless of whether any segments exist,
        // so merely having a running engine is enough to assert liveness.
        HerdDBKubernetesIT.execSql(k3s, toolsPod,
                "CREATE TABLE opt_test (id int primary key, vec floata not null)");
        HerdDBKubernetesIT.execSql(k3s, toolsPod,
                "CREATE VECTOR INDEX vidx_opt ON opt_test(vec)");
        LOG.info("Vector table and index created.");

        // Wait for the optimizer to tick at least once (intervalMs=10s → 3 ticks in 60s).
        String optimizerPod = getComponentPodName("index-optimizer");
        LOG.info("Waiting for optimizer to tick (checking /metrics)...");
        long runs = waitForOptimizerTick(optimizerPod, 2, TimeUnit.MINUTES);
        assertTrue("Expected at least 1 optimizer tick but got " + runs
                + " — see 'Last /metrics body' WARNING log above for the raw curl response",
                runs >= 1);

        LOG.info("Test passed: index-optimizer started successfully, resolved 'herd' tablespace "
                + "by name (no UUID needed), and the engine ticked " + runs + " time(s).");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Waits up to {@code timeout} for the optimizer's {@code /metrics} endpoint to
     * report {@code herddb_optimizer_runs_total >= 1}, then returns the current value.
     */
    private long waitForOptimizerTick(String podName, long timeout, TimeUnit unit) throws Exception {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        String lastMetricsBody = "(no response received yet)";
        while (System.currentTimeMillis() < deadline) {
            try {
                // -f: fail on HTTP errors, -s: suppress progress, -S: show errors.
                // Together they exit non-zero on 4xx/5xx so the catch block logs it;
                // without -f an error body would be captured as if it were metrics.
                org.testcontainers.containers.Container.ExecResult result =
                        k3s.execInContainer("kubectl", "exec", podName, "--",
                                "curl", "-fsS", "http://localhost:9853/metrics");
                lastMetricsBody = result.getStdout();
                LOG.log(Level.FINE, "metrics response (exit={0}): {1}",
                        new Object[]{result.getExitCode(), lastMetricsBody});
                // Look for a line like: herddb_optimizer_runs_total 3
                for (String line : lastMetricsBody.split("\n")) {
                    if (line.startsWith("herddb_optimizer_runs_total ")) {
                        String[] parts = line.trim().split("\\s+");
                        if (parts.length >= 2) {
                            long val = Long.parseLong(parts[1].trim());
                            if (val >= 1) {
                                return val;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                lastMetricsBody = "curl error: " + e.getMessage();
                LOG.log(Level.FINE, "metrics check failed (will retry): {0}", e.getMessage());
            }
            Thread.sleep(5_000);
        }
        // Timed out — log the last response at WARNING so CI logs contain enough context.
        LOG.warning("waitForOptimizerTick timed out. Last /metrics body: " + lastMetricsBody);
        return 0L;
    }

    private String getComponentPodName(String component) {
        List<Pod> pods = kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", component)
                .list().getItems();
        assertEquals("Expected exactly 1 pod for component " + component, 1, pods.size());
        return pods.get(0).getMetadata().getName();
    }

    private void waitForComponent(String component, long timeout, TimeUnit unit) throws Exception {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        while (System.currentTimeMillis() < deadline) {
            List<Pod> pods = kubernetesClient.pods()
                    .inNamespace("default")
                    .withLabel("app.kubernetes.io/component", component)
                    .list().getItems();
            if (!pods.isEmpty()) {
                logPodStatus(component);
                boolean ready = pods.stream().allMatch(p ->
                        p.getStatus() != null
                        && p.getStatus().getConditions() != null
                        && p.getStatus().getConditions().stream()
                                .anyMatch(c -> "Ready".equals(c.getType())
                                        && "True".equals(c.getStatus())));
                if (ready) {
                    return;
                }
            } else {
                LOG.info("No pods found yet for component " + component);
            }
            Thread.sleep(10_000);
        }
        logPodStatus(component);
        throw new RuntimeException("Timed out waiting for " + component + " pod(s) to be ready");
    }

    private void logPodStatus(String component) {
        List<Pod> pods = kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", component)
                .list().getItems();
        for (Pod pod : pods) {
            LOG.info("Pod " + pod.getMetadata().getName()
                    + " phase=" + pod.getStatus().getPhase()
                    + " conditions=" + pod.getStatus().getConditions());
            if (pod.getStatus().getContainerStatuses() != null) {
                pod.getStatus().getContainerStatuses().forEach(cs ->
                        LOG.info("  container " + cs.getName()
                                + " ready=" + cs.getReady()
                                + " restartCount=" + cs.getRestartCount()
                                + " state=" + cs.getState()));
            }
        }
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

    private static String helmTemplate(String chartPath, Map<String, String> values) throws Exception {
        List<String> command = new ArrayList<>();
        command.add("helm");
        command.add("template");
        command.add("test-opt");
        command.add(chartPath);
        for (Map.Entry<String, String> entry : values.entrySet()) {
            command.add("--set");
            command.add(entry.getKey() + "=" + entry.getValue());
        }
        ProcessBuilder pb = new ProcessBuilder(command);
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
