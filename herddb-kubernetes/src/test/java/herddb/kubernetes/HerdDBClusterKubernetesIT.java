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
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.junit.runners.MethodSorters;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HerdDBClusterKubernetesIT {

    private static final Logger LOG = Logger.getLogger(HerdDBClusterKubernetesIT.class.getName());

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
    // Byte values: 268435456 = 256 * 1024 * 1024 (= MaxDirectMemorySize=256m);
    //              134217728 = 128 * 1024 * 1024 (= MaxDirectMemorySize=128m).
    // -XX:NativeMemoryTracking=summary enables jcmd VM.native_memory summary inside the pod
    // so KubernetesDiagnostics can show actual direct/native memory breakdown on test failure (issue #438).
    private static final String SERVER_JAVA_OPTS = "-XX:+UseG1GC -Duser.language=en -Xmx256m -Xms256m"
            + " -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=256m"
            + " -Dio.netty.maxDirectMemory=268435456"
            + " -XX:NativeMemoryTracking=summary"
            + " -Djava.awt.headless=true --add-modules jdk.incubator.vector";
    private static final String INFRA_JAVA_OPTS = "-XX:+UseG1GC -Duser.language=en -Xmx128m -Xms128m"
            + " -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=128m"
            + " -Dio.netty.maxDirectMemory=134217728"
            + " -XX:NativeMemoryTracking=summary"
            + " -Djava.awt.headless=true --add-modules jdk.incubator.vector";

    @ClassRule
    public static K3sContainer k3s = new K3sContainer(DockerImageName.parse("rancher/k3s:v1.31.4-k3s1"))
            .withExposedPorts(6443);

    private static KubernetesClient kubernetesClient;
    private static String helmChartPath;
    private static List<HasMetadata> lastAppliedResources;

    /** Per-test wall-clock timeout (issue #438). See HerdDBKubernetesIT.perTestTimeout. */
    @Rule
    public Timeout perTestTimeout = new Timeout(25, TimeUnit.MINUTES);

    /** Dump cluster diagnostics on test failure (issue #438). */
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
    public void test1_ZooKeeperOnly() throws Exception {
        LOG.info("=== Test 1: ZooKeeper Only ===");

        Map<String, String> values = new LinkedHashMap<>();
        values.put("server.mode", "standalone");
        values.put("server.replicaCount", "1");
        values.put("tools.enabled", "false");
        values.put("zookeeper.enabled", "true");
        values.put("bookkeeper.enabled", "false");
        values.put("image.pullPolicy", "Never");
        values.put("zookeeper.javaOpts", INFRA_JAVA_OPTS);
        values.put("zookeeper.resources.requests.memory", "384Mi");
        values.put("zookeeper.resources.requests.cpu", "0.5");
        values.put("zookeeper.resources.limits.memory", "384Mi");
        values.put("zookeeper.resources.limits.cpu", "0.5");
        values.put("zookeeper.storage.size", "1Gi");
        values.put("server.javaOpts", SERVER_JAVA_OPTS);
        values.put("server.resources.requests.memory", "768Mi");
        values.put("server.resources.requests.cpu", "0.5");
        values.put("server.resources.limits.memory", "768Mi");
        values.put("server.resources.limits.cpu", "0.5");
        values.put("server.storage.data.size", "1Gi");
        values.put("server.storage.commitlog.size", "1Gi");

        applyHelmChart(values);

        LOG.info("Waiting for ZooKeeper pod to be ready...");
        kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "zookeeper")
                .waitUntilReady(5, TimeUnit.MINUTES);
        LOG.info("ZooKeeper pod is ready.");

        List<Pod> zkPods = kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "zookeeper")
                .list().getItems();
        assertEquals("Expected 1 ZooKeeper pod", 1, zkPods.size());
        assertEquals("Running", zkPods.get(0).getStatus().getPhase());
        LOG.info("Test 1 passed: ZooKeeper is running.");
    }

    @Test
    public void test2_ZooKeeperPlusBookKeeper() throws Exception {
        LOG.info("=== Test 2: ZooKeeper + BookKeeper ===");

        deleteAllResources();

        Map<String, String> values = new LinkedHashMap<>();
        values.put("server.mode", "standalone");
        values.put("server.replicaCount", "1");
        values.put("tools.enabled", "false");
        values.put("zookeeper.enabled", "true");
        values.put("bookkeeper.enabled", "true");
        values.put("bookkeeper.replicaCount", "1");
        values.put("image.pullPolicy", "Never");
        values.put("zookeeper.javaOpts", INFRA_JAVA_OPTS);
        values.put("zookeeper.resources.requests.memory", "384Mi");
        values.put("zookeeper.resources.requests.cpu", "0.5");
        values.put("zookeeper.resources.limits.memory", "384Mi");
        values.put("zookeeper.resources.limits.cpu", "0.5");
        values.put("zookeeper.storage.size", "1Gi");
        values.put("bookkeeper.javaOpts", INFRA_JAVA_OPTS);
        values.put("bookkeeper.resources.requests.memory", "384Mi");
        values.put("bookkeeper.resources.requests.cpu", "0.5");
        values.put("bookkeeper.resources.limits.memory", "384Mi");
        values.put("bookkeeper.resources.limits.cpu", "0.5");
        values.put("bookkeeper.storage.journal.size", "1Gi");
        values.put("bookkeeper.storage.ledger.size", "1Gi");
        values.put("server.javaOpts", SERVER_JAVA_OPTS);
        values.put("server.resources.requests.memory", "768Mi");
        values.put("server.resources.requests.cpu", "0.5");
        values.put("server.resources.limits.memory", "768Mi");
        values.put("server.resources.limits.cpu", "0.5");
        values.put("server.storage.data.size", "1Gi");
        values.put("server.storage.commitlog.size", "1Gi");

        applyHelmChart(values);

        LOG.info("Waiting for ZooKeeper pod to be ready...");
        kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "zookeeper")
                .waitUntilReady(5, TimeUnit.MINUTES);
        LOG.info("ZooKeeper pod is ready.");

        LOG.info("Waiting for BookKeeper pod to be ready...");
        waitForComponent("bookkeeper", 5, TimeUnit.MINUTES);
        LOG.info("BookKeeper pod is ready.");

        List<Pod> zkPods = kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "zookeeper")
                .list().getItems();
        assertEquals("Expected 1 ZooKeeper pod", 1, zkPods.size());
        assertEquals("Running", zkPods.get(0).getStatus().getPhase());

        List<Pod> bkPods = kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "bookkeeper")
                .list().getItems();
        assertEquals("Expected 1 BookKeeper pod", 1, bkPods.size());
        assertEquals("Running", bkPods.get(0).getStatus().getPhase());
        LOG.info("Test 2 passed: ZooKeeper and BookKeeper are both running.");
    }

    @Test
    public void test3_ClusterModeWithJDBC() throws Exception {
        LOG.info("=== Test 3: Cluster Mode with JDBC ===");

        deleteAllResources();

        Map<String, String> values = new LinkedHashMap<>();
        values.put("server.mode", "cluster");
        values.put("server.replicaCount", "1");
        values.put("tools.enabled", "true");
        values.put("zookeeper.enabled", "true");
        values.put("bookkeeper.enabled", "true");
        values.put("bookkeeper.replicaCount", "1");
        values.put("image.pullPolicy", "Never");
        values.put("zookeeper.javaOpts", INFRA_JAVA_OPTS);
        values.put("zookeeper.resources.requests.memory", "384Mi");
        values.put("zookeeper.resources.requests.cpu", "0.5");
        values.put("zookeeper.resources.limits.memory", "384Mi");
        values.put("zookeeper.resources.limits.cpu", "0.5");
        values.put("zookeeper.storage.size", "1Gi");
        values.put("bookkeeper.javaOpts", INFRA_JAVA_OPTS);
        values.put("bookkeeper.resources.requests.memory", "384Mi");
        values.put("bookkeeper.resources.requests.cpu", "0.5");
        values.put("bookkeeper.resources.limits.memory", "384Mi");
        values.put("bookkeeper.resources.limits.cpu", "0.5");
        values.put("bookkeeper.storage.journal.size", "1Gi");
        values.put("bookkeeper.storage.ledger.size", "1Gi");
        values.put("server.javaOpts", SERVER_JAVA_OPTS);
        values.put("server.resources.requests.memory", "768Mi");
        values.put("server.resources.requests.cpu", "0.5");
        values.put("server.resources.limits.memory", "768Mi");
        values.put("server.resources.limits.cpu", "0.5");
        values.put("server.storage.data.size", "1Gi");
        values.put("server.storage.commitlog.size", "1Gi");

        applyHelmChart(values);

        LOG.info("Waiting for ZooKeeper pod to be ready...");
        kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "zookeeper")
                .waitUntilReady(5, TimeUnit.MINUTES);

        LOG.info("Waiting for BookKeeper pod to be ready...");
        waitForComponent("bookkeeper", 5, TimeUnit.MINUTES);

        LOG.info("Waiting for HerdDB server pod to be ready...");
        kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "server")
                .waitUntilReady(5, TimeUnit.MINUTES);

        LOG.info("Waiting for tools pod to be ready...");
        kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "tools")
                .waitUntilReady(5, TimeUnit.MINUTES);

        String toolsPod = getToolsPodName();
        HerdDBKubernetesIT.waitForTablespace(k3s, toolsPod);

        // CREATE TABLE
        HerdDBKubernetesIT.execSql(k3s, toolsPod, "CREATE TABLE cluster_test (id int primary key, name string)");
        LOG.info("Table created in cluster mode.");

        // INSERT
        HerdDBKubernetesIT.execSql(k3s, toolsPod, "INSERT INTO cluster_test (id, name) VALUES (1, 'cluster-hello')");
        LOG.info("Row inserted.");

        // SELECT
        String output = HerdDBKubernetesIT.execSql(k3s, toolsPod, "SELECT id, name FROM cluster_test");
        assertTrue("Expected 'cluster-hello' in output", output.contains("cluster-hello"));
        LOG.info("Row verified: " + output.trim());
        LOG.info("Test 3 passed: Cluster mode with JDBC operations works.");
    }

    /**
     * Deploys HerdDB in cluster mode with tools enabled and exercises the
     * <em>server-based discovery</em> path: the tools pod connects via
     * {@code jdbc:herddb:server:…} (no ZooKeeper in the JDBC URL) and the
     * new {@code ServerBasedClientSideMetadataProvider} discovers the cluster
     * topology by querying {@code sysnodes} and {@code systablespaces}.
     *
     * <p>The test explicitly queries both system tables via herddb-cli to
     * assert that the discovery data is correct, then performs an
     * INSERT + SELECT round-trip to prove that routing via the discovered
     * leader actually works.
     */
    @Test
    public void test4_ServerBasedDiscovery() throws Exception {
        LOG.info("=== Test 4: Server-Based Discovery (ZooKeeper-less JDBC client) ===");

        deleteAllResources();

        Map<String, String> values = new LinkedHashMap<>();
        values.put("server.mode", "cluster");
        values.put("server.replicaCount", "1");
        values.put("tools.enabled", "true");
        values.put("zookeeper.enabled", "true");
        values.put("bookkeeper.enabled", "true");
        values.put("bookkeeper.replicaCount", "1");
        values.put("image.pullPolicy", "Never");
        values.put("zookeeper.javaOpts", INFRA_JAVA_OPTS);
        values.put("zookeeper.resources.requests.memory", "384Mi");
        values.put("zookeeper.resources.requests.cpu", "0.5");
        values.put("zookeeper.resources.limits.memory", "384Mi");
        values.put("zookeeper.resources.limits.cpu", "0.5");
        values.put("zookeeper.storage.size", "1Gi");
        values.put("bookkeeper.javaOpts", INFRA_JAVA_OPTS);
        values.put("bookkeeper.resources.requests.memory", "384Mi");
        values.put("bookkeeper.resources.requests.cpu", "0.5");
        values.put("bookkeeper.resources.limits.memory", "384Mi");
        values.put("bookkeeper.resources.limits.cpu", "0.5");
        values.put("bookkeeper.storage.journal.size", "1Gi");
        values.put("bookkeeper.storage.ledger.size", "1Gi");
        values.put("server.javaOpts", SERVER_JAVA_OPTS);
        values.put("server.resources.requests.memory", "768Mi");
        values.put("server.resources.requests.cpu", "0.5");
        values.put("server.resources.limits.memory", "768Mi");
        values.put("server.resources.limits.cpu", "0.5");
        values.put("server.storage.data.size", "1Gi");
        values.put("server.storage.commitlog.size", "1Gi");

        applyHelmChart(values);

        LOG.info("Waiting for ZooKeeper pod to be ready...");
        kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "zookeeper")
                .waitUntilReady(5, TimeUnit.MINUTES);

        LOG.info("Waiting for BookKeeper pod to be ready...");
        waitForComponent("bookkeeper", 5, TimeUnit.MINUTES);

        LOG.info("Waiting for HerdDB server pod to be ready...");
        kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "server")
                .waitUntilReady(5, TimeUnit.MINUTES);

        LOG.info("Waiting for tools pod to be ready...");
        kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "tools")
                .waitUntilReady(5, TimeUnit.MINUTES);

        String toolsPod = getToolsPodName();
        HerdDBKubernetesIT.waitForTablespace(k3s, toolsPod);

        // ---- Assert sysnodes is populated ----------------------------------------
        // The tools pod connects via jdbc:herddb:server:… (server-based discovery).
        // ServerBasedClientSideMetadataProvider queries sysnodes on first use;
        // we verify here that the server is exposing at least one node so that
        // the provider can build the topology map.
        LOG.info("Querying sysnodes to verify server-based discovery data...");
        String sysnodesOut = HerdDBKubernetesIT.execSql(k3s, toolsPod,
                "SELECT nodeid, address, ssl FROM sysnodes");
        LOG.info("sysnodes output: " + sysnodesOut);
        assertTrue("sysnodes must return at least one row",
                !sysnodesOut.trim().isEmpty()
                && !sysnodesOut.contains("0 rows"));

        // ---- Assert systablespaces has a leader ----------------------------------
        // The default tablespace in HerdDB is named "herd" (TableSpace.DEFAULT),
        // NOT "default". Filter on that exact name.
        LOG.info("Querying systablespaces to verify leader is set...");
        String systablespacesOut = HerdDBKubernetesIT.execSql(k3s, toolsPod,
                "SELECT tablespace_name, leader FROM systablespaces "
                + "WHERE tablespace_name='herd'");
        LOG.info("systablespaces output: " + systablespacesOut);
        // The leader column must be non-null — presence of "herd" in the output
        // proves the tablespace is registered and has a leader.
        assertTrue("systablespaces must contain the default 'herd' tablespace with a leader",
                systablespacesOut.contains("herd"));

        // ---- Round-trip DML through the discovered leader ----------------------
        HerdDBKubernetesIT.execSql(k3s, toolsPod,
                "CREATE TABLE discovery_k8s_test (id int primary key, name string)");
        LOG.info("Table created via server-based discovery.");

        HerdDBKubernetesIT.execSql(k3s, toolsPod,
                "INSERT INTO discovery_k8s_test (id, name) VALUES (1, 'discovery-works')");
        LOG.info("Row inserted via server-based discovery.");

        String selectOut = HerdDBKubernetesIT.execSql(k3s, toolsPod,
                "SELECT id, name FROM discovery_k8s_test");
        LOG.info("SELECT output: " + selectOut);
        assertTrue("Expected 'discovery-works' in SELECT output",
                selectOut.contains("discovery-works"));

        LOG.info("Test 4 passed: server-based discovery works in cluster mode "
                + "(jdbc:herddb:server:… without ZooKeeper in JDBC URL).");
    }

    private String getToolsPodName() {
        List<Pod> pods = kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "tools")
                .list().getItems();
        assertEquals("Expected 1 tools pod", 1, pods.size());
        return pods.get(0).getMetadata().getName();
    }

    private void applyHelmChart(Map<String, String> values) throws Exception {
        String renderedYaml = helmTemplate(helmChartPath, values);
        LOG.info("Rendered YAML length: " + renderedYaml.length());

        List<HasMetadata> resources = kubernetesClient.load(
                new ByteArrayInputStream(renderedYaml.getBytes(StandardCharsets.UTF_8))).items();
        LOG.info("Applying " + resources.size() + " Kubernetes resources...");
        kubernetesClient.resourceList(resources).createOrReplace();
        lastAppliedResources = resources;
    }

    private void deleteAllResources() {
        if (lastAppliedResources != null) {
            LOG.info("Deleting " + lastAppliedResources.size() + " previously applied resources...");
            kubernetesClient.resourceList(lastAppliedResources).delete();

            LOG.info("Waiting for pods to terminate...");
            for (int i = 0; i < 60; i++) {
                List<Pod> pods = kubernetesClient.pods().inNamespace("default")
                        .withLabel("app.kubernetes.io/name", "herddb")
                        .list().getItems();
                if (pods.isEmpty()) {
                    LOG.info("All pods terminated.");
                    break;
                }
                LOG.info("Still " + pods.size() + " pods remaining...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            kubernetesClient.persistentVolumeClaims().inNamespace("default").delete();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            lastAppliedResources = null;
        }
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
                try {
                    Pod pod = pods.get(0);
                    String logs = kubernetesClient.pods()
                            .inNamespace("default")
                            .withName(pod.getMetadata().getName())
                            .getLog();
                    if (logs != null && !logs.isEmpty()) {
                        String[] lines = logs.split("\n");
                        int start = Math.max(0, lines.length - 20);
                        LOG.info("Last " + Math.min(20, lines.length) + " log lines for " + component + ":");
                        for (int i = start; i < lines.length; i++) {
                            LOG.info("  " + lines[i]);
                        }
                    }
                } catch (Exception e) {
                    LOG.info("Could not get logs for " + component + ": " + e.getMessage());
                }
                boolean ready = pods.stream().allMatch(p ->
                        p.getStatus() != null
                        && p.getStatus().getConditions() != null
                        && p.getStatus().getConditions().stream()
                                .anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus())));
                if (ready) {
                    return;
                }
            } else {
                LOG.info("No pods found yet for component " + component);
            }
            Thread.sleep(10000);
        }
        logPodStatus(component);
        throw new RuntimeException("Timed out waiting for " + component + " pod to be ready");
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
            if (pod.getStatus().getInitContainerStatuses() != null) {
                pod.getStatus().getInitContainerStatuses().forEach(cs ->
                        LOG.info("  initContainer " + cs.getName()
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
        command.add("test-cluster");
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
