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
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
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

    private static final String SERVER_JAVA_OPTS = "-XX:+UseG1GC -Duser.language=en -Xmx256m -Xms256m"
            + " -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=128m"
            + " -Djava.awt.headless=true --add-modules jdk.incubator.vector";
    private static final String INFRA_JAVA_OPTS = "-XX:+UseG1GC -Duser.language=en -Xmx128m -Xms128m"
            + " -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=64m"
            + " -Djava.awt.headless=true --add-modules jdk.incubator.vector";

    @ClassRule
    public static K3sContainer k3s = new K3sContainer(DockerImageName.parse("rancher/k3s:v1.31.4-k3s1"))
            .withExposedPorts(6443);

    private static KubernetesClient kubernetesClient;
    private static String helmChartPath;
    private static List<HasMetadata> lastAppliedResources;

    @BeforeClass
    public static void setup() throws Exception {
        Process checkImage = new ProcessBuilder("docker", "image", "inspect", FULL_IMAGE)
                .redirectErrorStream(true)
                .start();
        int exitCode = checkImage.waitFor();
        assumeTrue("Docker image " + FULL_IMAGE + " must be built first "
                + "(run: mvn package jib:dockerBuild@build -pl herddb-docker)", exitCode == 0);

        Path imageTar = Files.createTempFile("herddb-image", ".tar");
        try {
            LOG.info("Saving docker image to tarball...");
            Process save = new ProcessBuilder("docker", "save", FULL_IMAGE, "-o", imageTar.toString())
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
        values.put("zookeeper.resources.requests.memory", "256Mi");
        values.put("zookeeper.resources.requests.cpu", "0.5");
        values.put("zookeeper.resources.limits.memory", "256Mi");
        values.put("zookeeper.resources.limits.cpu", "0.5");
        values.put("zookeeper.storage.size", "1Gi");
        values.put("server.javaOpts", SERVER_JAVA_OPTS);
        values.put("server.resources.requests.memory", "512Mi");
        values.put("server.resources.requests.cpu", "0.5");
        values.put("server.resources.limits.memory", "512Mi");
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
        values.put("zookeeper.resources.requests.memory", "256Mi");
        values.put("zookeeper.resources.requests.cpu", "0.5");
        values.put("zookeeper.resources.limits.memory", "256Mi");
        values.put("zookeeper.resources.limits.cpu", "0.5");
        values.put("zookeeper.storage.size", "1Gi");
        values.put("bookkeeper.javaOpts", INFRA_JAVA_OPTS);
        values.put("bookkeeper.resources.requests.memory", "256Mi");
        values.put("bookkeeper.resources.requests.cpu", "0.5");
        values.put("bookkeeper.resources.limits.memory", "256Mi");
        values.put("bookkeeper.resources.limits.cpu", "0.5");
        values.put("bookkeeper.storage.journal.size", "1Gi");
        values.put("bookkeeper.storage.ledger.size", "1Gi");
        values.put("server.javaOpts", SERVER_JAVA_OPTS);
        values.put("server.resources.requests.memory", "512Mi");
        values.put("server.resources.requests.cpu", "0.5");
        values.put("server.resources.limits.memory", "512Mi");
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
        values.put("zookeeper.resources.requests.memory", "256Mi");
        values.put("zookeeper.resources.requests.cpu", "0.5");
        values.put("zookeeper.resources.limits.memory", "256Mi");
        values.put("zookeeper.resources.limits.cpu", "0.5");
        values.put("zookeeper.storage.size", "1Gi");
        values.put("bookkeeper.javaOpts", INFRA_JAVA_OPTS);
        values.put("bookkeeper.resources.requests.memory", "256Mi");
        values.put("bookkeeper.resources.requests.cpu", "0.5");
        values.put("bookkeeper.resources.limits.memory", "256Mi");
        values.put("bookkeeper.resources.limits.cpu", "0.5");
        values.put("bookkeeper.storage.journal.size", "1Gi");
        values.put("bookkeeper.storage.ledger.size", "1Gi");
        values.put("server.javaOpts", SERVER_JAVA_OPTS);
        values.put("server.resources.requests.memory", "512Mi");
        values.put("server.resources.requests.cpu", "0.5");
        values.put("server.resources.limits.memory", "512Mi");
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
        HerdDBKubernetesIT.waitForTablespace(toolsPod);

        // CREATE TABLE
        HerdDBKubernetesIT.execSql(toolsPod, "CREATE TABLE cluster_test (id int primary key, name string)");
        LOG.info("Table created in cluster mode.");

        // INSERT
        HerdDBKubernetesIT.execSql(toolsPod, "INSERT INTO cluster_test (id, name) VALUES (1, 'cluster-hello')");
        LOG.info("Row inserted.");

        // SELECT
        String output = HerdDBKubernetesIT.execSql(toolsPod, "SELECT id, name FROM cluster_test");
        assertTrue("Expected 'cluster-hello' in output", output.contains("cluster-hello"));
        LOG.info("Row verified: " + output.trim());
        LOG.info("Test 3 passed: Cluster mode with JDBC operations works.");
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
