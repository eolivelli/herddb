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
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Kubernetes integration test: cluster mode with 2 IndexingService replicas
 * discovered via ZooKeeper for vector search.
 *
 * <p>Validates: ZK, BookKeeper, 2 indexing services (ZK-registered),
 * HerdDB server with local storage, vector index creation, ingest,
 * checkpoint, and ANN search with recall check.
 */
public class HerdDBVectorKubernetesIT {

    private static final Logger LOG = Logger.getLogger(HerdDBVectorKubernetesIT.class.getName());

    private static final String IMAGE_NAME = "herddb/herddb-server";
    private static final String IMAGE_TAG = "0.30.0-SNAPSHOT";
    private static final String FULL_IMAGE = IMAGE_NAME + ":" + IMAGE_TAG;
    private static final int NODE_PORT = 30009;

    private static final String JAVA_OPTS = "-XX:+UseG1GC -Duser.language=en -Xmx128m -Xms128m"
            + " -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=64m"
            + " -Djava.awt.headless=true --add-modules jdk.incubator.vector";

    @ClassRule
    public static K3sContainer k3s = new K3sContainer(DockerImageName.parse("rancher/k3s:v1.31.4-k3s1"))
            .withExposedPorts(6443, NODE_PORT);

    private static KubernetesClient kubernetesClient;
    private static String helmChartPath;

    @BeforeClass
    public static void setup() throws Exception {
        // Check that the docker image exists locally
        Process checkImage = new ProcessBuilder("docker", "image", "inspect", FULL_IMAGE)
                .redirectErrorStream(true)
                .start();
        int exitCode = checkImage.waitFor();
        assumeTrue("Docker image " + FULL_IMAGE + " must be built first "
                + "(run: mvn package jib:dockerBuild@build -pl herddb-docker)", exitCode == 0);

        // Save docker image to a tarball
        Path imageTar = Files.createTempFile("herddb-image", ".tar");
        try {
            LOG.info("Saving docker image to tarball...");
            Process save = new ProcessBuilder("docker", "save", FULL_IMAGE, "-o", imageTar.toString())
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
    public void testClusterModeWithVectorIndexing() throws Exception {
        LOG.info("=== Test: Cluster Mode with Vector Indexing Services ===");

        Map<String, String> values = new LinkedHashMap<>();
        // Server: cluster mode with local storage
        values.put("server.mode", "cluster");
        values.put("server.storageMode", "local");
        values.put("server.replicaCount", "1");
        values.put("tools.enabled", "false");
        values.put("image.pullPolicy", "Never");
        // ZooKeeper
        values.put("zookeeper.enabled", "true");
        values.put("zookeeper.javaOpts", JAVA_OPTS);
        values.put("zookeeper.resources.requests.memory", "256Mi");
        values.put("zookeeper.resources.requests.cpu", "0.5");
        values.put("zookeeper.resources.limits.memory", "256Mi");
        values.put("zookeeper.resources.limits.cpu", "0.5");
        values.put("zookeeper.storage.size", "1Gi");
        // BookKeeper
        values.put("bookkeeper.enabled", "true");
        values.put("bookkeeper.replicaCount", "1");
        values.put("bookkeeper.javaOpts", JAVA_OPTS);
        values.put("bookkeeper.resources.requests.memory", "256Mi");
        values.put("bookkeeper.resources.requests.cpu", "0.5");
        values.put("bookkeeper.resources.limits.memory", "256Mi");
        values.put("bookkeeper.resources.limits.cpu", "0.5");
        values.put("bookkeeper.storage.journal.size", "1Gi");
        values.put("bookkeeper.storage.ledger.size", "1Gi");
        // Indexing Service: 2 replicas
        values.put("indexingService.enabled", "true");
        values.put("indexingService.replicaCount", "2");
        values.put("indexingService.javaOpts", JAVA_OPTS);
        values.put("indexingService.resources.requests.memory", "256Mi");
        values.put("indexingService.resources.requests.cpu", "0.5");
        values.put("indexingService.resources.limits.memory", "256Mi");
        values.put("indexingService.resources.limits.cpu", "0.5");
        values.put("indexingService.storage.data.size", "1Gi");
        values.put("indexingService.storage.log.size", "1Gi");
        // Server resources — needs more memory for checkpoint operations
        String serverJavaOpts = "-XX:+UseG1GC -Duser.language=en -Xmx256m -Xms256m"
                + " -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=128m"
                + " -Djava.awt.headless=true --add-modules jdk.incubator.vector";
        values.put("server.javaOpts", serverJavaOpts);
        values.put("server.resources.requests.memory", "512Mi");
        values.put("server.resources.requests.cpu", "0.5");
        values.put("server.resources.limits.memory", "512Mi");
        values.put("server.resources.limits.cpu", "0.5");
        values.put("server.storage.data.size", "1Gi");
        values.put("server.storage.commitlog.size", "1Gi");

        // Render and apply Helm chart
        String renderedYaml = helmTemplate(helmChartPath, values);
        LOG.info("Rendered YAML length: " + renderedYaml.length());

        List<HasMetadata> resources = kubernetesClient.load(
                new ByteArrayInputStream(renderedYaml.getBytes(StandardCharsets.UTF_8))).items();
        LOG.info("Applying " + resources.size() + " Kubernetes resources...");
        kubernetesClient.resourceList(resources).createOrReplace();

        // Wait for ZooKeeper
        LOG.info("Waiting for ZooKeeper pod to be ready...");
        waitForComponent("zookeeper", 5, TimeUnit.MINUTES);
        LOG.info("ZooKeeper pod is ready.");

        // Wait for BookKeeper
        LOG.info("Waiting for BookKeeper pod to be ready...");
        waitForComponent("bookkeeper", 5, TimeUnit.MINUTES);
        LOG.info("BookKeeper pod is ready.");

        // Wait for Indexing Services (2 replicas)
        LOG.info("Waiting for Indexing Service pods to be ready...");
        waitForComponent("indexing-service", 5, TimeUnit.MINUTES);
        LOG.info("Indexing Service pods are ready.");

        // Verify 2 indexing service pods
        List<Pod> isPods = kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", "indexing-service")
                .list().getItems();
        assertEquals("Expected 2 indexing service pods", 2, isPods.size());

        // Create NodePort service for JDBC access
        kubernetesClient.services().inNamespace("default").createOrReplace(
                new ServiceBuilder()
                        .withNewMetadata()
                            .withName("herddb-vec-nodeport")
                            .withNamespace("default")
                        .endMetadata()
                        .withNewSpec()
                            .withType("NodePort")
                            .withSelector(Collections.singletonMap("app.kubernetes.io/component", "server"))
                            .withPorts(new ServicePortBuilder()
                                    .withPort(7000)
                                    .withTargetPort(new IntOrString(7000))
                                    .withNodePort(NODE_PORT)
                                    .build())
                        .endSpec()
                        .build());
        LOG.info("NodePort service created on port " + NODE_PORT);

        // Wait for HerdDB server
        LOG.info("Waiting for HerdDB server pod to be ready...");
        waitForComponent("server", 5, TimeUnit.MINUTES);
        LOG.info("HerdDB server pod is ready.");

        // Connect via JDBC and run vector operations
        String host = k3s.getHost();
        int mappedPort = k3s.getMappedPort(NODE_PORT);
        LOG.info("Connecting to HerdDB via NodePort at " + host + ":" + mappedPort);

        String jdbcUrl = "jdbc:herddb:server:" + host + ":" + mappedPort;
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             Statement statement = connection.createStatement()) {

            // CREATE TABLE with vector column
            statement.execute("CREATE TABLE vec_test (id int primary key, vec floata not null)");
            LOG.info("Table with vector column created.");

            // CREATE VECTOR INDEX
            statement.execute("CREATE VECTOR INDEX vidx ON vec_test(vec)");
            LOG.info("Vector index created.");

            // INSERT 4 orthogonal vectors using PreparedStatement
            float[] vecX = {1.0f, 0.0f, 0.0f, 0.0f};
            float[] vecY = {0.0f, 1.0f, 0.0f, 0.0f};
            float[] vecZ = {0.0f, 0.0f, 1.0f, 0.0f};
            float[] vecW = {0.0f, 0.0f, 0.0f, 1.0f};

            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO vec_test(id, vec) VALUES(?, ?)")) {
                ps.setInt(1, 1);
                ps.setObject(2, vecX);
                assertEquals(1, ps.executeUpdate());

                ps.setInt(1, 2);
                ps.setObject(2, vecY);
                assertEquals(1, ps.executeUpdate());

                ps.setInt(1, 3);
                ps.setObject(2, vecZ);
                assertEquals(1, ps.executeUpdate());

                ps.setInt(1, 4);
                ps.setObject(2, vecW);
                assertEquals(1, ps.executeUpdate());
            }
            LOG.info("4 orthogonal vectors inserted.");

            // Force checkpoint so the indexing services catch up via WAL tailing
            statement.execute("EXECUTE CHECKPOINT 'herd'");
            LOG.info("Checkpoint executed.");

            // Wait for indexing services to process the WAL
            Thread.sleep(10000);
            LOG.info("Waited for indexing service catch-up.");

            // ANN search for vector closest to X axis
            float[] queryVec = {1.0f, 0.0f, 0.0f, 0.0f};
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT id FROM vec_test ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT 2")) {
                ps.setObject(1, queryVec);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue("ANN search must return at least one result", rs.next());
                    int firstId = rs.getInt("id");
                    assertEquals("Closest vector to [1,0,0,0] should be id=1", 1, firstId);
                    LOG.info("ANN search result: first id=" + firstId + " (correct).");
                }
            }
        }
        LOG.info("Test passed: Cluster mode with vector indexing services works.");
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
                // Try to get logs
                for (Pod pod : pods) {
                    try {
                        String logs = kubernetesClient.pods()
                                .inNamespace("default")
                                .withName(pod.getMetadata().getName())
                                .getLog();
                        if (logs != null && !logs.isEmpty()) {
                            String[] lines = logs.split("\n");
                            int start = Math.max(0, lines.length - 20);
                            LOG.info("Last " + Math.min(20, lines.length) + " log lines for "
                                    + pod.getMetadata().getName() + ":");
                            for (int i = start; i < lines.length; i++) {
                                LOG.info("  " + lines[i]);
                            }
                        }
                    } catch (Exception e) {
                        LOG.info("Could not get logs for " + pod.getMetadata().getName()
                                + ": " + e.getMessage());
                    }
                }
                // Check if all pods are ready
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
        command.add("test-vec");
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
