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
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class HerdDBKubernetesIT {

    private static final Logger LOG = Logger.getLogger(HerdDBKubernetesIT.class.getName());

    private static final String IMAGE_NAME = "herddb/herddb-server";
    private static final String IMAGE_TAG = "0.30.0-SNAPSHOT";
    private static final String FULL_IMAGE = IMAGE_NAME + ":" + IMAGE_TAG;

    private static final String SERVER_JAVA_OPTS = "-XX:+UseG1GC -Duser.language=en -Xmx256m -Xms256m"
            + " -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=128m"
            + " -Djava.awt.headless=true --add-modules jdk.incubator.vector";

    @ClassRule
    public static K3sContainer k3s = new K3sContainer(DockerImageName.parse("rancher/k3s:v1.31.4-k3s1"))
            .withExposedPorts(6443);

    private static KubernetesClient kubernetesClient;

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

        // Wait for the server pod to be ready (readiness probe checks TCP port 7000)
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
        waitForTablespace(toolsPod);

        // CREATE TABLE
        execSql(toolsPod, "CREATE TABLE test_table (id int primary key, name string)");
        LOG.info("Table created.");

        // INSERT
        execSql(toolsPod, "INSERT INTO test_table (id, name) VALUES (1, 'hello')");
        LOG.info("Row inserted.");

        // SELECT and verify
        String output = execSql(toolsPod, "SELECT id, name FROM test_table");
        LOG.info("SELECT output: " + output);
        assertTrue("Expected 'hello' in output", output.contains("hello"));
        assertTrue("Expected '1' in output", output.contains("1"));
        LOG.info("Row verified.");
    }

    /**
     * Wait for the HerdDB tablespace to be fully booted by polling via CLI.
     */
    static void waitForTablespace(String toolsPod) throws Exception {
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
        while (System.currentTimeMillis() < deadline) {
            try {
                execSql(toolsPod, "SELECT * FROM systables LIMIT 1");
                LOG.info("Tablespace is ready.");
                return;
            } catch (Exception e) {
                LOG.info("Tablespace not ready yet: " + e.getMessage());
            }
            Thread.sleep(3000);
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
    static String execSql(String toolsPodName, String sql) throws Exception {
        return execInPod(kubernetesClient, toolsPodName, "herddb-cli", "--query", sql);
    }

    /**
     * Execute a command in a pod and return stdout.
     * Throws RuntimeException if the command fails.
     */
    static String execInPod(KubernetesClient client, String podName, String... command) throws Exception {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        CompletableFuture<Integer> exitCodeFuture = new CompletableFuture<>();

        try (ExecWatch exec = client.pods()
                .inNamespace("default")
                .withName(podName)
                .writingOutput(stdout)
                .writingError(stderr)
                .usingListener(new ExecListener() {
                    @Override
                    public void onOpen() {
                    }
                    @Override
                    public void onFailure(Throwable t, Response failureResponse) {
                        exitCodeFuture.completeExceptionally(t);
                    }
                    @Override
                    public void onClose(int code, String reason) {
                        exitCodeFuture.complete(code);
                    }
                })
                .exec(command)) {
            int exitCode = exitCodeFuture.get(5, TimeUnit.MINUTES);
            String out = stdout.toString(StandardCharsets.UTF_8);
            String err = stderr.toString(StandardCharsets.UTF_8);
            if (exitCode != 0 && exitCode != 1000) {
                throw new RuntimeException("Command failed (exit=" + exitCode + "): " + err + "\nstdout: " + out);
            }
            // Also check stderr for Java exceptions
            if (err.contains("Exception") || err.contains("ERROR")) {
                // herddb-cli may print errors to stderr but still exit 0
                // Check if stdout has valid output
                if (out.trim().isEmpty()) {
                    throw new RuntimeException("Command produced error output: " + err);
                }
            }
            return out;
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

    private static String helmTemplate(String chartPath) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
                "helm", "template", "test-release", chartPath,
                "--set", "server.mode=standalone",
                "--set", "server.replicaCount=1",
                "--set", "tools.enabled=true",
                "--set", "server.javaOpts=" + SERVER_JAVA_OPTS,
                "--set", "server.resources.requests.memory=512Mi",
                "--set", "server.resources.requests.cpu=0.5",
                "--set", "server.resources.limits.memory=512Mi",
                "--set", "server.resources.limits.cpu=0.5",
                "--set", "server.storage.data.size=1Gi",
                "--set", "server.storage.commitlog.size=1Gi",
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
