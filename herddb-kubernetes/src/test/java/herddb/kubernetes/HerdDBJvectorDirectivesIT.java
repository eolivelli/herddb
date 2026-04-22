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
import org.junit.Test;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Minimal Kubernetes integration test that verifies the jvector JVM
 * compiler-directives file is bundled in the image and applied to the JVM
 * for both the {@code herddb-server} pod and the {@code indexing-service}
 * pod. Deliberately uses a stripped-down chart layout (standalone server +
 * memory-storage indexing-service, no ZooKeeper, no BookKeeper, no Tools)
 * to keep the test fast and resource-light: the goal is to assert flag
 * propagation, not to exercise the data path.
 */
public class HerdDBJvectorDirectivesIT {

    private static final Logger LOG = Logger.getLogger(HerdDBJvectorDirectivesIT.class.getName());

    private static final String IMAGE_NAME = "herddb/herddb-server";
    private static final String IMAGE_TAG = "0.30.0-SNAPSHOT";
    private static final String FULL_IMAGE = IMAGE_NAME + ":" + IMAGE_TAG;

    @ClassRule
    public static K3sContainer k3s = new K3sContainer(DockerImageName.parse("rancher/k3s:v1.31.4-k3s1"))
            .withExposedPorts(6443);

    private static KubernetesClient kubernetesClient;
    private static String helmChartPath;

    @BeforeClass
    public static void setup() throws Exception {
        Process checkImage = dockerProcess("docker", "image", "inspect", FULL_IMAGE)
                .redirectErrorStream(true)
                .start();
        int exitCode = checkImage.waitFor();
        assumeTrue("Docker image " + FULL_IMAGE + " must be built first "
                + "(run: mvn package -Dkubernetes -pl herddb-docker)", exitCode == 0);

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
    public void jvectorDirectivesAppliedToServerAndIndexingService() throws Exception {
        // Minimal chart values: standalone server with local storage + a single
        // indexing-service replica using in-memory storage. No ZooKeeper, no
        // BookKeeper, no Tools pod. The assertion only needs the JVMs to be
        // alive long enough to inspect /proc/<pid>/cmdline and the bundled
        // compile-command file.
        Map<String, String> values = new LinkedHashMap<>();
        values.put("server.mode", "standalone");
        values.put("server.storageMode", "local");
        values.put("server.replicaCount", "1");
        values.put("server.resources.requests.memory", "256Mi");
        values.put("server.resources.requests.cpu", "0.5");
        values.put("server.resources.limits.memory", "256Mi");
        values.put("server.resources.limits.cpu", "0.5");
        values.put("server.storage.data.size", "1Gi");
        values.put("server.storage.commitlog.size", "1Gi");
        // Tiny heap is fine — we don't run real workloads against it.
        values.put("server.javaOpts",
                "-XX:+UseG1GC -Duser.language=en -Xmx128m -Xms128m"
                        + " -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=64m"
                        + " -Djava.awt.headless=true --add-modules jdk.incubator.vector");
        values.put("tools.enabled", "false");
        values.put("zookeeper.enabled", "false");
        values.put("bookkeeper.enabled", "false");

        values.put("indexingService.enabled", "true");
        values.put("indexingService.replicaCount", "1");
        values.put("indexingService.storageMode", "memory");
        values.put("indexingService.javaOpts",
                "-XX:+UseG1GC -Duser.language=en -Xmx128m -Xms128m"
                        + " -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=64m"
                        + " -Djava.awt.headless=true --add-modules jdk.incubator.vector");
        values.put("indexingService.resources.requests.memory", "256Mi");
        values.put("indexingService.resources.requests.cpu", "0.5");
        values.put("indexingService.resources.limits.memory", "256Mi");
        values.put("indexingService.resources.limits.cpu", "0.5");
        values.put("indexingService.storage.data.size", "1Gi");
        values.put("indexingService.storage.log.size", "1Gi");
        values.put("image.pullPolicy", "Never");

        String renderedYaml = helmTemplate(helmChartPath, values);
        List<HasMetadata> resources = kubernetesClient.load(
                new ByteArrayInputStream(renderedYaml.getBytes(StandardCharsets.UTF_8))).items();
        kubernetesClient.resourceList(resources).createOrReplace();

        LOG.info("Waiting for HerdDB server pod to be ready...");
        waitForRunning("server", 5);
        LOG.info("Waiting for IndexingService pod to be ready...");
        waitForRunning("indexing-service", 5);

        Pod serverPod = singlePod("server");
        Pod isPod = singlePod("indexing-service");
        HerdDBKubernetesIT.assertJvectorCompilerDirectivesActive(k3s, serverPod.getMetadata().getName());
        HerdDBKubernetesIT.assertJvectorCompilerDirectivesActive(k3s, isPod.getMetadata().getName());
    }

    private Pod singlePod(String component) {
        List<Pod> pods = kubernetesClient.pods()
                .inNamespace("default")
                .withLabel("app.kubernetes.io/component", component)
                .list().getItems();
        assertEquals("Expected 1 " + component + " pod, found " + pods.size(), 1, pods.size());
        return pods.get(0);
    }

    /**
     * Wait for at least one pod with the given component label to reach the
     * Running phase (sufficient for assertJvectorCompilerDirectivesActive
     * which only needs a live PID). We don't require Ready since the JVM
     * has already started and parsed -XX:CompileCommandFile by the time
     * the kubelet flips the container to Running.
     */
    private void waitForRunning(String component, long timeoutMinutes) throws Exception {
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(timeoutMinutes);
        while (System.currentTimeMillis() < deadline) {
            List<Pod> pods = kubernetesClient.pods()
                    .inNamespace("default")
                    .withLabel("app.kubernetes.io/component", component)
                    .list().getItems();
            boolean anyRunning = pods.stream()
                    .anyMatch(p -> p.getStatus() != null && "Running".equals(p.getStatus().getPhase()));
            if (anyRunning) {
                return;
            }
            Thread.sleep(5000);
        }
        throw new RuntimeException("Timed out waiting for " + component + " pod to be Running");
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
        command.add("test-jvec");
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
