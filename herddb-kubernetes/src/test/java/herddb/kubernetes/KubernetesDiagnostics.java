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

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.testcontainers.containers.Container;
import org.testcontainers.k3s.K3sContainer;

/**
 * Best-effort diagnostic dump for a K3s integration test failure (issue #438).
 *
 * <p>The K3s integration tests in this module have been failing on CI with
 * {@code TimeoutException: Request timedout (600s)} from the JDBC client during
 * {@code CREATE TABLE} statements — the server pod is in {@code Phase=Running}
 * but its JVM has become unresponsive. Without on-failure diagnostics we have
 * no way to tell whether the JVM is stuck in GC, deadlocked, OOM-ing on direct
 * memory, OOMKilled by the kubelet, or something else.
 *
 * <p>This helper is invoked from a JUnit {@code @Rule TestWatcher} in each
 * {@code *IT} class and dumps:
 * <ul>
 *   <li>{@code kubectl get pods -A -o wide}, {@code kubectl get events -A}</li>
 *   <li>For each pod in the default namespace:
 *       <ul>
 *         <li>{@code kubectl describe pod} (status, conditions, restartCount, OOMKilled reasons)</li>
 *         <li>Last 1000 lines of pod logs (current container)</li>
 *         <li>Previous-container logs if the container has restarted (where OOMKill messages live)</li>
 *         <li>{@code jstack -l &lt;pid&gt;} (full thread dump with locks)</li>
 *         <li>{@code jcmd &lt;pid&gt; VM.native_memory summary} (requires
 *             {@code -XX:NativeMemoryTracking=summary} on the JVM)</li>
 *         <li>{@code jcmd &lt;pid&gt; GC.heap_info}, {@code VM.flags}</li>
 *         <li>{@code /proc/&lt;pid&gt;/status} VM and Threads counters</li>
 *         <li>{@code /proc/&lt;pid&gt;/smaps_rollup} (RSS / Pss / Anonymous breakdown)</li>
 *       </ul>
 *   </li>
 *   <li>{@code dmesg | tail -200} from the K3s host (looking for
 *       {@code Memory cgroup out of memory} OOMKill events)</li>
 * </ul>
 *
 * <p>Every individual command is wrapped in try/catch so a partial diagnostic
 * is still better than nothing. The output is written via {@link Logger} so
 * Surefire captures it in {@code target/surefire-reports/*-output.txt} which
 * the CI workflow uploads as an artifact on failure.
 */
public final class KubernetesDiagnostics {

    private static final Logger LOG = Logger.getLogger(KubernetesDiagnostics.class.getName());

    /** Maximum log lines to fetch per pod (current container). */
    private static final int POD_LOG_TAIL_LINES = 1000;
    /** Maximum log lines to fetch per pod (previous container after restart). */
    private static final int PREV_POD_LOG_TAIL_LINES = 500;
    /** Per-kubectl-exec timeout: jstack on a stuck JVM may itself be slow. */
    private static final int EXEC_TIMEOUT_LINES = 1500;

    private KubernetesDiagnostics() {
    }

    /**
     * Dump comprehensive diagnostics for the K3s cluster. Safe to call from a
     * {@code @Rule TestWatcher.failed()} hook — every sub-step is best-effort.
     *
     * @param k3s    the testcontainers K3s container (used for kubectl exec / dmesg)
     * @param client the fabric8 client (used for {@code .pods().getLog()})
     * @param tag    a short identifier (typically the failing test method name)
     */
    public static void dumpAll(K3sContainer k3s, KubernetesClient client, String tag) {
        section("KUBERNETES DIAGNOSTICS START [" + tag + "]");
        // Cluster overview ----------------------------------------------------
        runKubectl(k3s, "get", "pods", "-A", "-o", "wide");
        runKubectl(k3s, "get", "events", "-A", "--sort-by=.metadata.creationTimestamp");
        runKubectl(k3s, "top", "pods", "-A");
        runKubectl(k3s, "top", "nodes");

        // Per-pod -------------------------------------------------------------
        List<Pod> pods;
        try {
            pods = client.pods().inNamespace("default").list().getItems();
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "Listing pods in default namespace failed", e);
            pods = List.of();
        }
        for (Pod pod : pods) {
            String podName = pod.getMetadata().getName();
            section("POD " + podName);
            runKubectl(k3s, "describe", "pod", podName);
            dumpPodLogs(client, pod);
            dumpJvmState(k3s, podName);
            dumpProcessState(k3s, podName);
        }

        // K3s host ------------------------------------------------------------
        section("DMESG (K3s host)");
        runK3sShell(k3s, "dmesg 2>/dev/null | tail -200 || echo 'dmesg unavailable'");

        section("KUBERNETES DIAGNOSTICS END [" + tag + "]");
    }

    // -------------------------------------------------------------------------
    // Pod logs
    // -------------------------------------------------------------------------

    private static void dumpPodLogs(KubernetesClient client, Pod pod) {
        String podName = pod.getMetadata().getName();
        try {
            String logs = client.pods()
                    .inNamespace("default")
                    .withName(podName)
                    .tailingLines(POD_LOG_TAIL_LINES)
                    .getLog();
            section("LOGS " + podName + " (current container, last " + POD_LOG_TAIL_LINES + " lines)");
            logMultiline(logs);
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "current-container log fetch for " + podName + " failed", e);
        }

        if (pod.getStatus() == null || pod.getStatus().getContainerStatuses() == null) {
            return;
        }
        for (ContainerStatus cs : pod.getStatus().getContainerStatuses()) {
            int restartCount = cs.getRestartCount() == null ? 0 : cs.getRestartCount();
            if (restartCount <= 0) {
                continue;
            }
            try {
                String prev = client.pods()
                        .inNamespace("default")
                        .withName(podName)
                        .inContainer(cs.getName())
                        .terminated()
                        .tailingLines(PREV_POD_LOG_TAIL_LINES)
                        .getLog();
                section("PREVIOUS LOGS " + podName + "/" + cs.getName()
                        + " (restartCount=" + restartCount + ", last "
                        + PREV_POD_LOG_TAIL_LINES + " lines)");
                logMultiline(prev);
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "previous-container log fetch for "
                        + podName + "/" + cs.getName() + " failed", e);
            }
        }
    }

    // -------------------------------------------------------------------------
    // JVM state inside the pod
    // -------------------------------------------------------------------------

    /**
     * jstack and jcmd inside the pod's JVM. The HerdDB docker image is built
     * from {@code eclipse-temurin:25-jdk} (see herddb-docker/.../Dockerfile)
     * so {@code jstack} and {@code jcmd} are on PATH.
     *
     * <p>{@code jcmd VM.native_memory summary} requires the JVM to be started
     * with {@code -XX:NativeMemoryTracking=summary}; a missing flag is
     * gracefully handled (jcmd prints {@code Native memory tracking is not enabled}).
     */
    private static void dumpJvmState(K3sContainer k3s, String podName) {
        // Find the java PID. Prefer pgrep on a HerdDB main class; fall back to any java.
        String shellCmd =
                "set +e; "
                + "pid=$(pgrep -f 'herddb\\.|herddb-' 2>/dev/null | head -n1); "
                + "if [ -z \"$pid\" ]; then pid=$(pgrep -x java 2>/dev/null | head -n1); fi; "
                + "if [ -z \"$pid\" ]; then echo 'NO_JAVA_PID'; ps -ef; exit 0; fi; "
                + "echo '----- pid -----'; echo \"$pid\"; "
                + "echo '----- jcmd VM.flags -----'; "
                + "jcmd \"$pid\" VM.flags 2>&1 || echo 'jcmd VM.flags failed'; "
                + "echo '----- jcmd GC.heap_info -----'; "
                + "jcmd \"$pid\" GC.heap_info 2>&1 || echo 'jcmd GC.heap_info failed'; "
                + "echo '----- jcmd VM.native_memory summary -----'; "
                + "jcmd \"$pid\" VM.native_memory summary 2>&1 || echo 'jcmd VM.native_memory failed'; "
                + "echo '----- jstack -l (head " + EXEC_TIMEOUT_LINES + " lines) -----'; "
                + "jstack -l \"$pid\" 2>&1 | head -n " + EXEC_TIMEOUT_LINES + " || echo 'jstack failed'";
        section("JVM STATE " + podName);
        runKubectlExec(k3s, podName, shellCmd);
    }

    // -------------------------------------------------------------------------
    // Process / cgroup state inside the pod
    // -------------------------------------------------------------------------

    private static void dumpProcessState(K3sContainer k3s, String podName) {
        String shellCmd =
                "set +e; "
                + "pid=$(pgrep -f 'herddb\\.|herddb-' 2>/dev/null | head -n1); "
                + "if [ -z \"$pid\" ]; then pid=$(pgrep -x java 2>/dev/null | head -n1); fi; "
                + "if [ -z \"$pid\" ]; then echo 'NO_JAVA_PID'; exit 0; fi; "
                + "echo '----- /proc/'\"$pid\"'/status (VM/RSS/Threads/State) -----'; "
                + "grep -E '^(Vm|Rss|Threads|State|Name)' /proc/\"$pid\"/status 2>/dev/null "
                + "  || echo 'status read failed'; "
                + "echo '----- /proc/'\"$pid\"'/smaps_rollup -----'; "
                + "cat /proc/\"$pid\"/smaps_rollup 2>/dev/null || echo 'smaps_rollup unavailable'; "
                + "echo '----- /proc/'\"$pid\"'/limits (memory + nofile rows) -----'; "
                + "grep -E 'memory|files' /proc/\"$pid\"/limits 2>/dev/null || echo 'limits unavailable'; "
                + "echo '----- top RSS consumers in container -----'; "
                + "ps -eo pid,rss,vsz,pcpu,pmem,comm --sort=-rss 2>/dev/null | head -10";
        section("PROCESS STATE " + podName);
        runKubectlExec(k3s, podName, shellCmd);
    }

    // -------------------------------------------------------------------------
    // Low-level helpers
    // -------------------------------------------------------------------------

    private static void runKubectl(K3sContainer k3s, String... args) {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "kubectl";
        System.arraycopy(args, 0, cmd, 1, args.length);
        runAndLog(k3s, cmd);
    }

    private static void runKubectlExec(K3sContainer k3s, String podName, String shellCmd) {
        runAndLog(k3s, new String[]{"kubectl", "exec", podName, "--", "sh", "-c", shellCmd});
    }

    private static void runK3sShell(K3sContainer k3s, String shellCmd) {
        runAndLog(k3s, new String[]{"sh", "-c", shellCmd});
    }

    /**
     * Run a command inside the K3s container and log its stdout/stderr. Catches
     * {@code Exception} (broad on purpose: this helper is on the failure path —
     * any propagation would mask the original test failure with a noisy
     * diagnostic dump exception).
     */
    @SuppressWarnings({"checkstyle:IllegalCatch", "PMD.AvoidCatchingGenericException"})
    private static void runAndLog(K3sContainer k3s, String[] cmd) {
        LOG.info("$ " + String.join(" ", cmd));
        Container.ExecResult r;
        try {
            r = k3s.execInContainer(cmd);
        } catch (Exception e) {
            // Broad catch: testcontainers throws a mix of IOException and
            // InterruptedException, and we never want a diagnostic command to
            // mask the test failure. See class javadoc.
            LOG.log(Level.WARNING, "exec failed: " + String.join(" ", cmd), e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return;
        }
        logMultiline(r.getStdout());
        if (r.getStderr() != null && !r.getStderr().isEmpty()) {
            LOG.info("(stderr)");
            logMultiline(r.getStderr());
        }
        if (r.getExitCode() != 0) {
            LOG.info("(exit=" + r.getExitCode() + ")");
        }
    }

    private static void logMultiline(String s) {
        if (s == null || s.isEmpty()) {
            return;
        }
        for (String line : s.split("\n")) {
            LOG.info("  " + line);
        }
    }

    private static void section(String title) {
        LOG.info("====== " + title + " ======");
    }
}
