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
package herddb.indexing.optimizer;

import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.TableSpace;
import herddb.server.RemoteFileClient;
import herddb.server.RemoteFileServiceFactory;
import herddb.server.ServerConfiguration;
import herddb.storage.DataStorageManager;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Bootstrap entry point for the index-optimizer service. Loaded by the
 * {@code herddb-services} {@code bin/service index-optimizer …} launcher.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Parse a properties file ({@link OptimizerConfiguration}).</li>
 *   <li>Open a ZooKeeper connection and a {@link SegmentRegistryClient}.</li>
 *   <li>Build a {@link DataStorageManager} backed by the remote file service
 *       (so the merger can download input segment files and upload the merged
 *       output) when {@code indexoptimizer.remote.file.servers} is configured
 *       — falls back to a {@link NoopMerger} otherwise so the unit test suite
 *       can exercise the registry plumbing without provisioning a file server.</li>
 *   <li>Resolve a {@link SegmentMerger} via the standard {@link ServiceLoader}
 *       SPI when one is registered (test override path); otherwise default to
 *       the production {@link RemoteSegmentMerger}.</li>
 *   <li>Drive {@link IndexOptimizerEngine#runOnce()} via two paths
 *       (issue #484): a persistent-recursive ZK watch over the registry's
 *       tablespace path that fires {@link IndexOptimizerEngine#runOnce()}
 *       (debounced) on every children-changed event, plus a periodic
 *       {@code scheduleAtFixedRate} fallback that ticks every {@code intervalMs}
 *       as a safety net for the (rare) cases when ZK delivers no event.</li>
 * </ul>
 *
 * <p>Singleton enforcement is delegated to the deployment layer (Helm
 * {@code replicas: 1}); two optimizers running concurrently will fight over
 * registry CAS but never corrupt the registry — they will simply waste cycles.
 */
public final class IndexOptimizerMain {

    private static final Logger LOGGER = Logger.getLogger(IndexOptimizerMain.class.getName());

    private final OptimizerConfiguration configuration;
    /**
     * Optional pre-built merger (e.g. injected by tests). When {@code null},
     * {@link #start} resolves a merger via the SPI / RemoteSegmentMerger
     * plumbing. The previous constructor accepting an explicit merger is
     * retained for backward compatibility with existing tests.
     */
    private final SegmentMerger preconfiguredMerger;

    private ZooKeeper zooKeeper;
    /**
     * Scheduler used by both the periodic safety-net tick and the event-driven
     * wakeup. {@code volatile} so the ZK watcher thread (which reads it without
     * holding the {@code IndexOptimizerMain} monitor) sees a consistent view
     * once {@link #start} has published it (issue #484). The mixed
     * synchronized-write / unsynchronized-read pattern is filtered project-wide
     * via {@code excludeFindBugsFilter.xml}.
     */
    private volatile ScheduledExecutorService scheduler;
    private SegmentRegistryClient registry;
    private OptimizerLeaderLock leaderLock;
    private IndexOptimizerEngine engine;
    private OptimizerHttpServer httpServer;
    private SegmentMerger merger;
    private DataStorageManager mergerDataStorageManager;
    private RemoteFileClient mergerFileClient;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    /**
     * Coalescing flag for event-driven scheduling. The persistent-recursive
     * ZK watch fires {@link #scheduleEventDrivenTick} on every change at or
     * below the registry tablespace path; {@code pendingWakeup} is flipped
     * to {@code true} via CAS so a burst of N events still produces only one
     * scheduled tick. Reset to {@code false} as the tick body starts.
     */
    private final AtomicBoolean pendingWakeup = new AtomicBoolean(false);
    /** Number of event-driven ticks executed (observable by tests). */
    private final AtomicLong eventDrivenTicks = new AtomicLong();
    /** Number of distinct ZK events observed by the persistent-recursive watcher. */
    private final AtomicLong watcherEvents = new AtomicLong();
    /**
     * Set to {@code true} when the ZooKeeper session expires. Once set, the
     * persistent-recursive watch is dead, the leader-lock ephemeral znode is
     * gone, and any registry read against {@link #zooKeeper} will fail —
     * which means the pod is silently broken. The HTTP {@code /health}
     * handler consults this flag and returns 503 so Helm's liveness probe
     * restarts the pod (review item B.3 from the first pr-reviewer pass).
     */
    private final AtomicBoolean sessionExpired = new AtomicBoolean(false);
    /**
     * Debounce window for the event-driven tick. {@code volatile} for the same
     * reason as {@link #scheduler}: the ZK watcher thread reads it without
     * holding the {@code IndexOptimizerMain} monitor (issue #484).
     */
    private volatile long eventDebounceMs;
    private volatile String tablespaceUuid;

    /**
     * Production constructor: the merger is built at {@link #start} time from
     * configuration. Pass {@code null} to defer to the SPI / RemoteSegmentMerger
     * fallback chain.
     */
    public IndexOptimizerMain(OptimizerConfiguration configuration) {
        this(configuration, null);
    }

    /**
     * Test / SPI-override constructor. Production code uses
     * {@link #IndexOptimizerMain(OptimizerConfiguration)}; this overload exists
     * so tests can plug a synthetic merger ({@code InMemorySegmentMerger}, etc.)
     * without going through the {@code ServiceLoader} indirection.
     */
    public IndexOptimizerMain(OptimizerConfiguration configuration, SegmentMerger merger) {
        this.configuration = configuration;
        this.preconfiguredMerger = merger;
    }

    public IndexOptimizerEngine getEngine() {
        return engine;
    }

    public SegmentMerger getMerger() {
        return merger;
    }

    public long getEventDrivenTicks() {
        return eventDrivenTicks.get();
    }

    public long getWatcherEvents() {
        return watcherEvents.get();
    }

    /**
     * Returns {@code true} when the ZooKeeper session has expired. The HTTP
     * health handler uses this to flip {@code /health} to 503 so Helm's
     * liveness probe restarts the pod.
     */
    public boolean isSessionExpired() {
        return sessionExpired.get();
    }

    public synchronized void start() throws Exception {
        if (engine != null) {
            return;
        }
        String zkAddress = configuration.getString(
                OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS,
                OptimizerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT);
        int sessionTimeout = configuration.getInt(
                OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT,
                OptimizerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
        String basePath = configuration.getString(
                OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH,
                OptimizerConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT);
        String tablespaceName = configuration.getString(
                OptimizerConfiguration.PROPERTY_TABLESPACE_NAME, null);
        if (tablespaceName == null || tablespaceName.isEmpty()) {
            throw new IllegalStateException(
                    OptimizerConfiguration.PROPERTY_TABLESPACE_NAME + " must be set");
        }

        // Resolve the tablespace UUID from the human-readable name by consulting
        // the HerdDB cluster metadata in ZooKeeper.  We open a short-lived
        // ZookeeperMetadataStorageManager exclusively for this lookup and close
        // it immediately after so that the optimizer's permanent ZK connection
        // (used by SegmentRegistryClient and OptimizerLeaderLock) is separate
        // and independently reconnectable.
        List<String> discoveredFileServers = new ArrayList<>();
        try (ZookeeperMetadataStorageManager zkmeta =
                new ZookeeperMetadataStorageManager(zkAddress, sessionTimeout, basePath)) {
            zkmeta.start(false); // read-only: do not create/format cluster metadata paths
            TableSpace ts = zkmeta.describeTableSpace(tablespaceName);
            if (ts == null) {
                throw new IllegalStateException(
                        "No tablespace named '" + tablespaceName + "' found under "
                        + basePath + "/tableSpaces — ensure the HerdDB cluster has started "
                        + "and created its default tablespace before starting the optimizer.");
            }
            tablespaceUuid = ts.uuid;
            try {
                discoveredFileServers.addAll(zkmeta.listFileServers());
            } catch (MetadataStorageManagerException listErr) {
                LOGGER.log(Level.WARNING,
                        "could not discover remote file servers via ZK; the optimizer"
                                + " will fall back to NoopMerger if the static server list"
                                + " is also empty: {0}",
                        listErr.getMessage());
            }
        } catch (MetadataStorageManagerException e) {
            throw new IllegalStateException(
                    "Failed to resolve tablespace '" + tablespaceName + "' from ZooKeeper: "
                    + e.getMessage(), e);
        }
        LOGGER.log(Level.INFO, "Resolved tablespace name ''{0}'' to UUID {1}",
                new Object[]{tablespaceName, tablespaceUuid});

        CountDownLatch zkConnected = new CountDownLatch(1);
        AtomicReference<ZooKeeper> zkRef = new AtomicReference<>();
        ZooKeeper zk = new ZooKeeper(zkAddress, sessionTimeout, (WatchedEvent event) -> {
            switch (event.getState()) {
                case SyncConnected:
                    zkConnected.countDown();
                    break;
                case Expired:
                    // Session expiry kills the persistent-recursive watch and the
                    // leader-lock ephemeral znode. The pod is silently broken from
                    // here on; flag /health to 503 so Helm restarts us (review item
                    // B.3 from the first pr-reviewer pass).
                    if (sessionExpired.compareAndSet(false, true)) {
                        LOGGER.log(Level.SEVERE,
                                "ZooKeeper session expired — /health will return 503"
                                        + " so the pod is restarted.");
                    }
                    break;
                default:
                    // Disconnected / AuthFailed / etc. — log at FINE; the ZK client
                    // recovers from transient disconnects on its own.
                    break;
            }
        });
        zkRef.set(zk);
        if (!zkConnected.await(sessionTimeout, TimeUnit.MILLISECONDS)) {
            zk.close();
            throw new IllegalStateException("ZooKeeper connect timed out: " + zkAddress);
        }
        this.zooKeeper = zk;
        this.registry = new SegmentRegistryClient(zkRef::get, basePath);
        registry.ensureRoot();

        long intervalMs = configuration.getLong(
                OptimizerConfiguration.PROPERTY_INTERVAL_MS,
                OptimizerConfiguration.PROPERTY_INTERVAL_MS_DEFAULT);
        long retentionMs = configuration.getLong(
                OptimizerConfiguration.PROPERTY_RETENTION_MS,
                OptimizerConfiguration.PROPERTY_RETENTION_MS_DEFAULT);
        long targetMaxBytes = configuration.getLong(
                OptimizerConfiguration.PROPERTY_TARGET_MAX_BYTES,
                OptimizerConfiguration.PROPERTY_TARGET_MAX_BYTES_DEFAULT);
        int maxCount = configuration.getInt(
                OptimizerConfiguration.PROPERTY_MAX_COUNT,
                OptimizerConfiguration.PROPERTY_MAX_COUNT_DEFAULT);
        long perCycleMaxBytes = configuration.getLong(
                OptimizerConfiguration.PROPERTY_MAX_BYTES,
                OptimizerConfiguration.PROPERTY_MAX_BYTES_DEFAULT);
        this.eventDebounceMs = configuration.getLong(
                OptimizerConfiguration.PROPERTY_EVENT_DEBOUNCE_MS,
                OptimizerConfiguration.PROPERTY_EVENT_DEBOUNCE_MS_DEFAULT);

        // Issue #484: aggressive policy by default. Segments at or above
        // targetMaxBytes are graduated; everything else is mergeable as long
        // as ≥2 sub-target segments exist — there's no minCount/minBytes gate.
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                targetMaxBytes, perCycleMaxBytes, maxCount);

        // Resolve the merger (SPI override → preconfigured → RemoteSegmentMerger
        // → NoopMerger) and the DataStorageManager that drives it (only when
        // remote file servers are configured).
        this.merger = resolveMerger(zkAddress, basePath, discoveredFileServers);

        this.leaderLock = new OptimizerLeaderLock(zkRef::get, basePath, tablespaceUuid);
        boolean safeModeFileDeletion = configuration.getBoolean(
                OptimizerConfiguration.PROPERTY_SAFE_MODE_FILE_DELETION,
                OptimizerConfiguration.PROPERTY_SAFE_MODE_FILE_DELETION_DEFAULT);
        // The merger constructs its own DSM for the merge path. The reaper
        // can use the same DSM to physically delete files at retention if
        // safeMode is opted out (the operator's responsibility).
        this.engine = new IndexOptimizerEngine(registry, merger, tablespaceUuid, policy, retentionMs,
                () -> 0 /* MVP: assign new segments to instance 0 — see step 7 for owner-aware routing */,
                System::currentTimeMillis,
                mergerDataStorageManager,
                leaderLock,
                safeModeFileDeletion);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            FastThreadLocalThread t = new FastThreadLocalThread(r, "index-optimizer-engine");
            t.setDaemon(true);
            return t;
        });
        // Periodic safety-net tick. Bursty ingestion is handled by the
        // event-driven path below; this only fires when ZK has been quiet
        // for at least intervalMs.
        scheduler.scheduleAtFixedRate(this::tickSafe, intervalMs, intervalMs, TimeUnit.MILLISECONDS);

        // Issue #484: arm a persistent-recursive ZK watch on the registry's
        // tablespace path so that any new segment / state change anywhere
        // under it produces an immediate (debounced) tick. The watch fires
        // forever — no re-arming needed across single ZK events — and
        // survives disconnects (it does NOT survive session expiry, but the
        // optimizer pod has a single long-lived ZK session and operators
        // restart the pod if the session ever expires).
        armPersistentRecursiveWatch();

        // Admin HTTP endpoint (review item E1+E3). Disabled when port == 0; otherwise
        // exposes /health (Helm probe target) and /metrics (Prometheus scrape).
        int httpPort = configuration.getInt(
                OptimizerConfiguration.PROPERTY_HTTP_PORT,
                OptimizerConfiguration.PROPERTY_HTTP_PORT_DEFAULT);
        if (httpPort > 0) {
            String httpHost = configuration.getString(
                    OptimizerConfiguration.PROPERTY_HTTP_HOST,
                    OptimizerConfiguration.PROPERTY_HTTP_HOST_DEFAULT);
            // Liveness staleness = 2 × tick interval — the engine should have ticked
            // at least once in that window unless something is stuck (review-item B6
            // from second pr-reviewer pass).
            this.httpServer = new OptimizerHttpServer(httpHost, httpPort, engine,
                    /* stalenessThresholdMillis */ 2L * intervalMs,
                    System::currentTimeMillis,
                    /* criticalFailure */ this::isSessionExpired);
            this.httpServer.start();
        }

        LOGGER.log(Level.INFO,
                "index-optimizer started: zk={0}, basePath={1}, tablespace={2} (uuid={3}),"
                        + " intervalMs={4}, eventDebounceMs={5}, mergerType={6}, httpPort={7}",
                new Object[]{zkAddress, basePath, tablespaceName, tablespaceUuid,
                        intervalMs, eventDebounceMs, merger.getClass().getSimpleName(), httpPort});
    }

    private void tickSafe() {
        try {
            engine.runOnce();
        } catch (herddb.indexing.segment.SegmentRegistryException | RuntimeException e) {
            // Narrow catch (review item H1): the engine's runOnce now declares a typed
            // SegmentRegistryException; merger / scheduler failures still surface as
            // RuntimeException. Either way we log and let the next tick retry — a
            // misbehaving merger or transient ZK error must never kill the scheduler.
            LOGGER.log(Level.WARNING, "optimizer tick failed", e);
        }
    }

    // -------------------------------------------------------------------------
    // Event-driven scheduling (issue #484)
    // -------------------------------------------------------------------------

    private void armPersistentRecursiveWatch() {
        if (zooKeeper == null || registry == null || tablespaceUuid == null) {
            return;
        }
        String path = registry.tablespacePath(tablespaceUuid);
        Watcher eventWatcher = (WatchedEvent event) -> {
            // Only KeeperState.SyncConnected events carry a meaningful path; lifecycle
            // events (Disconnected, Expired) just advise us to re-arm or restart, but
            // for simplicity we always try to schedule a wakeup — the engine's
            // runOnce will short-circuit on connection loss anyway.
            if (event.getType() == Watcher.Event.EventType.None) {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    // Reconnect after a transient disconnect: the persistent-recursive
                    // watch is automatically re-registered by the ZK client, but we
                    // schedule a wakeup to catch up on any events that may have been
                    // missed during the disconnect.
                    scheduleEventDrivenTick();
                }
                return;
            }
            watcherEvents.incrementAndGet();
            scheduleEventDrivenTick();
        };
        try {
            // Make sure the parent znode exists so addWatch doesn't fail with NoNode.
            registry.ensureRoot();
            try {
                zooKeeper.create(path, new byte[0], org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        org.apache.zookeeper.CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ok) {
                // expected
            }
            zooKeeper.addWatch(path, eventWatcher, AddWatchMode.PERSISTENT_RECURSIVE);
            LOGGER.log(Level.INFO,
                    "armed persistent-recursive watch on {0} (event-driven scheduling enabled)",
                    path);
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            // Don't fail startup — the periodic safety-net tick still runs.
            LOGGER.log(Level.WARNING,
                    "failed to arm persistent-recursive watch on {0}: {1}; falling back to"
                            + " periodic-only scheduling",
                    new Object[]{path, e.getMessage()});
        } catch (herddb.indexing.segment.SegmentRegistryException e) {
            LOGGER.log(Level.WARNING,
                    "failed to ensure registry root before arming watch: {0}",
                    e.getMessage());
        }
    }

    /**
     * Coalescing scheduler for the event-driven path. The first event in a
     * burst flips {@link #pendingWakeup} from false→true and enqueues a
     * scheduled task on the engine's single-threaded executor with the
     * configured debounce delay. All later events in the same burst short-
     * circuit on the CAS — they observe pendingWakeup==true and return
     * without enqueuing.
     */
    private void scheduleEventDrivenTick() {
        if (scheduler == null) {
            return;
        }
        if (!pendingWakeup.compareAndSet(false, true)) {
            return;
        }
        long delay = Math.max(0L, eventDebounceMs);
        scheduler.schedule(this::runEventDrivenTick, delay, TimeUnit.MILLISECONDS);
    }

    private void runEventDrivenTick() {
        // Reset the flag BEFORE running the engine so that any event that
        // arrives while we're working schedules another wakeup. This is the
        // safe ordering: missing one event is bad (we'd wait until the next
        // periodic tick), running an extra event-driven tick is harmless.
        pendingWakeup.set(false);
        eventDrivenTicks.incrementAndGet();
        tickSafe();
    }

    // -------------------------------------------------------------------------
    // Merger resolution
    // -------------------------------------------------------------------------

    /**
     * Resolves the {@link SegmentMerger} for this optimizer. Resolution order:
     * <ol>
     *   <li>If a merger was passed to the constructor explicitly, use it (test path).</li>
     *   <li>Else if the {@link ServiceLoader} returns a registered SPI provider, use it
     *       (also a test override mechanism via {@code META-INF/services}).</li>
     *   <li>Else if remote file servers are configured (or discovered via ZK),
     *       construct a {@link RemoteSegmentMerger} backed by a freshly-built
     *       {@link DataStorageManager}.</li>
     *   <li>Else fall back to {@link NoopMerger} with a clear WARNING log so the
     *       service still starts in environments where no remote storage is wired.</li>
     * </ol>
     */
    private SegmentMerger resolveMerger(String zkAddress, String basePath,
                                        List<String> discoveredFileServers) throws IOException {
        if (preconfiguredMerger != null) {
            LOGGER.log(Level.INFO, "using pre-configured merger {0}",
                    preconfiguredMerger.getClass().getName());
            return preconfiguredMerger;
        }
        ServiceLoader<SegmentMerger> loader = ServiceLoader.load(SegmentMerger.class);
        for (SegmentMerger candidate : loader) {
            LOGGER.log(Level.INFO, "loaded segment merger SPI {0}", candidate.getClass().getName());
            return candidate;
        }
        // Build a RemoteSegmentMerger when remote-file servers are reachable.
        String staticServers = configuration.getString(
                OptimizerConfiguration.PROPERTY_REMOTE_FILE_SERVERS,
                OptimizerConfiguration.PROPERTY_REMOTE_FILE_SERVERS_DEFAULT);
        List<String> servers = new ArrayList<>();
        if (staticServers != null && !staticServers.isEmpty()) {
            for (String s : staticServers.split(",")) {
                String trimmed = s.trim();
                if (!trimmed.isEmpty()) {
                    servers.add(trimmed);
                }
            }
            LOGGER.log(Level.INFO, "remote file servers (static): {0}", servers);
        } else if (discoveredFileServers != null && !discoveredFileServers.isEmpty()) {
            servers.addAll(discoveredFileServers);
            LOGGER.log(Level.INFO, "remote file servers (ZK discovery): {0}", servers);
        }
        if (servers.isEmpty()) {
            LOGGER.log(Level.WARNING,
                    "no remote file servers configured (and ZK discovery returned none) —"
                            + " falling back to NoopMerger; the optimizer will decline every"
                            + " merge until a real merger is provided.");
            return new NoopMerger();
        }
        try {
            return buildRemoteSegmentMerger(servers, zkAddress, basePath);
        } catch (RuntimeException buildErr) {
            // Building the DSM is the plugin boundary — surface and fall back rather
            // than killing the pod. If construction fails (bad config, bad JVM args,
            // etc.) the operator gets a clear error in the logs and the optimizer
            // continues to run with NoopMerger so the registry plumbing is still
            // observable.
            LOGGER.log(Level.SEVERE,
                    "failed to construct RemoteSegmentMerger; falling back to NoopMerger: {0}",
                    buildErr.getMessage());
            return new NoopMerger();
        }
    }

    private RemoteSegmentMerger buildRemoteSegmentMerger(List<String> servers, String zkAddress,
                                                         String basePath) throws IOException {
        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put("remote.file.client.timeout",
                configuration.getLong(OptimizerConfiguration.PROPERTY_REMOTE_FILE_TIMEOUT,
                        OptimizerConfiguration.PROPERTY_REMOTE_FILE_TIMEOUT_DEFAULT));
        clientConfig.put("remote.file.client.retries",
                configuration.getInt(OptimizerConfiguration.PROPERTY_REMOTE_FILE_RETRIES,
                        OptimizerConfiguration.PROPERTY_REMOTE_FILE_RETRIES_DEFAULT));
        clientConfig.put(ServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_READ_BYTES,
                configuration.getLong(
                        OptimizerConfiguration.PROPERTY_REMOTE_FILE_MAX_INFLIGHT_READ_BYTES,
                        OptimizerConfiguration.PROPERTY_REMOTE_FILE_MAX_INFLIGHT_READ_BYTES_DEFAULT));
        clientConfig.put(ServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_WRITE_BYTES,
                configuration.getLong(
                        OptimizerConfiguration.PROPERTY_REMOTE_FILE_MAX_INFLIGHT_WRITE_BYTES,
                        OptimizerConfiguration.PROPERTY_REMOTE_FILE_MAX_INFLIGHT_WRITE_BYTES_DEFAULT));

        RemoteFileServiceFactory factory = RemoteFileServiceFactory.load();
        this.mergerFileClient = factory.createClient(servers, clientConfig);
        Path tmpDir = resolveTmpDirectory();
        Path metaDir = tmpDir.resolve("merger-metadata");
        Path remoteTmp = tmpDir.resolve("merger-remote-tmp");
        Files.createDirectories(metaDir);
        Files.createDirectories(remoteTmp);
        this.mergerDataStorageManager = factory.createDataStorageManager(
                metaDir, remoteTmp, Integer.MAX_VALUE, mergerFileClient);

        int dim = configuration.getInt("indexoptimizer.merge.dim", 0);
        if (dim <= 0) {
            // The merger needs the dimension up front. Fall back: we don't
            // currently store dim in the segment registry, so operators must
            // configure it explicitly. If it's missing we refuse to merge.
            throw new IllegalStateException("indexoptimizer.merge.dim must be set to the index"
                    + " vector dimension (no inference is currently supported)");
        }
        int graphM = configuration.getInt(OptimizerConfiguration.PROPERTY_MERGE_M,
                OptimizerConfiguration.PROPERTY_MERGE_M_DEFAULT);
        int beamWidth = configuration.getInt(OptimizerConfiguration.PROPERTY_MERGE_BEAM_WIDTH,
                OptimizerConfiguration.PROPERTY_MERGE_BEAM_WIDTH_DEFAULT);
        float neighborOverflow = configuration.getFloat(
                OptimizerConfiguration.PROPERTY_MERGE_NEIGHBOR_OVERFLOW,
                OptimizerConfiguration.PROPERTY_MERGE_NEIGHBOR_OVERFLOW_DEFAULT);
        float alpha = configuration.getFloat(OptimizerConfiguration.PROPERTY_MERGE_ALPHA,
                OptimizerConfiguration.PROPERTY_MERGE_ALPHA_DEFAULT);
        String similarityName = configuration.getString(
                OptimizerConfiguration.PROPERTY_MERGE_SIMILARITY,
                OptimizerConfiguration.PROPERTY_MERGE_SIMILARITY_DEFAULT);
        VectorSimilarityFunction similarity;
        try {
            similarity = VectorSimilarityFunction.valueOf(similarityName);
        } catch (IllegalArgumentException badName) {
            throw new IllegalStateException(
                    "indexoptimizer.merge.similarity has unsupported value: " + similarityName
                            + " (expected one of " + Arrays.toString(VectorSimilarityFunction.values())
                            + ")", badName);
        }
        return new RemoteSegmentMerger(mergerDataStorageManager, tmpDir,
                dim, graphM, beamWidth, neighborOverflow, alpha, similarity);
    }

    private Path resolveTmpDirectory() throws IOException {
        String configured = configuration.getString(OptimizerConfiguration.PROPERTY_TMP_DIR, null);
        if (configured == null) {
            // Java system property (set by Helm via -D) takes precedence over the
            // OS default tmp dir.
            configured = System.getProperty(OptimizerConfiguration.PROPERTY_TMP_DIR,
                    System.getProperty("java.io.tmpdir"));
        }
        Path tmpDir = Paths.get(configured);
        Files.createDirectories(tmpDir);
        return tmpDir;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    public synchronized void shutdown() {
        if (httpServer != null) {
            try {
                httpServer.close();
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "http server close failed: {0}", e.getMessage());
            }
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        // Release the leader lock BEFORE closing the ZK client so the ephemeral
        // znode is gone immediately; otherwise a peer waiting on session-expiry
        // would have to wait the full session timeout to take over.
        if (leaderLock != null) {
            try {
                leaderLock.release();
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "leader lock release on shutdown failed: {0}",
                        e.getMessage());
            }
        }
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (mergerFileClient instanceof AutoCloseable) {
            try {
                ((AutoCloseable) mergerFileClient).close();
            } catch (Exception e) {
                // Broad catch: the client's close() is a best-effort cleanup
                // path during shutdown. Log and continue rather than masking
                // an earlier exception.
                LOGGER.log(Level.WARNING, "merger file client close failed: {0}",
                        e.getMessage());
            }
        }
        shutdownLatch.countDown();
    }

    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }

    /**
     * CLI entry point: {@code service index-optimizer console <props-file>}.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: IndexOptimizerMain <properties-file>");
            System.exit(1);
        }
        Properties properties = new Properties();
        try (FileInputStream in = new FileInputStream(args[0])) {
            properties.load(in);
        }
        OptimizerConfiguration configuration = new OptimizerConfiguration(properties);
        IndexOptimizerMain optimizer = new IndexOptimizerMain(configuration);
        Runtime.getRuntime().addShutdownHook(new Thread(optimizer::shutdown,
                "index-optimizer-shutdown"));
        optimizer.start();
        optimizer.awaitShutdown();
    }

    /**
     * Backward-compatibility helper for tests that exercise the SPI fallback
     * path: returns the first {@link SegmentMerger} registered through
     * {@link ServiceLoader}, or a {@link NoopMerger} when none is registered.
     * Production code uses {@link #resolveMerger} (called from {@link #start})
     * which adds the {@link RemoteSegmentMerger} fallback when a remote DSM
     * can be wired.
     */
    public static SegmentMerger loadMergerSpi() {
        ServiceLoader<SegmentMerger> loader = ServiceLoader.load(SegmentMerger.class);
        for (SegmentMerger candidate : loader) {
            LOGGER.log(Level.INFO, "loaded segment merger SPI {0}", candidate.getClass().getName());
            return candidate;
        }
        LOGGER.log(Level.WARNING,
                "no SegmentMerger SPI registered; using NoopMerger — the optimizer will decline"
                        + " every merge until a real merger is provided.");
        return new NoopMerger();
    }

    /**
     * Inert {@link SegmentMerger} that logs and returns {@code null}, declining the
     * merge attempt. Used as the fallback in unit-test environments and any
     * deployment that does not have remote file servers configured (the
     * registry plumbing still runs and ticks are still scheduled, but no merge
     * ever lands until a real merger is wired).
     */
    public static final class NoopMerger implements SegmentMerger {
        @Override
        public herddb.indexing.segment.SegmentMetadata merge(
                java.util.List<herddb.indexing.segment.SegmentMetadata> inputs, int newOwnerInstance) {
            LOGGER.log(Level.INFO,
                    "NoopMerger declining merge of {0} segments (real merger not configured)",
                    inputs.size());
            return null;
        }
    }
}
