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
import io.netty.util.concurrent.FastThreadLocalThread;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
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
 *   <li>Resolve a {@link SegmentMerger} via the standard {@link ServiceLoader}
 *       SPI. Production deployments register an implementation that wraps the
 *       real graph merger; until that wiring is in place the service starts
 *       with a {@code NoopMerger} that logs and declines every merge — useful
 *       for integration tests that exercise the registry plumbing.</li>
 *   <li>Schedule {@link IndexOptimizerEngine#runOnce()} on a fixed interval.</li>
 * </ul>
 *
 * <p>Singleton enforcement is delegated to the deployment layer (Helm
 * {@code replicas: 1}); two optimizers running concurrently will fight over
 * registry CAS but never corrupt the registry — they will simply waste cycles.
 */
public final class IndexOptimizerMain {

    private static final Logger LOGGER = Logger.getLogger(IndexOptimizerMain.class.getName());

    private final OptimizerConfiguration configuration;
    private final SegmentMerger merger;

    private ZooKeeper zooKeeper;
    private ScheduledExecutorService scheduler;
    private SegmentRegistryClient registry;
    private OptimizerLeaderLock leaderLock;
    private IndexOptimizerEngine engine;
    private OptimizerHttpServer httpServer;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public IndexOptimizerMain(OptimizerConfiguration configuration, SegmentMerger merger) {
        this.configuration = configuration;
        this.merger = merger;
    }

    public IndexOptimizerEngine getEngine() {
        return engine;
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
        String tablespaceUuid;
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
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                zkConnected.countDown();
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
        int minCount = configuration.getInt(
                OptimizerConfiguration.PROPERTY_MIN_COUNT,
                OptimizerConfiguration.PROPERTY_MIN_COUNT_DEFAULT);
        int maxCount = configuration.getInt(
                OptimizerConfiguration.PROPERTY_MAX_COUNT,
                OptimizerConfiguration.PROPERTY_MAX_COUNT_DEFAULT);
        long minBytes = configuration.getLong(
                OptimizerConfiguration.PROPERTY_MIN_BYTES,
                OptimizerConfiguration.PROPERTY_MIN_BYTES_DEFAULT);
        long maxBytes = configuration.getLong(
                OptimizerConfiguration.PROPERTY_MAX_BYTES,
                OptimizerConfiguration.PROPERTY_MAX_BYTES_DEFAULT);

        MergePolicy policy = new MergePolicy.SmallestFirstPolicy(minCount, maxCount, minBytes, maxBytes);
        this.leaderLock = new OptimizerLeaderLock(zkRef::get, basePath, tablespaceUuid);
        boolean safeModeFileDeletion = configuration.getBoolean(
                OptimizerConfiguration.PROPERTY_SAFE_MODE_FILE_DELETION,
                OptimizerConfiguration.PROPERTY_SAFE_MODE_FILE_DELETION_DEFAULT);
        // Production deployments today have no DataStorageManager wired (the merger
        // SPI provides its own). When safeMode is true (default) this is fine; when
        // operators opt out we still pass null because file-deletion-from-the-engine
        // requires a real DSM and that's a future-work item — the constructor will
        // refuse the unsafe combination.
        this.engine = new IndexOptimizerEngine(registry, merger, tablespaceUuid, policy, retentionMs,
                () -> 0 /* MVP: assign new segments to instance 0 — see step 7 for owner-aware routing */,
                System::currentTimeMillis,
                /* dataStorageManager — wired by future production deployments */ null,
                leaderLock,
                safeModeFileDeletion);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            FastThreadLocalThread t = new FastThreadLocalThread(r, "index-optimizer-engine");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::tickSafe, intervalMs, intervalMs, TimeUnit.MILLISECONDS);

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
                    System::currentTimeMillis);
            this.httpServer.start();
        }

        LOGGER.log(Level.INFO,
                "index-optimizer started: zk={0}, basePath={1}, tablespace={2} (uuid={3}), intervalMs={4}, httpPort={5}",
                new Object[]{zkAddress, basePath, tablespaceName, tablespaceUuid, intervalMs, httpPort});
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
        SegmentMerger merger = loadMergerSpi();
        IndexOptimizerMain optimizer = new IndexOptimizerMain(configuration, merger);
        Runtime.getRuntime().addShutdownHook(new Thread(optimizer::shutdown,
                "index-optimizer-shutdown"));
        optimizer.start();
        optimizer.awaitShutdown();
    }

    /**
     * Resolve a {@link SegmentMerger} via the {@link ServiceLoader} SPI. If no
     * provider is registered, fall back to a {@link NoopMerger} that logs and
     * declines every merge. Production deployments register a real merger
     * (extracted from {@code VectorIndexCompactor}) on the classpath.
     */
    static SegmentMerger loadMergerSpi() {
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
     * merge attempt. Used as the default until a graph-aware merger is provided.
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
