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

import herddb.indexing.segment.SegmentRegistryClient;
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
        String tablespaceUuid = configuration.getString(
                OptimizerConfiguration.PROPERTY_TABLESPACE_UUID, null);
        if (tablespaceUuid == null || tablespaceUuid.isEmpty()) {
            throw new IllegalArgumentException(OptimizerConfiguration.PROPERTY_TABLESPACE_UUID
                    + " must be set");
        }

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
        this.engine = new IndexOptimizerEngine(registry, merger, tablespaceUuid, policy, retentionMs,
                () -> 0 /* MVP: assign new segments to instance 0 — see step 7 for owner-aware routing */,
                System::currentTimeMillis,
                /* dataStorageManager — wired by future production deployments */ null,
                leaderLock);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "index-optimizer-engine");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::tickSafe, intervalMs, intervalMs, TimeUnit.MILLISECONDS);

        LOGGER.log(Level.INFO,
                "index-optimizer started: zk={0}, basePath={1}, tablespace={2}, intervalMs={3}",
                new Object[]{zkAddress, basePath, tablespaceUuid, intervalMs});
    }

    private void tickSafe() {
        try {
            engine.runOnce();
        } catch (Exception e) {
            // Broad catch is intentional: a misbehaving merger or transient ZK error must
            // never kill the scheduler. Logged at WARNING; the next tick will retry.
            LOGGER.log(Level.WARNING, "optimizer tick failed", e);
        }
    }

    public synchronized void shutdown() {
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
                "no SegmentMerger SPI registered; using NoopMerger — the optimizer will declines"
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
