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

package herddb.server;

import herddb.daemons.PidFileLocker;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.component.Lifecycle;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.server.EmbeddedServer;
import org.apache.bookkeeper.server.conf.BookieConfiguration;

/**
 * Simple wrapper for standalone BookKeeper bookie server (for cluster mode)
 *
 * @author enrico.olivelli
 */
public class BookKeeperMainWrapper implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(BookKeeperMainWrapper.class.getName());

    private final Properties configuration;
    private final PidFileLocker pidFileLocker;
    private EmbeddedServer embeddedServer;

    private static BookKeeperMainWrapper runningInstance;

    public BookKeeperMainWrapper(Properties configuration) {
        this.configuration = configuration;
        this.pidFileLocker = new PidFileLocker(Paths.get(System.getProperty("user.dir", ".")).toAbsolutePath());
    }

    @Override
    public void close() {
        if (embeddedServer != null) {
            LOG.info("Apache BookKeeper stopping");
            try {
                embeddedServer.getLifecycleComponentStack().close();
                if (waitForBookieServiceState(Lifecycle.State.CLOSED)) {
                    LOG.info("Apache BookKeeper stopped");
                } else {
                    LOG.warning("Apache BookKeeper stop request did not complete in time");
                }
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
            } finally {
                embeddedServer = null;
            }
        }
    }

    public static void main(String... args) {
        try {
            String here = new File(System.getProperty("user.dir")).getAbsolutePath();
            LOG.severe("Starting BookKeeper bookie from HerdDB package version " + herddb.utils.Version.getVERSION());
            Properties configuration = new Properties();

            boolean configFileFromParameter = false;
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (!arg.startsWith("-")) {
                    File configFile = new File(args[i]).getAbsoluteFile();
                    LOG.severe("Reading configuration from " + configFile);
                    try (InputStreamReader reader =
                                 new InputStreamReader(new FileInputStream(configFile), StandardCharsets.UTF_8)) {
                        configuration.load(reader);
                    }
                    configFileFromParameter = true;
                } else if (arg.equals("--use-env")) {
                    System.getenv().forEach((key, value) -> {
                        System.out.println("Considering env as system property " + key + " -> " + value);
                        System.setProperty(key, value);
                    });
                } else if (arg.startsWith("-D")) {
                    int equals = arg.indexOf('=');
                    if (equals > 0) {
                        String key = arg.substring(2, equals);
                        String value = arg.substring(equals + 1);
                        System.setProperty(key, value);
                    }
                }
            }
            if (!configFileFromParameter) {
                File configFile = new File("conf/bookie.properties").getAbsoluteFile();
                System.out.println("Reading configuration from " + configFile);
                if (configFile.isFile()) {
                    try (InputStreamReader reader = new InputStreamReader(new FileInputStream(configFile), StandardCharsets.UTF_8)) {
                        configuration.load(reader);
                    }
                }
            }

            System.getProperties().forEach((k, v) -> {
                String key = k + "";
                if (!key.startsWith("java") && !key.startsWith("user")) {
                    configuration.put(k, v);
                }
            });

            for (Object key : configuration.keySet()) {
                String value = configuration.getProperty(key.toString());
                String newvalue = value.replace("${user.dir}", here);
                configuration.put(key, newvalue);
            }

            LogManager.getLogManager().readConfiguration();

            Runtime.getRuntime().addShutdownHook(new Thread("ctrlc-hook") {
                @Override
                public void run() {
                    System.out.println("Ctrl-C trapped. Shutting down");
                    BookKeeperMainWrapper _instance = runningInstance;
                    if (_instance != null) {
                        _instance.close();
                        Runtime.getRuntime().halt(0);
                    }
                }
            });
            runningInstance = new BookKeeperMainWrapper(configuration);
            runningInstance.run();

        } catch (Throwable t) {
            t.printStackTrace();
            Runtime.getRuntime().halt(1);
        }
    }

    public void run() throws Exception {
        pidFileLocker.lock();

        org.apache.bookkeeper.conf.ServerConfiguration conf = new org.apache.bookkeeper.conf.ServerConfiguration();

        // Core settings
        String zkServers = configuration.getProperty("zkServers", "localhost:2181");
        conf.setZkServers(zkServers);
        conf.setZkLedgersRootPath(configuration.getProperty("zkLedgersRootPath", "/ledgers"));

        int port = Integer.parseInt(configuration.getProperty("bookiePort", "3181"));
        conf.setBookiePort(port);

        // Data directories
        String ledgerDirs = configuration.getProperty("ledgerDirNames", "dbdata/bookie/ledgers");
        String journalDir = configuration.getProperty("journalDirName", "dbdata/bookie/journal");

        Path ledgerPath = Paths.get(ledgerDirs).toAbsolutePath();
        Path journalPath = Paths.get(journalDir).toAbsolutePath();
        Files.createDirectories(ledgerPath);
        Files.createDirectories(journalPath);

        conf.setLedgerDirNames(new String[]{ledgerPath.toString()});
        conf.setJournalDirName(journalPath.toString());

        // Sensible defaults matching EmbeddedBookie
        conf.setUseHostNameAsBookieID(true);
        conf.setLedgerManagerFactoryClass(HierarchicalLedgerManagerFactory.class);
        conf.setAutoRecoveryDaemonEnabled(false);
        conf.setEnableLocalTransport(true);
        conf.setFlushInterval(1000);
        conf.setMaxBackupJournals(5);
        conf.setMaxJournalSizeMB(1048);
        conf.setNumAddWorkerThreads(8);
        conf.setMaxPendingReadRequestPerThread(200000);
        conf.setMaxPendingAddRequestPerThread(200000);
        conf.setProperty("journalMaxGroupWaitMSec", 10L);
        conf.setJournalFlushWhenQueueEmpty(true);
        conf.setStatisticsEnabled(true);

        // Apply all remaining properties from configuration file
        for (String key : configuration.stringPropertyNames()) {
            if (!key.equals("zkServers") && !key.equals("zkLedgersRootPath")
                    && !key.equals("bookiePort") && !key.equals("ledgerDirNames")
                    && !key.equals("journalDirName")) {
                conf.setProperty(key, configuration.getProperty(key));
            }
        }

        long _start = System.currentTimeMillis();
        LOG.severe("Booting Apache BookKeeper bookie on port " + port
                + ", ZK: " + zkServers
                + ", ledgers: " + ledgerPath
                + ", journal: " + journalPath);

        // Format ZK metadata if needed
        boolean result = BookKeeperAdmin.format(conf, false, false);
        if (result) {
            LOG.info("BookKeeperAdmin.format: created a new workspace on ZK");
        } else {
            LOG.info("BookKeeperAdmin.format: ZK space does not need a format operation");
        }

        BookieConfiguration bkConf = new BookieConfiguration(conf);
        this.embeddedServer = EmbeddedServer.builder(bkConf).build();
        embeddedServer.getLifecycleComponentStack().start();

        if (waitForBookieServiceState(Lifecycle.State.STARTED)) {
            LOG.info("Apache BookKeeper bookie started");
        } else {
            LOG.warning("Apache BookKeeper bookie start request did not complete in time");
        }

        long _stop = System.currentTimeMillis();
        LOG.severe("Booting Apache BookKeeper bookie finished. Time " + (_stop - _start) + " ms");

        // Block the main thread
        Thread.currentThread().join();
    }

    private boolean waitForBookieServiceState(Lifecycle.State expectedState) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            Lifecycle.State currentState = embeddedServer.getBookieService().lifecycleState();
            if (currentState == expectedState) {
                return true;
            }
            Thread.sleep(500);
        }
        return false;
    }
}
