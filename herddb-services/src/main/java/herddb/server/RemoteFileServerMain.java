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
import herddb.remote.RemoteFileServer;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/**
 * Launcher for RemoteFileServer as a system service.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServerMain {

    private static final Logger LOG = Logger.getLogger(RemoteFileServerMain.class.getName());

    private static RemoteFileServer runningServer;

    public static void main(String... args) throws Exception {
        Properties configuration = new Properties();

        boolean configFileFromParameter = false;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("-")) {
                File configFile = new File(arg).getAbsoluteFile();
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
            } else if (arg.startsWith("--port=")) {
                configuration.setProperty("port", arg.substring("--port=".length()));
            } else if (arg.startsWith("--data-dir=")) {
                configuration.setProperty("data.dir", arg.substring("--data-dir=".length()));
            } else if (arg.startsWith("--bind-host=")) {
                configuration.setProperty("bind.host", arg.substring("--bind-host=".length()));
            }
        }

        if (!configFileFromParameter) {
            File configFile = new File("conf/fileserver.properties").getAbsoluteFile();
            System.out.println("Reading configuration from " + configFile);
            if (configFile.isFile()) {
                try (InputStreamReader reader =
                             new InputStreamReader(new FileInputStream(configFile), StandardCharsets.UTF_8)) {
                    configuration.load(reader);
                }
            }
        }

        // Allow -D system properties to override config file values
        System.getProperties().forEach((k, v) -> {
            String key = k + "";
            if (!key.startsWith("java") && !key.startsWith("user")) {
                configuration.put(k, v);
            }
        });

        int port = Integer.parseInt(configuration.getProperty("port", "9845"));
        String bindHost = configuration.getProperty("bind.host", "0.0.0.0");
        String dataDir = configuration.getProperty("data.dir", "fileserver_" + port);

        CountDownLatch shutdownLatch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("ctrlc-hook") {
            @Override
            public void run() {
                System.out.println("Ctrl-C trapped. Shutting down");
                RemoteFileServer server = runningServer;
                if (server != null) {
                    try {
                        server.stop();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                shutdownLatch.countDown();
            }
        });

        PidFileLocker pidFileLocker = new PidFileLocker(Paths.get(System.getProperty("user.dir", ".")).toAbsolutePath());
        pidFileLocker.lock();

        java.nio.file.Path dataDirPath = Paths.get(dataDir).toAbsolutePath();
        System.out.println("Starting RemoteFileServer on " + bindHost + ":" + port + ", data dir: " + dataDirPath);

        runningServer = new RemoteFileServer(bindHost, port, dataDirPath);
        try {
            runningServer.start();
            shutdownLatch.await();
        } finally {
            pidFileLocker.close();
        }
    }
}
