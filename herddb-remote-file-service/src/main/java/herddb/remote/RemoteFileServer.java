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

package herddb.remote;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Configurable gRPC server for RemoteFileService.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServer implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileServer.class.getName());

    private final String host;
    private final int port;
    private final Path dataDirectory;
    private Server server;

    public RemoteFileServer(String host, int port, Path dataDirectory) {
        this.host = host;
        this.port = port;
        this.dataDirectory = dataDirectory;
    }

    public RemoteFileServer(int port, Path dataDirectory) {
        this("0.0.0.0", port, dataDirectory);
    }

    public void start() throws IOException {
        RemoteFileServiceImpl serviceImpl = new RemoteFileServiceImpl(dataDirectory);
        server = ServerBuilder.forPort(port)
                .addService(serviceImpl)
                .build()
                .start();
        LOGGER.log(Level.INFO, "RemoteFileServer started on port {0}, data dir: {1}",
                new Object[]{port, dataDirectory});
    }

    public int getPort() {
        return server != null ? server.getPort() : port;
    }

    public String getHost() {
        return host;
    }

    public String getAddress() {
        return host + ":" + getPort();
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            LOGGER.log(Level.INFO, "RemoteFileServer stopped");
        }
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}
