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

package herddb.mysql;

import herddb.core.DBManager;
import herddb.mysql.protocol.MySQLServer;
import herddb.server.CustomTransport;
import herddb.server.Server;
import herddb.server.ServerConfiguration;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * CustomTransport implementation that starts a MySQL-protocol-compatible server.
 */
public class MySQLTransport implements CustomTransport {

    private static final Logger LOGGER = Logger.getLogger(MySQLTransport.class.getName());

    public static final String PROPERTY_MYSQL_ENABLED = "mysql.protocol.enabled";
    public static final boolean PROPERTY_MYSQL_ENABLED_DEFAULT = false;
    public static final String PROPERTY_MYSQL_PORT = "mysql.protocol.port";
    public static final int PROPERTY_MYSQL_PORT_DEFAULT = 3307;
    public static final String PROPERTY_MYSQL_HOST = "mysql.protocol.host";
    public static final String PROPERTY_MYSQL_HOST_DEFAULT = "0.0.0.0";

    private boolean enabled;
    private String host;
    private int port;
    private MySQLServer mysqlServer;
    private HerdDBMySQLCommandHandler handler;

    @Override
    public void init(ServerConfiguration configuration, DBManager manager, Server server) {
        this.enabled = configuration.getBoolean(PROPERTY_MYSQL_ENABLED, PROPERTY_MYSQL_ENABLED_DEFAULT);
        if (!enabled) {
            LOGGER.log(Level.INFO, "MySQL protocol transport is disabled");
            return;
        }
        this.host = configuration.getString(PROPERTY_MYSQL_HOST, PROPERTY_MYSQL_HOST_DEFAULT);
        this.port = configuration.getInt(PROPERTY_MYSQL_PORT, PROPERTY_MYSQL_PORT_DEFAULT);
        this.handler = new HerdDBMySQLCommandHandler(manager, server.getUserManager());
        this.mysqlServer = new MySQLServer(host, port, handler);
        LOGGER.log(Level.INFO, "MySQL protocol transport initialized on {0}:{1}",
                new Object[]{host, port});
    }

    @Override
    public void start() throws Exception {
        if (!enabled) {
            return;
        }
        mysqlServer.start();
        LOGGER.log(Level.INFO, "MySQL protocol transport started on {0}:{1}",
                new Object[]{host, mysqlServer.getPort()});
    }

    @Override
    public void close() throws Exception {
        if (mysqlServer != null) {
            mysqlServer.close();
            LOGGER.log(Level.INFO, "MySQL protocol transport stopped");
        }
    }

    public int getPort() {
        return mysqlServer != null ? mysqlServer.getPort() : port;
    }

    public boolean isEnabled() {
        return enabled;
    }
}
