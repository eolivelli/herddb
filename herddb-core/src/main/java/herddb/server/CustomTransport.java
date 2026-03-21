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

import herddb.core.DBManager;

/**
 * SPI interface for custom transport protocols.
 * Implementations are discovered via ServiceLoader.
 */
public interface CustomTransport extends AutoCloseable {

    /**
     * Initialize the transport with server context.
     */
    void init(ServerConfiguration configuration, DBManager manager, Server server);

    /**
     * Start accepting connections.
     */
    void start() throws Exception;

    /**
     * Stop the transport and release resources.
     */
    @Override
    void close() throws Exception;
}
