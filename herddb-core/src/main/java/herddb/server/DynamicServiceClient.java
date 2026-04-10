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

import java.util.List;

/**
 * Interface for clients that support dynamic server list updates.
 *
 * @author enrico.olivelli
 */
public interface DynamicServiceClient {
    void updateServers(List<String> servers);

    /**
     * Blocks for up to {@code timeoutMs} milliseconds until the client has at
     * least one server in its routing table. Returns {@code true} as soon as a
     * server is visible, {@code false} on timeout. Intended for use at
     * bootstrap on a cold cluster where service discovery may not yet have
     * populated the server list by the time the first RPC is issued.
     */
    boolean awaitServersReady(long timeoutMs) throws InterruptedException;
}
