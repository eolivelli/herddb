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

package herddb.mysql.protocol;

import java.util.concurrent.CompletableFuture;

/**
 * Callback interface for handling MySQL commands.
 */
public interface MySQLCommandHandler {

    /**
     * Authenticate a client connection.
     *
     * @param username          the username
     * @param scrambledPassword the scrambled password from the client
     * @param challenge         the 20-byte challenge sent to the client
     * @param database          the database name (may be null)
     * @return true if authentication succeeds
     */
    boolean authenticate(String username, byte[] scrambledPassword, byte[] challenge, String database);

    /**
     * Execute a SQL query.
     *
     * @param connectionId the connection ID
     * @param sql          the SQL statement
     * @return a future that completes with the query result
     */
    CompletableFuture<QueryResult> executeQuery(long connectionId, String sql);

    /**
     * Switch the current database for a connection.
     *
     * @param connectionId the connection ID
     * @param database     the database name
     */
    void useDatabase(long connectionId, String database);

    /**
     * Notification that a connection has been closed.
     *
     * @param connectionId the connection ID
     */
    void connectionClosed(long connectionId);
}
