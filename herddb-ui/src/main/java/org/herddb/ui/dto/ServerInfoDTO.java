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

package org.herddb.ui.dto;

/**
 * Server-level metadata returned by {@code GET /api/v2/server-info}.
 * Used by the SPA header to render the node id, the deployment mode, and
 * the JDBC URL (mainly for support/debug copy-paste).
 */
public final class ServerInfoDTO {

    private String nodeId;
    private String mode;
    private String jdbcUrl;
    private String defaultTablespace;

    public ServerInfoDTO() {
        // Default constructor for Jackson deserialisation in tests.
    }

    public ServerInfoDTO(String nodeId, String mode, String jdbcUrl, String defaultTablespace) {
        this.nodeId = nodeId;
        this.mode = mode;
        this.jdbcUrl = jdbcUrl;
        this.defaultTablespace = defaultTablespace;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getDefaultTablespace() {
        return defaultTablespace;
    }

    public void setDefaultTablespace(String defaultTablespace) {
        this.defaultTablespace = defaultTablespace;
    }
}
