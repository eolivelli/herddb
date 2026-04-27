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

import java.util.List;

/**
 * Wire-shape for a row of {@code systablespaces}, returned by
 * {@code GET /api/v2/tablespaces}.
 */
public final class TablespaceDTO {

    private String name;
    private String uuid;
    private String leader;
    private List<String> replicas;
    private int expectedReplicaCount;
    private long maxLeaderInactivityTime;

    public TablespaceDTO() {
        // Default constructor for Jackson deserialisation in tests.
    }

    public TablespaceDTO(
            String name,
            String uuid,
            String leader,
            List<String> replicas,
            int expectedReplicaCount,
            long maxLeaderInactivityTime) {
        this.name = name;
        this.uuid = uuid;
        this.leader = leader;
        this.replicas = replicas;
        this.expectedReplicaCount = expectedReplicaCount;
        this.maxLeaderInactivityTime = maxLeaderInactivityTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public List<String> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<String> replicas) {
        this.replicas = replicas;
    }

    public int getExpectedReplicaCount() {
        return expectedReplicaCount;
    }

    public void setExpectedReplicaCount(int expectedReplicaCount) {
        this.expectedReplicaCount = expectedReplicaCount;
    }

    public long getMaxLeaderInactivityTime() {
        return maxLeaderInactivityTime;
    }

    public void setMaxLeaderInactivityTime(long maxLeaderInactivityTime) {
        this.maxLeaderInactivityTime = maxLeaderInactivityTime;
    }
}
