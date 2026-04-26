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
 * Summary of a primary-key index, returned by
 * {@code GET /api/v2/tablespaces/{ts}/tables/{t}/primary-index}.
 *
 * <p>Currently scoped to high-level B-Link counters (entries, in-memory
 * nodes, used memory). Detailed per-node enumeration is intentionally
 * deferred to a follow-up because it requires forcing a checkpoint.
 */
public final class PrimaryIndexDTO {

    private String type;
    private long entries;
    private int loadedNodes;
    private long usedMemoryBytes;

    public PrimaryIndexDTO() {
    }

    public PrimaryIndexDTO(String type, long entries, int loadedNodes, long usedMemoryBytes) {
        this.type = type;
        this.entries = entries;
        this.loadedNodes = loadedNodes;
        this.usedMemoryBytes = usedMemoryBytes;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getEntries() {
        return entries;
    }

    public void setEntries(long entries) {
        this.entries = entries;
    }

    public int getLoadedNodes() {
        return loadedNodes;
    }

    public void setLoadedNodes(int loadedNodes) {
        this.loadedNodes = loadedNodes;
    }

    public long getUsedMemoryBytes() {
        return usedMemoryBytes;
    }

    public void setUsedMemoryBytes(long usedMemoryBytes) {
        this.usedMemoryBytes = usedMemoryBytes;
    }
}
