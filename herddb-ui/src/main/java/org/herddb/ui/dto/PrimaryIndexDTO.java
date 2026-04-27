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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Summary of a primary-key index, returned by
 * {@code GET /api/v2/tablespaces/{ts}/tables/{t}/primary-index}.
 *
 * <p>For B-Link indexes the response carries:
 * <ul>
 *   <li>top-level counters ({@code entries}, {@code loadedNodes},
 *       {@code usedMemoryBytes}) for the summary panel;</li>
 *   <li>a {@code nodes} list (one entry per BLink node, both on-disk and
 *       in-memory) for the layout heatmap;</li>
 *   <li>a {@code truncated} flag set when the tree is larger than the
 *       per-snapshot node cap and the node list was clipped.</li>
 * </ul>
 *
 * <p>For non-BLink primary indexes (e.g. the in-memory
 * {@code ConcurrentMapKeyToPageIndex} used in local mode) the {@code nodes}
 * list is empty and {@code truncated} is {@code false}.
 */
public final class PrimaryIndexDTO {

    private String type;
    private long entries;
    private int loadedNodes;
    private int totalNodes;
    private long usedMemoryBytes;
    private List<BLinkNodeDTO> nodes = Collections.emptyList();
    private boolean truncated;

    public PrimaryIndexDTO() {
    }

    /**
     * Legacy 4-argument constructor retained for callers that don't ship
     * per-node details (non-BLink fallback). The {@code nodes} list defaults
     * to empty, {@code truncated} to {@code false} and {@code totalNodes} to
     * the supplied {@code loadedNodes}.
     */
    public PrimaryIndexDTO(String type, long entries, int loadedNodes, long usedMemoryBytes) {
        this(type, entries, loadedNodes, loadedNodes, usedMemoryBytes,
                Collections.emptyList(), false);
    }

    public PrimaryIndexDTO(String type, long entries, int loadedNodes, int totalNodes,
                           long usedMemoryBytes, List<BLinkNodeDTO> nodes, boolean truncated) {
        this.type = type;
        this.entries = entries;
        this.loadedNodes = loadedNodes;
        this.totalNodes = totalNodes;
        this.usedMemoryBytes = usedMemoryBytes;
        this.nodes = nodes == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(nodes));
        this.truncated = truncated;
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

    public int getTotalNodes() {
        return totalNodes;
    }

    public void setTotalNodes(int totalNodes) {
        this.totalNodes = totalNodes;
    }

    public long getUsedMemoryBytes() {
        return usedMemoryBytes;
    }

    public void setUsedMemoryBytes(long usedMemoryBytes) {
        this.usedMemoryBytes = usedMemoryBytes;
    }

    public List<BLinkNodeDTO> getNodes() {
        return nodes;
    }

    public void setNodes(List<BLinkNodeDTO> nodes) {
        this.nodes = nodes == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(nodes));
    }

    public boolean isTruncated() {
        return truncated;
    }

    public void setTruncated(boolean truncated) {
        this.truncated = truncated;
    }

    /**
     * Wire-shape for a single BLink node: stable in-memory id, on-disk page
     * id (or {@code -1} when the node has never been flushed), leaf flag,
     * key count, byte size, loaded and dirty flags.
     */
    public static final class BLinkNodeDTO {

        private long nodeId;
        private long storeId;
        private boolean leaf;
        private int keys;
        private long sizeBytes;
        private boolean loaded;
        private boolean dirty;

        public BLinkNodeDTO() {
        }

        public BLinkNodeDTO(long nodeId, long storeId, boolean leaf, int keys,
                            long sizeBytes, boolean loaded, boolean dirty) {
            this.nodeId = nodeId;
            this.storeId = storeId;
            this.leaf = leaf;
            this.keys = keys;
            this.sizeBytes = sizeBytes;
            this.loaded = loaded;
            this.dirty = dirty;
        }

        public long getNodeId() {
            return nodeId;
        }

        public void setNodeId(long nodeId) {
            this.nodeId = nodeId;
        }

        public long getStoreId() {
            return storeId;
        }

        public void setStoreId(long storeId) {
            this.storeId = storeId;
        }

        public boolean isLeaf() {
            return leaf;
        }

        public void setLeaf(boolean leaf) {
            this.leaf = leaf;
        }

        public int getKeys() {
            return keys;
        }

        public void setKeys(int keys) {
            this.keys = keys;
        }

        public long getSizeBytes() {
            return sizeBytes;
        }

        public void setSizeBytes(long sizeBytes) {
            this.sizeBytes = sizeBytes;
        }

        public boolean isLoaded() {
            return loaded;
        }

        public void setLoaded(boolean loaded) {
            this.loaded = loaded;
        }

        public boolean isDirty() {
            return dirty;
        }

        public void setDirty(boolean dirty) {
            this.dirty = dirty;
        }
    }
}
