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
 * Wire-shape returned by
 * {@code GET /api/v2/tablespaces/{ts}/tables/{t}/indexes/{idx}}.
 *
 * <p>For BRIN indexes the {@code blocks} list is populated; for other
 * index types it is left empty (the {@link IndexSummaryDTO summary} is
 * always present).
 *
 * <p>The {@code statusProperties} field carries the raw JSON string from the
 * {@code sysindexstatus.properties} column (e.g. {@code {"numBlocks":3}} for
 * BRIN indexes). It is {@code null} when no status row was found.
 */
public final class IndexDetailDTO {

    private IndexSummaryDTO summary;
    private List<BrinBlockDTO> blocks;
    private String statusProperties;

    public IndexDetailDTO() {
    }

    public IndexSummaryDTO getSummary() {
        return summary;
    }

    public void setSummary(IndexSummaryDTO summary) {
        this.summary = summary;
    }

    public List<BrinBlockDTO> getBlocks() {
        return blocks;
    }

    public void setBlocks(List<BrinBlockDTO> blocks) {
        this.blocks = blocks;
    }

    public String getStatusProperties() {
        return statusProperties;
    }

    public void setStatusProperties(String statusProperties) {
        this.statusProperties = statusProperties;
    }

    /**
     * Wire-shape for a single BRIN block.
     */
    public static final class BrinBlockDTO {

        private long blockId;
        private long pageId;
        private long entries;
        private boolean loaded;
        private boolean dirty;

        public BrinBlockDTO() {
        }

        public BrinBlockDTO(long blockId, long pageId, long entries, boolean loaded, boolean dirty) {
            this.blockId = blockId;
            this.pageId = pageId;
            this.entries = entries;
            this.loaded = loaded;
            this.dirty = dirty;
        }

        public long getBlockId() {
            return blockId;
        }

        public void setBlockId(long blockId) {
            this.blockId = blockId;
        }

        public long getPageId() {
            return pageId;
        }

        public void setPageId(long pageId) {
            this.pageId = pageId;
        }

        public long getEntries() {
            return entries;
        }

        public void setEntries(long entries) {
            this.entries = entries;
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
