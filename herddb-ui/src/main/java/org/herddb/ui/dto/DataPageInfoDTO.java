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
 * {@code GET /api/v2/tablespaces/{ts}/tables/{t}/data-pages}.
 */
public final class DataPageInfoDTO {

    private long pageId;
    private long sizeBytes;
    private long averageRecordSize;
    private long dirtBytes;
    private boolean loaded;

    public DataPageInfoDTO() {
    }

    public DataPageInfoDTO(long pageId, long sizeBytes, long averageRecordSize,
                           long dirtBytes, boolean loaded) {
        this.pageId = pageId;
        this.sizeBytes = sizeBytes;
        this.averageRecordSize = averageRecordSize;
        this.dirtBytes = dirtBytes;
        this.loaded = loaded;
    }

    public long getPageId() {
        return pageId;
    }

    public void setPageId(long pageId) {
        this.pageId = pageId;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public void setSizeBytes(long sizeBytes) {
        this.sizeBytes = sizeBytes;
    }

    public long getAverageRecordSize() {
        return averageRecordSize;
    }

    public void setAverageRecordSize(long averageRecordSize) {
        this.averageRecordSize = averageRecordSize;
    }

    public long getDirtBytes() {
        return dirtBytes;
    }

    public void setDirtBytes(long dirtBytes) {
        this.dirtBytes = dirtBytes;
    }

    public boolean isLoaded() {
        return loaded;
    }

    public void setLoaded(boolean loaded) {
        this.loaded = loaded;
    }

    /**
     * Container returned at the top level of the response, with summary
     * counters that the UI can render without iterating the page list.
     */
    public static final class PageLayoutDTO {

        private int totalPages;
        private int loadedPages;
        private long totalSizeBytes;
        private long totalDirtBytes;
        private List<DataPageInfoDTO> pages;

        public PageLayoutDTO() {
        }

        public int getTotalPages() {
            return totalPages;
        }

        public void setTotalPages(int totalPages) {
            this.totalPages = totalPages;
        }

        public int getLoadedPages() {
            return loadedPages;
        }

        public void setLoadedPages(int loadedPages) {
            this.loadedPages = loadedPages;
        }

        public long getTotalSizeBytes() {
            return totalSizeBytes;
        }

        public void setTotalSizeBytes(long totalSizeBytes) {
            this.totalSizeBytes = totalSizeBytes;
        }

        public long getTotalDirtBytes() {
            return totalDirtBytes;
        }

        public void setTotalDirtBytes(long totalDirtBytes) {
            this.totalDirtBytes = totalDirtBytes;
        }

        public List<DataPageInfoDTO> getPages() {
            return pages;
        }

        public void setPages(List<DataPageInfoDTO> pages) {
            this.pages = pages;
        }
    }
}
