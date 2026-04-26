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

package herddb.core;

/**
 * Read-only snapshot of one active data page produced by
 * {@link TableManager#snapshotPagesLayout()}. Carries enough metadata to
 * render a "data pages" heatmap in the Web UI without exposing internal
 * mutable structures.
 */
public final class DataPageLayoutInfo {

    private final long pageId;
    private final long sizeBytes;
    private final long averageRecordSize;
    private final long dirtBytes;
    private final boolean loaded;

    public DataPageLayoutInfo(
            long pageId,
            long sizeBytes,
            long averageRecordSize,
            long dirtBytes,
            boolean loaded) {
        this.pageId = pageId;
        this.sizeBytes = sizeBytes;
        this.averageRecordSize = averageRecordSize;
        this.dirtBytes = dirtBytes;
        this.loaded = loaded;
    }

    public long getPageId() {
        return pageId;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public long getAverageRecordSize() {
        return averageRecordSize;
    }

    public long getDirtBytes() {
        return dirtBytes;
    }

    public boolean isLoaded() {
        return loaded;
    }
}
