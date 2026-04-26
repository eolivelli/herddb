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
 * Compact wire-shape used by {@code GET /api/v2/tablespaces/{ts}/tables}:
 * one row per table including the {@code systables} identity columns and
 * the most useful {@code systablestats} counters. Larger structural
 * details (columns, indexes, foreign keys) are returned by the per-table
 * detail endpoint.
 */
public final class TableSummaryDTO {

    private String tablespace;
    private String name;
    private String uuid;
    private boolean systemTable;
    private long tableSize;
    private int loadedPages;
    private long loadedPagesCount;
    private long unloadedPagesCount;
    private int dirtyPages;
    private long dirtyRecords;
    private long maxLogicalPageSize;
    private long keysMemory;
    private long buffersMemory;
    private long dirtyMemory;

    public TableSummaryDTO() {
        // Default constructor for Jackson deserialisation in tests.
    }

    public String getTablespace() {
        return tablespace;
    }

    public void setTablespace(String tablespace) {
        this.tablespace = tablespace;
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

    public boolean isSystemTable() {
        return systemTable;
    }

    public void setSystemTable(boolean systemTable) {
        this.systemTable = systemTable;
    }

    public long getTableSize() {
        return tableSize;
    }

    public void setTableSize(long tableSize) {
        this.tableSize = tableSize;
    }

    public int getLoadedPages() {
        return loadedPages;
    }

    public void setLoadedPages(int loadedPages) {
        this.loadedPages = loadedPages;
    }

    public long getLoadedPagesCount() {
        return loadedPagesCount;
    }

    public void setLoadedPagesCount(long loadedPagesCount) {
        this.loadedPagesCount = loadedPagesCount;
    }

    public long getUnloadedPagesCount() {
        return unloadedPagesCount;
    }

    public void setUnloadedPagesCount(long unloadedPagesCount) {
        this.unloadedPagesCount = unloadedPagesCount;
    }

    public int getDirtyPages() {
        return dirtyPages;
    }

    public void setDirtyPages(int dirtyPages) {
        this.dirtyPages = dirtyPages;
    }

    public long getDirtyRecords() {
        return dirtyRecords;
    }

    public void setDirtyRecords(long dirtyRecords) {
        this.dirtyRecords = dirtyRecords;
    }

    public long getMaxLogicalPageSize() {
        return maxLogicalPageSize;
    }

    public void setMaxLogicalPageSize(long maxLogicalPageSize) {
        this.maxLogicalPageSize = maxLogicalPageSize;
    }

    public long getKeysMemory() {
        return keysMemory;
    }

    public void setKeysMemory(long keysMemory) {
        this.keysMemory = keysMemory;
    }

    public long getBuffersMemory() {
        return buffersMemory;
    }

    public void setBuffersMemory(long buffersMemory) {
        this.buffersMemory = buffersMemory;
    }

    public long getDirtyMemory() {
        return dirtyMemory;
    }

    public void setDirtyMemory(long dirtyMemory) {
        this.dirtyMemory = dirtyMemory;
    }
}
