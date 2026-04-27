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
 * {@code GET /api/v2/tablespaces/{ts}/tables/{name}}: the table summary
 * plus the column list, the secondary indexes, and the FK constraints.
 */
public final class TableDetailDTO {

    private TableSummaryDTO summary;
    private List<ColumnDTO> columns;
    private List<IndexSummaryDTO> indexes;
    private List<ForeignKeyDTO> foreignKeys;

    public TableDetailDTO() {
        // Default constructor for Jackson deserialisation in tests.
    }

    public TableSummaryDTO getSummary() {
        return summary;
    }

    public void setSummary(TableSummaryDTO summary) {
        this.summary = summary;
    }

    public List<ColumnDTO> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnDTO> columns) {
        this.columns = columns;
    }

    public List<IndexSummaryDTO> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<IndexSummaryDTO> indexes) {
        this.indexes = indexes;
    }

    public List<ForeignKeyDTO> getForeignKeys() {
        return foreignKeys;
    }

    public void setForeignKeys(List<ForeignKeyDTO> foreignKeys) {
        this.foreignKeys = foreignKeys;
    }
}
