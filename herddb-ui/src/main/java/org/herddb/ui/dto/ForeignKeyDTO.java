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
 * Wire-shape for a foreign-key constraint, derived from
 * {@code sysforeignkeys} (one row per FK, with the per-column links
 * inlined in declaration order).
 */
public final class ForeignKeyDTO {

    private String name;
    private String parentTable;
    private List<String> childColumns;
    private List<String> parentColumns;
    private String onDeleteAction;
    private String onUpdateAction;

    public ForeignKeyDTO() {
        // Default constructor for Jackson deserialisation in tests.
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getParentTable() {
        return parentTable;
    }

    public void setParentTable(String parentTable) {
        this.parentTable = parentTable;
    }

    public List<String> getChildColumns() {
        return childColumns;
    }

    public void setChildColumns(List<String> childColumns) {
        this.childColumns = childColumns;
    }

    public List<String> getParentColumns() {
        return parentColumns;
    }

    public void setParentColumns(List<String> parentColumns) {
        this.parentColumns = parentColumns;
    }

    public String getOnDeleteAction() {
        return onDeleteAction;
    }

    public void setOnDeleteAction(String onDeleteAction) {
        this.onDeleteAction = onDeleteAction;
    }

    public String getOnUpdateAction() {
        return onUpdateAction;
    }

    public void setOnUpdateAction(String onUpdateAction) {
        this.onUpdateAction = onUpdateAction;
    }
}
