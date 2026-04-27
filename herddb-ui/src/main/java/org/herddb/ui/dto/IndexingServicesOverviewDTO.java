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
 * Wire-shape returned by {@code GET /api/v2/indexing-services}: a flat
 * list of every vector index visible from this server, plus a small
 * aggregated counter so the UI can render a "summary" header without
 * iterating the array.
 */
public final class IndexingServicesOverviewDTO {

    private int totalIndexes;
    private long totalVectorCount;
    private List<IndexingServiceIndexDTO> indexes;

    public IndexingServicesOverviewDTO() {
    }

    public int getTotalIndexes() {
        return totalIndexes;
    }

    public void setTotalIndexes(int totalIndexes) {
        this.totalIndexes = totalIndexes;
    }

    public long getTotalVectorCount() {
        return totalVectorCount;
    }

    public void setTotalVectorCount(long totalVectorCount) {
        this.totalVectorCount = totalVectorCount;
    }

    public List<IndexingServiceIndexDTO> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<IndexingServiceIndexDTO> indexes) {
        this.indexes = indexes;
    }
}
