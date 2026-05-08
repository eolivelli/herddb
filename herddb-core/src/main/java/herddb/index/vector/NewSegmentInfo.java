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
package herddb.index.vector;

import herddb.log.LogSequenceNumber;

/**
 * Snapshot of a freshly-emitted vector index segment, handed to a {@link SegmentPublisher}
 * by {@link PersistentVectorStore} immediately after a successful checkpoint.
 *
 * <p>For segmented-v2 indexes the publisher uses this information to register the
 * segment in the ZooKeeper segment registry. The publisher is expected to allocate
 * its own external identifier (a UUID); this snapshot carries only the data the
 * registry needs.
 */
public final class NewSegmentInfo {

    private final int segmentId;
    private final String segmentUuid;
    private final String graphFilePath;
    private final long graphFileSize;
    private final String mapFilePath;
    private final long mapFileSize;
    private final long estimatedSizeBytes;
    private final long vectorCount;
    private final long generation;
    private final LogSequenceNumber baseLsn;

    public NewSegmentInfo(int segmentId, String segmentUuid,
                          String graphFilePath, long graphFileSize,
                          String mapFilePath, long mapFileSize,
                          long estimatedSizeBytes, long vectorCount, long generation,
                          LogSequenceNumber baseLsn) {
        this.segmentId = segmentId;
        this.segmentUuid = segmentUuid;
        this.graphFilePath = graphFilePath;
        this.graphFileSize = graphFileSize;
        this.mapFilePath = mapFilePath;
        this.mapFileSize = mapFileSize;
        this.estimatedSizeBytes = estimatedSizeBytes;
        this.vectorCount = vectorCount;
        this.generation = generation;
        this.baseLsn = baseLsn;
    }

    public int getSegmentId() {
        return segmentId;
    }

    /**
     * Stable UUID stamped onto this segment by {@link PersistentVectorStore} during
     * Phase B; persisted in v4 IndexStatus so the same UUID is reused on restart.
     * Never null when the publisher is invoked.
     */
    public String getSegmentUuid() {
        return segmentUuid;
    }

    public String getGraphFilePath() {
        return graphFilePath;
    }

    public long getGraphFileSize() {
        return graphFileSize;
    }

    public String getMapFilePath() {
        return mapFilePath;
    }

    public long getMapFileSize() {
        return mapFileSize;
    }

    public long getEstimatedSizeBytes() {
        return estimatedSizeBytes;
    }

    public long getVectorCount() {
        return vectorCount;
    }

    public long getGeneration() {
        return generation;
    }

    public LogSequenceNumber getBaseLsn() {
        return baseLsn;
    }

    @Override
    public String toString() {
        return "NewSegmentInfo{segmentId=" + segmentId
                + ", segmentUuid=" + segmentUuid
                + ", graphPath=" + graphFilePath
                + ", graphSize=" + graphFileSize
                + ", mapPath=" + mapFilePath
                + ", mapSize=" + mapFileSize
                + ", estimatedBytes=" + estimatedSizeBytes
                + ", vectorCount=" + vectorCount
                + ", generation=" + generation
                + ", baseLsn=" + baseLsn
                + '}';
    }
}
