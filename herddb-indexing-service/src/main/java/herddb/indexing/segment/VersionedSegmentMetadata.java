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
package herddb.indexing.segment;

import java.util.Objects;

/**
 * A {@link SegmentMetadata} paired with the ZooKeeper version of the znode it
 * was read from. Used by callers to perform CAS updates via
 * {@link SegmentRegistryClient#casUpdateSegment(VersionedSegmentMetadata, SegmentMetadata)}.
 */
public final class VersionedSegmentMetadata {

    private final SegmentMetadata metadata;
    private final int zkVersion;

    public VersionedSegmentMetadata(SegmentMetadata metadata, int zkVersion) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.zkVersion = zkVersion;
    }

    public SegmentMetadata metadata() {
        return metadata;
    }

    public int zkVersion() {
        return zkVersion;
    }

    @Override
    public String toString() {
        return "VersionedSegmentMetadata{zkVersion=" + zkVersion + ", metadata=" + metadata + '}';
    }
}
