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
package herddb.indexing.optimizer;

import java.util.Objects;

/**
 * {@link IndexMergeConfigProvider} that returns the same fixed configuration
 * for every index (issue #516).
 *
 * <p>This implementation is intended exclusively for unit tests that do not
 * want to stand up a live remote file server or a real HerdDB cluster. Production
 * code uses {@link RemoteMetadataIndexMergeConfigProvider}, which reads the
 * per-index build parameters directly from the checkpoint metadata stored on the
 * remote file server.
 *
 * <p>Because the config is global, this provider is unsuitable for clusters with
 * indexes of different graph-build parameters — but for test purposes where all
 * indexes share the same synthetic config this is fine.
 */
public final class StaticIndexMergeConfigProvider implements IndexMergeConfigProvider {

    private final IndexMergeConfig config;

    /**
     * Constructor for tests that supply the config directly.
     *
     * @param config the fixed merge config returned on every call; must not be null
     */
    public StaticIndexMergeConfigProvider(IndexMergeConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public IndexMergeConfig getMergeConfig(String tablespace, String tableName,
                                           String indexName, String indexUuid) {
        return config;
    }
}
