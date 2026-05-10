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

/**
 * Per-index source of jvector build parameters for the index optimizer (issue #516).
 *
 * <p>The optimizer calls {@link #getMergeConfig} once per merge run, using the
 * first input segment's identifiers to look up the configuration. Implementations
 * may cache results and refresh them periodically so that parameter changes
 * (e.g. a change in {@code dim} when the index is rebuilt with a different
 * embedding model) are picked up without a pod restart.
 *
 * <p>Implementations MUST be thread-safe. The optimizer's single-threaded
 * scheduler is the only caller in production, but tests may call from multiple
 * threads.
 */
public interface IndexMergeConfigProvider {

    /**
     * Returns the merge configuration for the given index.
     *
     * @param tablespace  human-readable tablespace name (e.g. {@code "herd"})
     * @param tableName   human-readable table name
     * @param indexName   human-readable index name
     * @param indexUuid   stable UUID of the index (used for caching)
     * @return the resolved configuration — never {@code null}
     * @throws Exception if the configuration cannot be resolved (e.g. the IS
     *                   is unreachable, or the index does not expose a usable
     *                   dimension). The optimizer engine catches this, logs it,
     *                   and skips the index for this tick.
     */
    IndexMergeConfig getMergeConfig(String tablespace, String tableName,
                                    String indexName, String indexUuid)
            throws Exception;
}
