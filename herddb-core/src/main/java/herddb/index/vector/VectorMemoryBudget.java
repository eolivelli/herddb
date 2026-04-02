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

/**
 * Global memory budget shared across all vector stores.
 * Implementations track the aggregate memory usage of all registered stores
 * and enforce a single global limit to prevent heap exhaustion.
 *
 * @author enrico.olivelli
 */
public interface VectorMemoryBudget {

    /**
     * Returns the total estimated memory usage across all vector stores, in bytes.
     */
    long totalEstimatedMemoryUsageBytes();

    /**
     * Returns the global memory limit in bytes.
     */
    long maxMemoryBytes();

    /**
     * Returns {@code true} when the total estimated memory across all stores
     * exceeds the global limit, indicating that ingestion should be throttled.
     */
    default boolean isMemoryPressureActive() {
        long max = maxMemoryBytes();
        return max != Long.MAX_VALUE && totalEstimatedMemoryUsageBytes() > max;
    }

    /**
     * Returns {@code true} when the total estimated memory across all stores
     * exceeds the given fraction of the global limit, useful for triggering
     * early checkpoints before the hard limit is reached.
     *
     * @param fraction a value between 0.0 and 1.0
     */
    default boolean isAboveThreshold(double fraction) {
        long max = maxMemoryBytes();
        return max != Long.MAX_VALUE && totalEstimatedMemoryUsageBytes() > (long) (max * fraction);
    }
}
