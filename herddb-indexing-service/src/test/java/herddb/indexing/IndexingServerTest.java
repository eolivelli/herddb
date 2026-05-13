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

package herddb.indexing;

import static org.junit.Assert.assertEquals;
import herddb.core.MemoryManager;
import herddb.utils.HerdDBByteBufAllocators;
import java.util.Properties;
import org.junit.Test;

/**
 * Unit tests for {@link IndexingServer} configuration logic.
 *
 * <p>{@link IndexingServer#buildMemoryManager()} and the vector-budget set in
 * {@link IndexingServer#start()} must both use 1/3 of Netty direct memory when
 * {@code indexing.memory.vector.limit} is 0 (auto). Both paths share
 * {@link IndexingServer#resolveEffectiveVectorMemoryLimit()} so they cannot drift.
 */
public class IndexingServerTest {

    /**
     * When {@code indexing.memory.vector.limit} is not set (defaults to 0),
     * {@code buildMemoryManager()} must use
     * {@code HerdDBByteBufAllocators.maxDirectMemoryBytes() * 0.33}.
     */
    @Test
    public void testBuildMemoryManagerDefaultUsesOneThird() {
        try {
            IndexingServerConfiguration config = new IndexingServerConfiguration();
            // engine is not used by buildMemoryManager(), so null is safe here
            IndexingServer server = new IndexingServer("localhost", 0, null, config);

            MemoryManager mm = server.buildMemoryManager();

            long expected = (long) (IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_PERCENTAGE_DEFAULT
                    * HerdDBByteBufAllocators.maxDirectMemoryBytes());
            assertEquals(
                    "buildMemoryManager() must use maxDirectMemoryBytes()*0.33 when limit is 0",
                    expected, mm.getMaxDataUsedMemory());
        } finally {
            HerdDBByteBufAllocators.resetMaxDirectMemoryCacheForTesting();
        }
    }

    /**
     * When {@code indexing.memory.vector.limit} is set explicitly,
     * {@code buildMemoryManager()} must use that exact value.
     */
    @Test
    public void testBuildMemoryManagerExplicitLimit() {
        long explicitLimit = 2L * 1024 * 1024 * 1024; // 2 GB
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_LIMIT,
                String.valueOf(explicitLimit));
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);
        IndexingServer server = new IndexingServer("localhost", 0, null, config);

        MemoryManager mm = server.buildMemoryManager();

        assertEquals(
                "buildMemoryManager() must use the explicit limit when configured",
                explicitLimit, mm.getMaxDataUsedMemory());
    }

    /**
     * Ensures the auto-computed limit in {@code buildMemoryManager()} (default 33%)
     * matches the vector back-pressure budget set by {@code start()}, also resolved via
     * {@code resolveEffectiveVectorMemoryLimit()}. Both paths share the same helper,
     * so they must produce the same value.
     */
    @Test
    public void testBuildMemoryManagerAndStartAgreeOnDefaultFraction() {
        try {
            IndexingServerConfiguration config = new IndexingServerConfiguration();
            IndexingServer server = new IndexingServer("localhost", 0, null, config);

            MemoryManager mm = server.buildMemoryManager();

            // resolveEffectiveVectorMemoryLimit() uses maxDirectMemoryBytes() * percentage
            long expectedVectorBudget = (long) (IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_PERCENTAGE_DEFAULT
                    * HerdDBByteBufAllocators.maxDirectMemoryBytes());

            assertEquals(
                    "buildMemoryManager() and start() must use the same fraction of direct memory",
                    expectedVectorBudget, mm.getMaxDataUsedMemory());
        } finally {
            HerdDBByteBufAllocators.resetMaxDirectMemoryCacheForTesting();
        }
    }

    /**
     * When {@code indexing.memory.vector.percentage=0.5} is configured and no explicit
     * limit is set, {@code buildMemoryManager()} must use 50% of
     * {@code HerdDBByteBufAllocators.maxDirectMemoryBytes()}.
     */
    @Test
    public void testBuildMemoryManagerUsesConfiguredPercentage() {
        try {
            Properties props = new Properties();
            props.setProperty(IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_PERCENTAGE, "0.5");
            IndexingServerConfiguration config = new IndexingServerConfiguration(props);
            IndexingServer server = new IndexingServer("localhost", 0, null, config);

            MemoryManager mm = server.buildMemoryManager();

            long expected = (long) (0.5d * HerdDBByteBufAllocators.maxDirectMemoryBytes());
            assertEquals(
                    "buildMemoryManager() must use 50% of maxDirectMemoryBytes() when percentage=0.5",
                    expected, mm.getMaxDataUsedMemory());
        } finally {
            HerdDBByteBufAllocators.resetMaxDirectMemoryCacheForTesting();
        }
    }

    /**
     * When both {@code indexing.memory.vector.limit} and
     * {@code indexing.memory.vector.percentage} are configured, the explicit absolute
     * limit must win regardless of what the percentage would compute to.
     */
    @Test
    public void testBuildMemoryManagerExplicitLimitOverridesPercentage() {
        long explicitLimit = 1_234_567L;
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_LIMIT,
                String.valueOf(explicitLimit));
        // A nonsense percentage that would produce a very different number if used
        props.setProperty(IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_PERCENTAGE, "0.99");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);
        IndexingServer server = new IndexingServer("localhost", 0, null, config);

        MemoryManager mm = server.buildMemoryManager();

        assertEquals(
                "explicit limit must override percentage when limit > 0",
                explicitLimit, mm.getMaxDataUsedMemory());
    }
}
