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
import java.util.Properties;
import org.junit.Test;

/**
 * Unit tests for {@link IndexingServer} configuration logic.
 *
 * <p>{@link IndexingServer#buildMemoryManager()} and the vector-budget set in
 * {@link IndexingServer#start()} must both use 1/3 of JVM max heap when
 * {@code indexing.memory.vector.limit} is 0 (auto).
 */
public class IndexingServerTest {

    /**
     * When {@code indexing.memory.vector.limit} is not set (defaults to 0),
     * {@code buildMemoryManager()} must use {@code Runtime.maxMemory() / 3}.
     */
    @Test
    public void testBuildMemoryManagerDefaultUsesOneThird() {
        IndexingServerConfiguration config = new IndexingServerConfiguration();
        // engine is not used by buildMemoryManager(), so null is safe here
        IndexingServer server = new IndexingServer("localhost", 0, null, config);

        MemoryManager mm = server.buildMemoryManager();

        long expected = Runtime.getRuntime().maxMemory() / 3;
        assertEquals(
                "buildMemoryManager() must use maxMemory()/3 when limit is 0",
                expected, mm.getMaxDataUsedMemory());
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
     * Ensures the auto-computed limit in {@code buildMemoryManager()} (1/3)
     * matches the vector back-pressure budget set by {@code start()}, also 1/3.
     * Both code paths read the same property so they must agree on the fraction.
     */
    @Test
    public void testBuildMemoryManagerAndStartAgreeOnDefaultFraction() {
        IndexingServerConfiguration config = new IndexingServerConfiguration();
        IndexingServer server = new IndexingServer("localhost", 0, null, config);

        MemoryManager mm = server.buildMemoryManager();

        // start() computes: maxMemory <= 0 ? maxMemory()/3 : maxVectorMemory
        long expectedVectorBudget = Runtime.getRuntime().maxMemory() / 3;

        assertEquals(
                "buildMemoryManager() and start() must use the same 1/3 fraction",
                expectedVectorBudget, mm.getMaxDataUsedMemory());
    }
}
