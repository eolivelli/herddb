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

package herddb.server;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import herddb.storage.DataStorageManagerException;
import herddb.utils.HerdDBByteBufAllocators;
import java.lang.management.ManagementFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that the data/index/PK memory-budget defaults are derived from the
 * configured reference source: by default the JVM direct-memory limit (returned
 * by {@link HerdDBByteBufAllocators#maxDirectMemoryBytes()}); legacy heap-based
 * derivation is still available via the {@code server.memory.max.limit.source}
 * escape hatch.
 */
public class ServerConfigurationDirectMemoryBudgetTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void defaultSourceIsDirectMemory() throws Exception {
        try (Server server = new Server(newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            long expectedReference = HerdDBByteBufAllocators.maxDirectMemoryBytes();
            assertEquals("default reference must be the JVM direct-memory limit",
                    expectedReference, server.getManager().getMaxMemoryReference());

            long expectedData = (long) (ServerConfiguration.PROPERTY_MAX_DATA_MEMORY_PERCENTAGE_DEFAULT * expectedReference);
            long expectedPk = (long) (ServerConfiguration.PROPERTY_MAX_PK_MEMORY_PERCENTAGE_DEFAULT * expectedReference);
            assertEquals(expectedData, server.getManager().getMaxDataUsedMemory());
            assertEquals(expectedPk, server.getManager().getMaxPKUsedMemory());
        }
    }

    @Test
    public void heapSourceRestoresLegacyBehaviour() throws Exception {
        try (Server server = new Server(newServerConfigurationWithAutoPort(folder.newFolder().toPath())
                .set(ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE_SOURCE,
                        ServerConfiguration.MEMORY_LIMIT_REFERENCE_SOURCE_HEAP))) {
            server.start();
            long maxHeap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
            assertEquals("source=heap must derive from the JVM max heap",
                    maxHeap, server.getManager().getMaxMemoryReference());
        }
    }

    @Test
    public void explicitMemoryLimitReferenceWinsOverSource() throws Exception {
        long explicitLimit = 64L * 1024L * 1024L; // small enough to be ≤ both heap and direct on every test JVM
        try (Server server = new Server(newServerConfigurationWithAutoPort(folder.newFolder().toPath())
                .set(ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE, explicitLimit))) {
            server.start();
            assertEquals("explicit server.memory.max.limit must take precedence",
                    explicitLimit, server.getManager().getMaxMemoryReference());
            // Percentage-derived budgets follow the explicit reference.
            long expectedData = (long) (ServerConfiguration.PROPERTY_MAX_DATA_MEMORY_PERCENTAGE_DEFAULT * explicitLimit);
            assertEquals(expectedData, server.getManager().getMaxDataUsedMemory());
        }
    }

    @Test
    public void directMemoryReferenceIsPositive() {
        long ref = HerdDBByteBufAllocators.maxDirectMemoryBytes();
        assertTrue("direct-memory reference must be positive on a normal JVM, got " + ref, ref > 0L);
    }

    @Test
    public void invalidSourceIsRejected() throws Exception {
        try (Server server = new Server(newServerConfigurationWithAutoPort(folder.newFolder().toPath())
                .set(ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE_SOURCE, "totally-bogus"))) {
            DataStorageManagerException ex = assertThrows(DataStorageManagerException.class, server::start);
            String message = ex.getMessage();
            assertTrue("error message should mention the invalid value (got: " + message + ")",
                    message != null && message.contains("totally-bogus"));
        }
    }
}
