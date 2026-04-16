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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.Properties;
import org.junit.Test;

/**
 * Tests for {@link IndexingServerConfiguration}.
 *
 * @author enrico.olivelli
 */
public class IndexingServerConfigurationTest {

    @Test
    public void testDefaults() {
        IndexingServerConfiguration config = new IndexingServerConfiguration();

        assertEquals("0.0.0.0", config.getString(
                IndexingServerConfiguration.PROPERTY_GRPC_HOST,
                IndexingServerConfiguration.PROPERTY_GRPC_HOST_DEFAULT));
        assertEquals(9850, config.getInt(
                IndexingServerConfiguration.PROPERTY_GRPC_PORT,
                IndexingServerConfiguration.PROPERTY_GRPC_PORT_DEFAULT));
        assertFalse(config.getBoolean(
                IndexingServerConfiguration.PROPERTY_HTTP_ENABLE,
                IndexingServerConfiguration.PROPERTY_HTTP_ENABLE_DEFAULT));
        assertEquals("0.0.0.0", config.getString(
                IndexingServerConfiguration.PROPERTY_HTTP_HOST,
                IndexingServerConfiguration.PROPERTY_HTTP_HOST_DEFAULT));
        assertEquals(9851, config.getInt(
                IndexingServerConfiguration.PROPERTY_HTTP_PORT,
                IndexingServerConfiguration.PROPERTY_HTTP_PORT_DEFAULT));
        assertEquals("txlog", config.getString(
                IndexingServerConfiguration.PROPERTY_LOG_DIR,
                IndexingServerConfiguration.PROPERTY_LOG_DIR_DEFAULT));
        assertEquals("data", config.getString(
                IndexingServerConfiguration.PROPERTY_DATA_DIR,
                IndexingServerConfiguration.PROPERTY_DATA_DIR_DEFAULT));
        assertEquals(0L, config.getLong(
                IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_LIMIT,
                IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_LIMIT_DEFAULT));
        assertEquals(1048576L, config.getLong(
                IndexingServerConfiguration.PROPERTY_MEMORY_PAGE_SIZE,
                IndexingServerConfiguration.PROPERTY_MEMORY_PAGE_SIZE_DEFAULT));
        assertEquals(16, config.getInt(
                IndexingServerConfiguration.PROPERTY_VECTOR_M,
                IndexingServerConfiguration.PROPERTY_VECTOR_M_DEFAULT));
        assertEquals(100, config.getInt(
                IndexingServerConfiguration.PROPERTY_VECTOR_BEAM_WIDTH,
                IndexingServerConfiguration.PROPERTY_VECTOR_BEAM_WIDTH_DEFAULT));
        assertEquals(1.2, config.getDouble(
                IndexingServerConfiguration.PROPERTY_VECTOR_NEIGHBOR_OVERFLOW,
                IndexingServerConfiguration.PROPERTY_VECTOR_NEIGHBOR_OVERFLOW_DEFAULT), 0.001);
        assertEquals(1.4, config.getDouble(
                IndexingServerConfiguration.PROPERTY_VECTOR_ALPHA,
                IndexingServerConfiguration.PROPERTY_VECTOR_ALPHA_DEFAULT), 0.001);
        assertTrue(config.getBoolean(
                IndexingServerConfiguration.PROPERTY_VECTOR_FUSED_PQ,
                IndexingServerConfiguration.PROPERTY_VECTOR_FUSED_PQ_DEFAULT));
        assertEquals(2147483648L, config.getLong(
                IndexingServerConfiguration.PROPERTY_VECTOR_MAX_SEGMENT_SIZE,
                IndexingServerConfiguration.PROPERTY_VECTOR_MAX_SEGMENT_SIZE_DEFAULT));
        assertEquals(0, config.getInt(
                IndexingServerConfiguration.PROPERTY_VECTOR_MAX_LIVE_GRAPH_SIZE,
                IndexingServerConfiguration.PROPERTY_VECTOR_MAX_LIVE_GRAPH_SIZE_DEFAULT));
        assertEquals(60000L, config.getLong(
                IndexingServerConfiguration.PROPERTY_COMPACTION_INTERVAL,
                IndexingServerConfiguration.PROPERTY_COMPACTION_INTERVAL_DEFAULT));
        assertEquals(2, config.getInt(
                IndexingServerConfiguration.PROPERTY_COMPACTION_THREADS,
                IndexingServerConfiguration.PROPERTY_COMPACTION_THREADS_DEFAULT));
        assertEquals(100_000L, config.getLong(
                IndexingServerConfiguration.PROPERTY_WATERMARK_CHECKPOINT_INTERVAL_ENTRIES,
                IndexingServerConfiguration.PROPERTY_WATERMARK_CHECKPOINT_INTERVAL_ENTRIES_DEFAULT));
        assertEquals("file", config.getString(
                IndexingServerConfiguration.PROPERTY_STORAGE_TYPE,
                IndexingServerConfiguration.PROPERTY_STORAGE_TYPE_DEFAULT));
        assertEquals(1_800_000L, config.getLong(
                IndexingServerConfiguration.PROPERTY_REMOTE_FILE_BOOTSTRAP_WAIT_MS,
                IndexingServerConfiguration.PROPERTY_REMOTE_FILE_BOOTSTRAP_WAIT_MS_DEFAULT));
        assertEquals(1_800_000L, config.getLong(
                IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_TIMEOUT_MS,
                IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_TIMEOUT_MS_DEFAULT));
    }

    @Test
    public void testWatermarkCheckpointIntervalOverride() {
        IndexingServerConfiguration config = new IndexingServerConfiguration();
        config.set(IndexingServerConfiguration.PROPERTY_WATERMARK_CHECKPOINT_INTERVAL_ENTRIES, 250_000L);
        assertEquals(250_000L, config.getLong(
                IndexingServerConfiguration.PROPERTY_WATERMARK_CHECKPOINT_INTERVAL_ENTRIES,
                IndexingServerConfiguration.PROPERTY_WATERMARK_CHECKPOINT_INTERVAL_ENTRIES_DEFAULT));
    }

    @Test
    public void testBooleanGetterSetter() {
        IndexingServerConfiguration config = new IndexingServerConfiguration();
        config.set(IndexingServerConfiguration.PROPERTY_HTTP_ENABLE, true);
        assertTrue(config.getBoolean(IndexingServerConfiguration.PROPERTY_HTTP_ENABLE, false));

        config.set(IndexingServerConfiguration.PROPERTY_HTTP_ENABLE, false);
        assertFalse(config.getBoolean(IndexingServerConfiguration.PROPERTY_HTTP_ENABLE, true));
    }

    @Test
    public void testIntGetterSetter() {
        IndexingServerConfiguration config = new IndexingServerConfiguration();
        config.set(IndexingServerConfiguration.PROPERTY_GRPC_PORT, 12345);
        assertEquals(12345, config.getInt(IndexingServerConfiguration.PROPERTY_GRPC_PORT, 0));
    }

    @Test
    public void testLongGetterSetter() {
        IndexingServerConfiguration config = new IndexingServerConfiguration();
        config.set(IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_LIMIT, 999999999L);
        assertEquals(999999999L, config.getLong(IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_LIMIT, 0L));
    }

    @Test
    public void testDoubleGetterSetter() {
        IndexingServerConfiguration config = new IndexingServerConfiguration();
        config.set(IndexingServerConfiguration.PROPERTY_VECTOR_ALPHA, 2.5);
        assertEquals(2.5, config.getDouble(IndexingServerConfiguration.PROPERTY_VECTOR_ALPHA, 0.0), 0.001);
    }

    @Test
    public void testStringGetterSetter() {
        IndexingServerConfiguration config = new IndexingServerConfiguration();
        config.set(IndexingServerConfiguration.PROPERTY_GRPC_HOST, "127.0.0.1");
        assertEquals("127.0.0.1", config.getString(IndexingServerConfiguration.PROPERTY_GRPC_HOST, ""));
    }

    @Test
    public void testCopyIsIndependent() {
        IndexingServerConfiguration original = new IndexingServerConfiguration();
        original.set(IndexingServerConfiguration.PROPERTY_GRPC_PORT, 7777);

        IndexingServerConfiguration copy = original.copy();
        assertEquals(7777, copy.getInt(IndexingServerConfiguration.PROPERTY_GRPC_PORT, 0));

        // Modify the copy, original should be unaffected
        copy.set(IndexingServerConfiguration.PROPERTY_GRPC_PORT, 8888);
        assertEquals(7777, original.getInt(IndexingServerConfiguration.PROPERTY_GRPC_PORT, 0));
        assertEquals(8888, copy.getInt(IndexingServerConfiguration.PROPERTY_GRPC_PORT, 0));

        // Modify the original, copy should be unaffected
        original.set(IndexingServerConfiguration.PROPERTY_GRPC_HOST, "1.2.3.4");
        assertEquals("0.0.0.0", copy.getString(IndexingServerConfiguration.PROPERTY_GRPC_HOST,
                IndexingServerConfiguration.PROPERTY_GRPC_HOST_DEFAULT));
    }

    @Test
    public void testConstructionFromProperties() {
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_GRPC_PORT, "5555");
        props.setProperty(IndexingServerConfiguration.PROPERTY_HTTP_ENABLE, "true");
        props.setProperty(IndexingServerConfiguration.PROPERTY_VECTOR_ALPHA, "3.14");

        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        assertEquals(5555, config.getInt(IndexingServerConfiguration.PROPERTY_GRPC_PORT, 0));
        assertTrue(config.getBoolean(IndexingServerConfiguration.PROPERTY_HTTP_ENABLE, false));
        assertEquals(3.14, config.getDouble(IndexingServerConfiguration.PROPERTY_VECTOR_ALPHA, 0.0), 0.001);

        // Changing the original Properties should not affect the config
        props.setProperty(IndexingServerConfiguration.PROPERTY_GRPC_PORT, "6666");
        assertEquals(5555, config.getInt(IndexingServerConfiguration.PROPERTY_GRPC_PORT, 0));
    }

    @Test
    public void testFluentSet() {
        IndexingServerConfiguration config = new IndexingServerConfiguration()
                .set(IndexingServerConfiguration.PROPERTY_GRPC_HOST, "10.0.0.1")
                .set(IndexingServerConfiguration.PROPERTY_GRPC_PORT, 1234)
                .set(IndexingServerConfiguration.PROPERTY_HTTP_ENABLE, true);

        assertEquals("10.0.0.1", config.getString(IndexingServerConfiguration.PROPERTY_GRPC_HOST, ""));
        assertEquals(1234, config.getInt(IndexingServerConfiguration.PROPERTY_GRPC_PORT, 0));
        assertTrue(config.getBoolean(IndexingServerConfiguration.PROPERTY_HTTP_ENABLE, false));
    }

    @Test
    public void testSetNullRemovesProperty() {
        IndexingServerConfiguration config = new IndexingServerConfiguration();
        config.set(IndexingServerConfiguration.PROPERTY_GRPC_PORT, 1111);
        assertEquals(1111, config.getInt(IndexingServerConfiguration.PROPERTY_GRPC_PORT, 9999));

        config.set(IndexingServerConfiguration.PROPERTY_GRPC_PORT, null);
        // Should fall back to default since property was removed
        assertEquals(9999, config.getInt(IndexingServerConfiguration.PROPERTY_GRPC_PORT, 9999));
    }
}
