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
import static org.junit.Assert.assertTrue;
import java.util.Properties;
import org.junit.Test;

/**
 * Issue #638: regression test for the new direct-S3 upload config keys
 * added to {@link IndexingServerConfiguration}. Pins both the key names
 * (operators set these in {@code values.yaml} so they must not silently
 * change) and the documented default values.
 */
public class IndexingServerConfigurationDirectWriteTest {

    /**
     * The flag key and default are documented in {@code IndexingServerConfiguration}
     * and referenced in the Helm chart. Default is {@code true} so existing
     * deployments that already have direct-read enabled get direct-write for
     * free on upgrade — that is the rollout shape the issue called for.
     */
    @Test
    public void directWriteEnabledKeyAndDefault() {
        assertEquals("indexing.s3.direct.write.enabled",
                IndexingServerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED);
        assertTrue("default must be true",
                IndexingServerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED_DEFAULT);
    }

    /**
     * Inflight cap key + default. The 512 MiB default mirrors the
     * workaround value applied in {@code values.yaml} during the issue
     * investigation.
     */
    @Test
    public void inflightDirectWriteBytesKeyAndDefault() {
        assertEquals("indexing.remote.file.client.max.inflight.direct.write.bytes",
                IndexingServerConfiguration
                        .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES);
        assertEquals(512L * 1024 * 1024,
                IndexingServerConfiguration
                        .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES_DEFAULT);
    }

    /**
     * Values set in a {@link Properties} reach the configuration correctly
     * via the public {@code getBoolean} / {@code getLong} accessors. This
     * guards against typo regressions where the field name and the actual
     * key consulted at runtime drift apart.
     */
    @Test
    public void valuesAreRoundTrippedViaConfiguration() {
        Properties p = new Properties();
        p.setProperty(IndexingServerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED, "false");
        p.setProperty(
                IndexingServerConfiguration
                        .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES,
                "104857600"); // 100 MiB
        IndexingServerConfiguration cfg = new IndexingServerConfiguration(p);
        assertEquals(false, cfg.getBoolean(
                IndexingServerConfiguration.PROPERTY_S3_DIRECT_WRITE_ENABLED, true));
        assertEquals(104857600L, cfg.getLong(
                IndexingServerConfiguration
                        .PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_DIRECT_WRITE_BYTES,
                -1L));
    }
}
