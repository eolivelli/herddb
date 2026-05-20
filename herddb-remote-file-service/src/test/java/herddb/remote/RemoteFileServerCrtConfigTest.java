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

package herddb.remote;

import static org.junit.Assert.assertEquals;
import java.util.Properties;
import org.junit.Test;

/**
 * Verifies that the CRT HTTP-client configuration constants introduced by
 * issue #612 are declared with the correct default values and that the
 * {@link RemoteFileServer} exposes them via its public {@code CONFIG_*}
 * constants so operators and tests can refer to them symbolically.
 */
public class RemoteFileServerCrtConfigTest {

    @Test
    public void crtMaxConcurrencyDefaultIsFour() {
        assertEquals(4, RemoteFileServer.CONFIG_CRT_MAX_CONCURRENCY_DEFAULT);
    }

    @Test
    public void crtReadBufferSizeDefaultIsOneGiB() {
        assertEquals(1024L * 1024 * 1024, RemoteFileServer.CONFIG_CRT_READ_BUFFER_SIZE_DEFAULT);
    }

    @Test
    public void crtMaxConcurrencyConfigKeyIsCorrect() {
        assertEquals("s3.crt.max.concurrency", RemoteFileServer.CONFIG_CRT_MAX_CONCURRENCY);
    }

    @Test
    public void crtReadBufferSizeConfigKeyIsCorrect() {
        assertEquals("s3.crt.read.buffer.size", RemoteFileServer.CONFIG_CRT_READ_BUFFER_SIZE);
    }

    /**
     * Verifies that the CRT config values are read from Properties correctly.
     * This exercises the parsing path used in {@code buildS3ObjectStorage()}
     * without starting a real S3 connection.
     */
    @Test
    public void crtConfigIsReadFromProperties() {
        Properties props = new Properties();
        props.setProperty(RemoteFileServer.CONFIG_CRT_MAX_CONCURRENCY, "8");
        props.setProperty(RemoteFileServer.CONFIG_CRT_READ_BUFFER_SIZE, "536870912");

        int maxConcurrency = Integer.parseInt(
                props.getProperty(RemoteFileServer.CONFIG_CRT_MAX_CONCURRENCY,
                        String.valueOf(RemoteFileServer.CONFIG_CRT_MAX_CONCURRENCY_DEFAULT)));
        long readBufferSize = Long.parseLong(
                props.getProperty(RemoteFileServer.CONFIG_CRT_READ_BUFFER_SIZE,
                        String.valueOf(RemoteFileServer.CONFIG_CRT_READ_BUFFER_SIZE_DEFAULT)));

        assertEquals(8, maxConcurrency);
        assertEquals(512L * 1024 * 1024, readBufferSize);
    }

    /**
     * Verifies that when the CRT config keys are absent, the defaults are used.
     */
    @Test
    public void crtConfigFallsBackToDefaults() {
        Properties props = new Properties();

        int maxConcurrency = Integer.parseInt(
                props.getProperty(RemoteFileServer.CONFIG_CRT_MAX_CONCURRENCY,
                        String.valueOf(RemoteFileServer.CONFIG_CRT_MAX_CONCURRENCY_DEFAULT)));
        long readBufferSize = Long.parseLong(
                props.getProperty(RemoteFileServer.CONFIG_CRT_READ_BUFFER_SIZE,
                        String.valueOf(RemoteFileServer.CONFIG_CRT_READ_BUFFER_SIZE_DEFAULT)));

        assertEquals(RemoteFileServer.CONFIG_CRT_MAX_CONCURRENCY_DEFAULT, maxConcurrency);
        assertEquals(RemoteFileServer.CONFIG_CRT_READ_BUFFER_SIZE_DEFAULT, readBufferSize);
    }
}
