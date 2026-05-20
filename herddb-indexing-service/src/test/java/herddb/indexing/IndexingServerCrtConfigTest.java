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
import herddb.remote.storage.CrtS3HttpClientFactory;
import org.junit.Test;

/**
 * Verifies that the CRT HTTP-client configuration constants introduced by
 * issue #612 live in {@link CrtS3HttpClientFactory} (the shared factory) with
 * the correct property keys and default values, and that
 * {@link IndexingServerConfiguration} reads them from the same factory so that
 * the indexing service, the file server, and the optimizer all use identical
 * property names in their configuration files.
 */
public class IndexingServerCrtConfigTest {

    @Test
    public void crtMaxConcurrencyDefaultIsFour() {
        assertEquals(4, CrtS3HttpClientFactory.PROPERTY_CRT_MAX_CONCURRENCY_DEFAULT);
    }

    @Test
    public void crtReadBufferSizeDefaultIsOneGiB() {
        assertEquals(1024L * 1024 * 1024,
                CrtS3HttpClientFactory.PROPERTY_CRT_READ_BUFFER_SIZE_DEFAULT);
    }

    @Test
    public void crtMaxConcurrencyPropertyKeyIsCorrect() {
        assertEquals("s3.crt.max.concurrency",
                CrtS3HttpClientFactory.PROPERTY_CRT_MAX_CONCURRENCY);
    }

    @Test
    public void crtReadBufferSizePropertyKeyIsCorrect() {
        assertEquals("s3.crt.read.buffer.size",
                CrtS3HttpClientFactory.PROPERTY_CRT_READ_BUFFER_SIZE);
    }

    @Test
    public void defaultsAreReturnedWhenPropertiesAreAbsent() {
        IndexingServerConfiguration cfg = new IndexingServerConfiguration();
        assertEquals(CrtS3HttpClientFactory.PROPERTY_CRT_MAX_CONCURRENCY_DEFAULT,
                cfg.getInt(CrtS3HttpClientFactory.PROPERTY_CRT_MAX_CONCURRENCY,
                        CrtS3HttpClientFactory.PROPERTY_CRT_MAX_CONCURRENCY_DEFAULT));
        assertEquals(CrtS3HttpClientFactory.PROPERTY_CRT_READ_BUFFER_SIZE_DEFAULT,
                cfg.getLong(CrtS3HttpClientFactory.PROPERTY_CRT_READ_BUFFER_SIZE,
                        CrtS3HttpClientFactory.PROPERTY_CRT_READ_BUFFER_SIZE_DEFAULT));
    }

    @Test
    public void overridesAreHonouredByConfigurationAccessors() {
        IndexingServerConfiguration cfg = new IndexingServerConfiguration()
                .set(CrtS3HttpClientFactory.PROPERTY_CRT_MAX_CONCURRENCY, 2)
                .set(CrtS3HttpClientFactory.PROPERTY_CRT_READ_BUFFER_SIZE,
                        512L * 1024 * 1024);

        assertEquals(2,
                cfg.getInt(CrtS3HttpClientFactory.PROPERTY_CRT_MAX_CONCURRENCY,
                        CrtS3HttpClientFactory.PROPERTY_CRT_MAX_CONCURRENCY_DEFAULT));
        assertEquals(512L * 1024 * 1024,
                cfg.getLong(CrtS3HttpClientFactory.PROPERTY_CRT_READ_BUFFER_SIZE,
                        CrtS3HttpClientFactory.PROPERTY_CRT_READ_BUFFER_SIZE_DEFAULT));
    }
}
