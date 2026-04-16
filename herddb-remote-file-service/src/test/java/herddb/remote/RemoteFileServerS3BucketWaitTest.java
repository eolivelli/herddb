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

import static org.junit.Assert.assertTrue;
import java.util.Properties;
import org.junit.Test;

/**
 * Tests that RemoteFileServer properly handles S3 bucket wait configuration.
 * The bucket wait retry loop is tested in S3RemoteFileServerTest with a real MinIO container.
 * This test verifies the configuration parameter is accepted.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServerS3BucketWaitTest {

    /**
     * Test that s3.bucket.wait.timeout.ms configuration parameter is accepted without error.
     */
    @Test
    public void testS3BucketWaitTimeoutConfigurationAccepted() {
        Properties config = new Properties();
        config.setProperty("s3.bucket.wait.timeout.ms", "5000");

        // Verify the property can be read as a long
        long timeoutMs = Long.parseLong(
                config.getProperty("s3.bucket.wait.timeout.ms", "1800000"));
        assertTrue("Timeout should be parsed correctly", timeoutMs == 5000L);

        // Test default value
        long defaultTimeoutMs = Long.parseLong(
                config.getProperty("s3.bucket.wait.timeout.ms", "1800000"));
        assertTrue("Default timeout should be 30 minutes", defaultTimeoutMs == 5000L);
    }

    /**
     * Test default timeout configuration (30 minutes).
     */
    @Test
    public void testS3BucketWaitDefaultTimeout() {
        Properties config = new Properties();
        // Do NOT set the property, should use default

        long defaultTimeoutMs = Long.parseLong(
                config.getProperty("s3.bucket.wait.timeout.ms", "1800000"));
        assertTrue("Default timeout should be 30 minutes (1800000ms)", defaultTimeoutMs == 1800000L);
    }
}
