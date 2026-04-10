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

package herddb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import java.util.Properties;
import org.junit.Test;

public class VersionTest {

    @Test
    public void parsesVersionAndGitCommit() {
        Properties props = new Properties();
        props.setProperty("version", "0.30.0-SNAPSHOT");
        props.setProperty("git.commit", "abcdef12345");

        Version.Parsed parsed = Version.parse(props);

        assertEquals("0.30.0-SNAPSHOT", parsed.version);
        assertEquals("abcdef12345", parsed.gitCommit);
        assertEquals(0, parsed.jdbcMajor);
        assertEquals(30, parsed.jdbcMinor);
    }

    @Test
    public void missingGitCommitFallsBackToUnknown() {
        Properties props = new Properties();
        props.setProperty("version", "0.30.0-SNAPSHOT");

        Version.Parsed parsed = Version.parse(props);

        assertEquals(Version.UNKNOWN_GIT_COMMIT, parsed.gitCommit);
    }

    @Test
    public void unfilteredGitCommitPlaceholderFallsBackToUnknown() {
        Properties props = new Properties();
        props.setProperty("version", "0.30.0-SNAPSHOT");
        props.setProperty("git.commit", "${buildNumber}");

        Version.Parsed parsed = Version.parse(props);

        assertEquals(Version.UNKNOWN_GIT_COMMIT, parsed.gitCommit);
    }

    @Test
    public void jdbcMajorMinorFallBackToZeroForSingleSegmentVersion() {
        Properties props = new Properties();
        props.setProperty("version", "dev");
        props.setProperty("git.commit", "cafebabe000");

        Version.Parsed parsed = Version.parse(props);

        assertEquals(0, parsed.jdbcMajor);
        assertEquals(0, parsed.jdbcMinor);
    }

    @Test
    public void jdbcMajorMinorFallBackToZeroForNonNumericSegments() {
        Properties props = new Properties();
        props.setProperty("version", "a.b.c");

        Version.Parsed parsed = Version.parse(props);

        assertEquals(0, parsed.jdbcMajor);
        assertEquals(0, parsed.jdbcMinor);
    }

    @Test
    public void classpathResourceLoadsSuccessfully() {
        String version = Version.getVERSION();
        assertNotNull(version);
        assertFalse(version.isEmpty());
        assertNotNull(Version.getGitCommit());
    }
}
