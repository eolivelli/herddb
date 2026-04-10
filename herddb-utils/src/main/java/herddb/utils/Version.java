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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author enrico.olivelli
 */
public class Version {

    static final String UNKNOWN_GIT_COMMIT = "unknown";

    private static final String VERSION;
    private static final String GIT_COMMIT;
    private static final int JDBC_DRIVER_MAJOR_VERSION;
    private static final int JDBC_DRIVER_MINOR_VERSION;

    static {
        Properties pluginProperties = new Properties();
        try (InputStream in = Version.class.getResourceAsStream("/META-INF/herddb.version.properties")) {
            if (in != null) {
                pluginProperties.load(in);
            }
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
        Parsed parsed = parse(pluginProperties);
        VERSION = parsed.version;
        GIT_COMMIT = parsed.gitCommit;
        JDBC_DRIVER_MAJOR_VERSION = parsed.jdbcMajor;
        JDBC_DRIVER_MINOR_VERSION = parsed.jdbcMinor;
    }

    static final class Parsed {
        final String version;
        final String gitCommit;
        final int jdbcMajor;
        final int jdbcMinor;

        Parsed(String version, String gitCommit, int jdbcMajor, int jdbcMinor) {
            this.version = version;
            this.gitCommit = gitCommit;
            this.jdbcMajor = jdbcMajor;
            this.jdbcMinor = jdbcMinor;
        }
    }

    static Parsed parse(Properties properties) {
        String version = properties.getProperty("version", "");
        String gitCommitRaw = properties.getProperty("git.commit", "");
        String gitCommit;
        // If Maven resource filtering did not run (e.g. inside an IDE that bypasses
        // the buildnumber-maven-plugin), the value is the literal placeholder.
        // Fall back to a stable sentinel so consumers never see "${buildNumber}".
        if (gitCommitRaw.isEmpty() || gitCommitRaw.startsWith("${")) {
            gitCommit = UNKNOWN_GIT_COMMIT;
        } else {
            gitCommit = gitCommitRaw;
        }
        int major = 0;
        int minor = 0;
        String[] split = version.split("\\.");
        if (split.length > 2) {
            try {
                major = Integer.parseInt(split[0]);
                minor = Integer.parseInt(split[1]);
            } catch (NumberFormatException ignore) {
                major = 0;
                minor = 0;
            }
        }
        return new Parsed(version, gitCommit, major, minor);
    }

    public static String getVERSION() {
        return VERSION;
    }

    public static String getGitCommit() {
        return GIT_COMMIT;
    }

    public static int getJDBC_DRIVER_MAJOR_VERSION() {
        return JDBC_DRIVER_MAJOR_VERSION;
    }

    public static int getJDBC_DRIVER_MINOR_VERSION() {
        return JDBC_DRIVER_MINOR_VERSION;
    }

}
