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
package herddb.kubernetes;

final class DockerProcessUtil {

    private DockerProcessUtil() {
    }

    /**
     * Build a {@link ProcessBuilder} that invokes the docker CLI without inheriting the
     * {@code DOCKER_API_VERSION} environment variable that Testcontainers pins inside the JVM.
     * Testcontainers locks itself to the API version supported by its bundled docker-java client,
     * which on Docker 28+ daemons is too old and makes the docker CLI fail with
     * "client version X is too old. Minimum supported API version is Y".
     */
    static ProcessBuilder dockerProcess(String... command) {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.environment().remove("DOCKER_API_VERSION");
        return pb;
    }
}
