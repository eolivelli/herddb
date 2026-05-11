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
package herddb.indexing.optimizer;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Identifies whether the current optimizer pod is the leader (produces and may
 * also execute tasks) or a worker (executes tasks only). In a Kubernetes
 * StatefulSet the leader is the pod with ordinal 0; workers are ordinals
 * 1..N-1.
 *
 * <p>Detection order (first match wins):
 * <ol>
 *   <li>Explicit config key {@code indexoptimizer.role.is.leader} —
 *       {@code true} / {@code false} forces the role; {@code auto} (default)
 *       falls through to the next step. Used by tests where there's no pod.</li>
 *   <li>Environment variable named by
 *       {@code indexoptimizer.role.pod.ordinal.env} (default {@code POD_ORDINAL}).
 *       The Helm chart wires this from the K8s downward API field
 *       {@code metadata.labels['apps.kubernetes.io/pod-index']}.</li>
 *   <li>Hostname regex named by
 *       {@code indexoptimizer.role.hostname.ordinal.regex}
 *       (default {@code ^.*-(\d+)$}, matching the StatefulSet naming
 *       convention {@code <release>-index-optimizer-<ordinal>}).</li>
 *   <li>Default {@link #WORKER} when nothing matches. A pod that can't prove
 *       it's leader stays out of the producer path so we never get two
 *       producers running by accident.</li>
 * </ol>
 */
public enum OptimizerRole {
    LEADER,
    WORKER;

    public static OptimizerRole detect(OptimizerConfiguration configuration, Map<String, String> env) {
        String explicit = configuration.getString(
                OptimizerConfiguration.PROPERTY_ROLE_IS_LEADER,
                OptimizerConfiguration.PROPERTY_ROLE_IS_LEADER_DEFAULT);
        if ("true".equalsIgnoreCase(explicit)) {
            return LEADER;
        }
        if ("false".equalsIgnoreCase(explicit)) {
            return WORKER;
        }
        String ordinalEnvName = configuration.getString(
                OptimizerConfiguration.PROPERTY_ROLE_POD_ORDINAL_ENV,
                OptimizerConfiguration.PROPERTY_ROLE_POD_ORDINAL_ENV_DEFAULT);
        Integer fromEnv = parseOrdinal(env.get(ordinalEnvName));
        if (fromEnv != null) {
            return fromEnv == 0 ? LEADER : WORKER;
        }
        String hostnameRegex = configuration.getString(
                OptimizerConfiguration.PROPERTY_ROLE_HOSTNAME_ORDINAL_REGEX,
                OptimizerConfiguration.PROPERTY_ROLE_HOSTNAME_ORDINAL_REGEX_DEFAULT);
        Integer fromHostname = extractOrdinalFromHostname(env.get("HOSTNAME"), hostnameRegex);
        if (fromHostname != null) {
            return fromHostname == 0 ? LEADER : WORKER;
        }
        return WORKER;
    }

    private static Integer parseOrdinal(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed >= 0 ? parsed : null;
        } catch (NumberFormatException nfe) {
            return null;
        }
    }

    private static Integer extractOrdinalFromHostname(String hostname, String regex) {
        if (hostname == null || hostname.isEmpty() || regex == null || regex.isEmpty()) {
            return null;
        }
        Matcher matcher;
        try {
            matcher = Pattern.compile(regex).matcher(hostname);
        } catch (java.util.regex.PatternSyntaxException badPattern) {
            return null;
        }
        if (!matcher.matches() || matcher.groupCount() < 1) {
            return null;
        }
        return parseOrdinal(matcher.group(1));
    }
}
