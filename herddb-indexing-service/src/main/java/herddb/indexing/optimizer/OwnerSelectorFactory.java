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

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Builds an {@link OwnerSelector} from {@link OptimizerConfiguration}. Step 1
 * default is {@link Fixed0OwnerSelector} so production behaviour is unchanged
 * across the boundary; step 2 will flip the default to {@code LEAST_LOADED}.
 */
public final class OwnerSelectorFactory {

    private static final Logger LOGGER = Logger.getLogger(OwnerSelectorFactory.class.getName());

    private OwnerSelectorFactory() {
    }

    /**
     * @param probe may be {@code null} — required only when policy is
     *     {@code LEAST_LOADED}; otherwise the factory falls back to
     *     {@link Fixed0OwnerSelector} and logs SEVERE so the misconfiguration
     *     is visible.
     */
    public static OwnerSelector create(OptimizerConfiguration configuration, InstanceLoadProbe probe) {
        String policy = configuration.getString(
                OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_POLICY,
                OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_POLICY_DEFAULT);
        switch (policy.trim().toUpperCase(java.util.Locale.ROOT)) {
            case "FIXED_ZERO":
                return new Fixed0OwnerSelector();
            case "ROUND_ROBIN":
                return new RoundRobinOwnerSelector(numInstancesSupplier(configuration));
            case "STATIC":
                return new StaticAssignmentOwnerSelector(parseStaticAssignment(configuration));
            case "LEAST_LOADED":
                if (probe == null) {
                    LOGGER.log(Level.SEVERE,
                            "owner selector policy {0} requires a live InstanceLoadProbe but none"
                                    + " was provided; falling back to {1}",
                            new Object[]{"LEAST_LOADED", "FIXED_ZERO"});
                    return new Fixed0OwnerSelector();
                }
                return new LeastLoadedOwnerSelector(probe);
            default:
                throw new IllegalArgumentException("unknown "
                        + OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_POLICY
                        + " value: " + policy);
        }
    }

    private static IntSupplier numInstancesSupplier(OptimizerConfiguration configuration) {
        return () -> configuration.getInt(
                OptimizerConfiguration.PROPERTY_INDEXING_NUM_INSTANCES,
                OptimizerConfiguration.PROPERTY_INDEXING_NUM_INSTANCES_DEFAULT);
    }

    private static int[] parseStaticAssignment(OptimizerConfiguration configuration) {
        String csv = configuration.getString(
                OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_STATIC_ASSIGNMENT, "");
        if (csv == null || csv.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_STATIC_ASSIGNMENT
                            + " must be set when "
                            + OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_POLICY
                            + "=STATIC");
        }
        List<Integer> parsed = new ArrayList<>();
        for (String token : csv.split(",")) {
            String trimmed = token.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            parsed.add(Integer.parseInt(trimmed));
        }
        int[] assignment = new int[parsed.size()];
        for (int i = 0; i < assignment.length; i++) {
            assignment[i] = parsed.get(i);
        }
        return assignment;
    }
}
