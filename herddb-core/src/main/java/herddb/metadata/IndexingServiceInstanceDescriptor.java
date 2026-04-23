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

package herddb.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Objects;

/**
 * Registration record for a single indexing-service process. Primaries and
 * shadow replicas both register under
 * {@code /herddb/indexingServices/instances/{serviceId}} as an ephemeral
 * znode whose payload is the JSON serialization of this descriptor.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class IndexingServiceInstanceDescriptor {

    public static final String ROLE_PRIMARY = "primary";
    public static final String ROLE_SHADOW = "shadow";
    public static final int NO_SHADOW_OF = -1;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String serviceId;
    private final String address;
    private final String role;
    private final int instanceId;
    private final int shadowOf;

    @JsonCreator
    public IndexingServiceInstanceDescriptor(
            @JsonProperty("serviceId") String serviceId,
            @JsonProperty("address") String address,
            @JsonProperty("role") String role,
            @JsonProperty("instanceId") int instanceId,
            @JsonProperty("shadowOf") int shadowOf) {
        this.serviceId = Objects.requireNonNull(serviceId, "serviceId");
        this.address = Objects.requireNonNull(address, "address");
        this.role = Objects.requireNonNull(role, "role");
        this.instanceId = instanceId;
        this.shadowOf = shadowOf;
    }

    public static IndexingServiceInstanceDescriptor primary(String serviceId, String address, int instanceId) {
        return new IndexingServiceInstanceDescriptor(serviceId, address, ROLE_PRIMARY, instanceId, NO_SHADOW_OF);
    }

    public static IndexingServiceInstanceDescriptor shadow(String serviceId, String address, int shadowOf) {
        return new IndexingServiceInstanceDescriptor(serviceId, address, ROLE_SHADOW, shadowOf, shadowOf);
    }

    public String getServiceId() {
        return serviceId;
    }

    public String getAddress() {
        return address;
    }

    public String getRole() {
        return role;
    }

    public int getInstanceId() {
        return instanceId;
    }

    public int getShadowOf() {
        return shadowOf;
    }

    @JsonIgnore
    public boolean isShadow() {
        return ROLE_SHADOW.equals(role);
    }

    /**
     * Effective instanceId for query routing: a shadow answers for its
     * {@code shadowOf} primary; a primary answers for its own {@code instanceId}.
     */
    @JsonIgnore
    public int effectiveInstanceId() {
        return isShadow() ? shadowOf : instanceId;
    }

    public byte[] serialize() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize descriptor", e);
        }
    }

    public static IndexingServiceInstanceDescriptor deserialize(byte[] data) throws IOException {
        return MAPPER.readValue(data, IndexingServiceInstanceDescriptor.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexingServiceInstanceDescriptor)) {
            return false;
        }
        IndexingServiceInstanceDescriptor that = (IndexingServiceInstanceDescriptor) o;
        return instanceId == that.instanceId
                && shadowOf == that.shadowOf
                && serviceId.equals(that.serviceId)
                && address.equals(that.address)
                && role.equals(that.role);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceId, address, role, instanceId, shadowOf);
    }

    @Override
    public String toString() {
        return "IndexingServiceInstanceDescriptor{"
                + "serviceId='" + serviceId + '\''
                + ", address='" + address + '\''
                + ", role='" + role + '\''
                + ", instanceId=" + instanceId
                + ", shadowOf=" + shadowOf
                + '}';
    }
}
