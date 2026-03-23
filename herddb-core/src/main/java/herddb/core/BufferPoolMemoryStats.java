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

package herddb.core;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Exposes JVM buffer pool memory statistics (direct and mapped) as metrics.
 */
public class BufferPoolMemoryStats {

    BufferPoolMemoryStats(StatsLogger statsLogger) {
        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for (BufferPoolMXBean pool : pools) {
            String name = pool.getName(); // "direct", "mapped", "mapped - 'ByteBuffer'"
            String safeName = name.replace(" ", "_").replace("'", "").replace("-", "");
            statsLogger.registerGauge("jvm_buffer_" + safeName + "_used_bytes", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }

                @Override
                public Long getSample() {
                    return pool.getMemoryUsed();
                }
            });
            statsLogger.registerGauge("jvm_buffer_" + safeName + "_capacity_bytes", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }

                @Override
                public Long getSample() {
                    return pool.getTotalCapacity();
                }
            });
            statsLogger.registerGauge("jvm_buffer_" + safeName + "_count", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }

                @Override
                public Long getSample() {
                    return pool.getCount();
                }
            });
        }
    }
}
