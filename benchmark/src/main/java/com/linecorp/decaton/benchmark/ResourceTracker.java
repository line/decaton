/*
 * Copyright 2020 LINE Corporation
 *
 * LINE Corporation licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.linecorp.decaton.benchmark;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.sun.management.ThreadMXBean;

import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResourceTracker {
    @Value
    @Accessors(fluent = true)
    public static class TrackingValues {
        long cpuTime;
        long allocatedBytes;
    }

    private static final ThreadLocal<Boolean> tracking = ThreadLocal.withInitial(() -> false);
    private final ConcurrentMap<Long, TrackingValues> targetThreadIds;
    private final ThreadMXBean threadMxBean;

    public ResourceTracker() {
        targetThreadIds = new ConcurrentHashMap<>();
        threadMxBean = getSunThreadMxBean();
    }

    private static ThreadMXBean getSunThreadMxBean() {
        java.lang.management.ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
        if (mxBean instanceof ThreadMXBean) {
            return (ThreadMXBean) mxBean;
        }
        throw new IllegalStateException(
                "requires sun.management.ThreadMXBean of but found " + mxBean.getClass());
    }

    /**
     * Start tracking a thread specified by the given ID.
     * This method should be called at the very beginning of the thread before it starts doing actual work
     * because its base values for accumulated counters (e.g, cputime) is recorded upon its registration.
     * Duplicate call for the same thread ID is fine and it has no effect.
     *
     * @param threadId thread ID to start tracking.
     */
    public void track(long threadId) {
        if (tracking.get()) {
            return;
        }
        tracking.set(true);
        targetThreadIds.computeIfAbsent(
                threadId,
                key -> new TrackingValues(threadMxBean.getThreadCpuTime(threadId),
                                          threadMxBean.getThreadAllocatedBytes(threadId)));
    }

    /**
     * Report all tracking threads resource usage at the point this method is called.
     * The returned {@link TrackingValues} might contains {@code -1} for those that it has failed to observe.
     *
     * @return per-thread map where key is the thread ID and the value is tracked values.
     */
    public Map<Long, TrackingValues> report() {
        Map<Long, TrackingValues> report = new HashMap<>();

        for (Entry<Long, TrackingValues> entry : targetThreadIds.entrySet()) {
            Long threadId = entry.getKey();
            TrackingValues values = entry.getValue();
            long curCpuTime = threadMxBean.getThreadCpuTime(threadId);
            if (curCpuTime < 0) {
                log.warn("Couldn't record cpuTime for thread {}", threadId);
            }
            long cpuTime = Math.max(-1, curCpuTime - values.cpuTime);

            long curAllocatedBytes = threadMxBean.getThreadAllocatedBytes(threadId);
            if (curAllocatedBytes < 0) {
                log.warn("Couldn't record allocatedBytes for thread {}", threadId);
            }
            long allocatedBytes = Math.max(-1, curAllocatedBytes - values.allocatedBytes);

            report.put(threadId, new TrackingValues(cpuTime, allocatedBytes));
        }

        return report;
    }
}
