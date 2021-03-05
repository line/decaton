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

package com.linecorp.decaton.processor.metrics;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.linecorp.decaton.processor.metrics.Metrics.SchedulerMetrics;

import io.micrometer.core.instrument.Meter;

public class MetricsTest {

    private static Set<Meter.Id> registeredIds() {
        return Metrics.registry().getMeters().stream().map(Meter::getId).collect(toSet());
    }

    private static Set<Meter.Id> idSet(Meter.Id... ids) {
        return new HashSet<>(Arrays.asList(ids));
    }

    @Before
    public void setUp() {
        Metrics.registry().clear();
    }

    @Test
    public void testMetricsLifecycleManagement() {
        String[] tags = {
                "subscription", "abc",
                "topic", "topic",
                "partition", "1",
        };
        SchedulerMetrics m1 = Metrics.withTags(tags).new SchedulerMetrics();
        SchedulerMetrics m2 = Metrics.withTags(tags).new SchedulerMetrics();
        // Creating multiple instances with Metrics with same labels must leave only unique metrics
        assertEquals(idSet(m1.partitionThrottledTime.getId(),
                           m1.tasksSchedulingDelay.getId()),
                     registeredIds());
        m2.close();
        // Closing one of them must not delete instances from registry yet
        assertEquals(idSet(m1.partitionThrottledTime.getId(),
                           m1.tasksSchedulingDelay.getId()),
                     registeredIds());
        m1.close();
        // Closing all of them must clean up instances from registry
        assertEquals(emptySet(), registeredIds());
    }
}
