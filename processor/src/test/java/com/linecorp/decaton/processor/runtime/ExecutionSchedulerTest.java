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

package com.linecorp.decaton.processor.runtime;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.ProcessorProperties;
import com.linecorp.decaton.processor.TaskMetadata;

public class ExecutionSchedulerTest {
    private static final ThreadScope scope = new ThreadScope(
            new PartitionScope(
                    new SubscriptionScope("subscription", "topic",
                                          Optional.empty(), ProcessorProperties.builder().build()),
                    new TopicPartition("topic", 0)),
            0);

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private Supplier<Long> currentTimeMillis;

    @Mock
    private RateLimiter rateLimiter;

    private ExecutionScheduler scheduler;

    @Before
    public void setUp() {
        scheduler = spy(new ExecutionScheduler(scope, rateLimiter, currentTimeMillis));
    }

    @Test(timeout = 5000)
    public void testScheduledProcess_IMMEDIATE() throws InterruptedException {
        doReturn(1L).when(currentTimeMillis).get();
        scheduler.waitOnScheduledTime(TaskMetadata.builder().scheduledTimeMillis(0).build());
        verify(scheduler, never()).sleep(anyLong());

        scheduler.waitOnScheduledTime(TaskMetadata.builder().scheduledTimeMillis(1).build());
        verify(scheduler, never()).sleep(anyLong());
    }

    @Test(timeout = 5000)
    public void testScheduledProcess_DELAYED() throws InterruptedException {
        doReturn(1L).when(currentTimeMillis).get();
        long t0 = System.nanoTime();
        scheduler.waitOnScheduledTime(TaskMetadata.builder().scheduledTimeMillis(500).build());
        long elapsed = System.nanoTime() - t0;

        verify(scheduler, times(1)).sleep(499);
        assertTrue(TimeUnit.NANOSECONDS.toMillis(elapsed) >= 499);
    }
}
