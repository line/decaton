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

package com.linecorp.decaton.processor.runtime.internal;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.SchedulerMetrics;
import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.SubPartitionRuntime;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ExecutionSchedulerTest {
    private static final ThreadScope scope = new ThreadScope(
            new PartitionScope(
                    new SubscriptionScope("subscription", "topic",
                                          SubPartitionRuntime.THREAD_POOL,
                                          Optional.empty(), Optional.empty(), ProcessorProperties.builder().build(),
                                          NoopTracingProvider.INSTANCE,
                                          ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                                          DefaultSubPartitioner::new),
                    new TopicPartition("topic", 0)),
            0);

    @Mock
    private Supplier<Long> currentTimeMillis;

    @Spy
    private final RateLimiter rateLimiter = RateLimiter.create(RateLimiter.PAUSED);

    private ExecutionScheduler scheduler;

    @BeforeEach
    public void setUp() {
        doAnswer(invocation -> System.currentTimeMillis()).when(currentTimeMillis).get();
        SchedulerMetrics metrics = Metrics.withTags("subscription", "sub",
                                                    "topic", "topic",
                                                    "partition", "0").new SchedulerMetrics();
        scheduler = spy(new ExecutionScheduler(scope, rateLimiter, metrics, currentTimeMillis));
    }

    @Test
    @Timeout(5)
    public void testScheduledProcess_IMMEDIATE() throws InterruptedException {
        doReturn(1L).when(currentTimeMillis).get();
        scheduler.waitOnScheduledTime(TaskMetadata.builder().scheduledTimeMillis(0).build());
        verify(scheduler, never()).sleep(anyLong());

        scheduler.waitOnScheduledTime(TaskMetadata.builder().scheduledTimeMillis(1).build());
        verify(scheduler, never()).sleep(anyLong());
    }

    @Test
    @Timeout(5)
    public void testScheduledProcess_DELAY_BY_METADATA() throws InterruptedException {
        doReturn(1L).when(currentTimeMillis).get();
        long t0 = System.nanoTime();
        scheduler.waitOnScheduledTime(TaskMetadata.builder().scheduledTimeMillis(500).build());
        long elapsed = System.nanoTime() - t0;

        verify(scheduler, times(1)).sleep(499);
        assertTrue(TimeUnit.NANOSECONDS.toMillis(elapsed) >= 499);
    }

    @Test
    @Timeout(5)
    public void testScheduledProcess_TERMINATE_AT_METADATA() throws Exception {
        CountDownLatch atSleepLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            atSleepLatch.countDown();
            return invocation.callRealMethod();
        }).when(scheduler).sleep(anyLong());

        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.execute(() -> {
            try {
                scheduler.schedule(TaskMetadata.builder().scheduledTimeMillis(Long.MAX_VALUE).build());
            } catch (InterruptedException e) {
                fail("Fail by exception " + e);
            }
        });
        atSleepLatch.await();
        scheduler.close();

        // Checking the above call actually returns
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        verify(rateLimiter, never()).acquire();
    }

    @Test
    @Timeout(5)
    public void testScheduledProcess_TERMINATE_AT_LIMITER() throws Exception {
        CountDownLatch atAcquire = new CountDownLatch(1);
        doAnswer(invocation -> {
            atAcquire.countDown();
            return invocation.callRealMethod();
        }).when(rateLimiter).acquire();

        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.execute(() -> {
            try {
                scheduler.schedule(TaskMetadata.builder().scheduledTimeMillis(0).build());
            } catch (InterruptedException e) {
                fail("Fail by exception " + e);
            }
        });
        atAcquire.await();
        scheduler.close();
        rateLimiter.close();

        // Checking the above call actually returns
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    @Test
    @Timeout(5)
    public void testScheduledProcess_TERMINATE_AT_ENTER() throws Exception {
        scheduler.close();
        rateLimiter.close();

        // Checking the above call actually returns
        scheduler.schedule(TaskMetadata.builder().scheduledTimeMillis(Long.MAX_VALUE).build());
    }
}
