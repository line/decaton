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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;

import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.SchedulerMetrics;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecutionScheduler implements AutoCloseable {
    private final ThreadScope scope;
    private final RateLimiter rateLimiter;
    private final Supplier<Long> currentTimeMillis;
    private final CountDownLatch terminateLatch;
    private final SchedulerMetrics metrics;

    ExecutionScheduler(ThreadScope scope, RateLimiter rateLimiter, Supplier<Long> currentTimeMillis) {
        this.scope = scope;
        this.rateLimiter = rateLimiter;
        this.currentTimeMillis = currentTimeMillis;
        terminateLatch = new CountDownLatch(1);

        TopicPartition tp = scope.topicPartition();
        metrics = Metrics.withTags("subscription", scope.subscriptionId(),
                                   "topic", tp.topic(),
                                   "partition", String.valueOf(tp.partition()))
                .new SchedulerMetrics();
    }

    public ExecutionScheduler(ThreadScope scope, RateLimiter rateLimiter) {
        this(scope, rateLimiter, System::currentTimeMillis);
    }

    // visible for testing
    void sleep(long millis) throws InterruptedException {
        terminateLatch.await(millis, TimeUnit.MILLISECONDS);
    }

    // visible for testing
    long waitOnScheduledTime(TaskMetadata metadata) throws InterruptedException {
        long timeToWaitMs = metadata.scheduledTimeMillis() - currentTimeMillis.get();
        if (timeToWaitMs > 0) {
            log.debug("Sleeping {} ms awaiting scheduled time for task in {} - ({})",
                      timeToWaitMs, scope, metadata);
            sleep(timeToWaitMs);
        }
        return Math.max(0, timeToWaitMs);
    }

    /**
     * Wait for the time ready to start executing a task which is associated with the given
     * {@link TaskMetadata}.
     * This method pauses until all below criteria tells it is ready to start executing next task.
     * - Execution time configured in task metadata
     * - Per-partition rate limiting
     *
     * This method might returns earlier than the time ready to execute the task when {@link #close()} called.
     *
     * @param metadata the {@link TaskMetadata} associated with the task waiting to be processed.
     * @throws InterruptedException when interrupted.
     */
    public void schedule(TaskMetadata metadata) throws InterruptedException {
        // For tasks which are configured to delay its execution.
        long timeWaitedMs = waitOnScheduledTime(metadata);
        if (terminated()) {
            return;
        }
        metrics.tasksSchedulingDelay.record(timeWaitedMs, TimeUnit.MILLISECONDS);

        long throttledMicros = rateLimiter.acquire();
        if (terminated()) {
            return;
        }
        metrics.partitionThrottledTime.record(throttledMicros, TimeUnit.MICROSECONDS);
    }

    private boolean terminated() {
        return terminateLatch.getCount() == 0;
    }

    @Override
    public void close() {
        terminateLatch.countDown();
        metrics.close();
    }
}
