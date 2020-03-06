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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.SchedulerMetrics;

public class ExecutionScheduler implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ExecutionScheduler.class);

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
    void waitOnScheduledTime(TaskMetadata metadata) throws InterruptedException {
        long timeUntilProcess = metadata.scheduledTimeMillis() - currentTimeMillis.get();
        if (timeUntilProcess > 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Sleeping {} ms awaiting scheduled time for task in {} - ({})",
                             timeUntilProcess, scope, metadata);
            }
            sleep(timeUntilProcess);
        }

        metrics.tasksSchedulingDelay.record(Math.max(timeUntilProcess, 0), TimeUnit.MILLISECONDS);
    }

    public void schedule(TaskMetadata metadata) throws InterruptedException {
        // For tasks which are configured to delay its execution.
        waitOnScheduledTime(metadata);

        long throttledMicros = rateLimiter.acquire();
        if (throttledMicros > 0L) {
            metrics.partitionThrottledTime.record(throttledMicros, TimeUnit.MICROSECONDS);
        }
    }

    @Override
    public void close() {
        terminateLatch.countDown();
    }
}
