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

import java.time.Duration;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;

public class Metrics {
    public static final String NAMESPACE = "decaton";

    private static final CompositeMeterRegistry registry = new CompositeMeterRegistry();
    static {
        registry.config().meterFilter(new MeterFilter() {
            @Override
            public Id map(Id id) {
                return id.withName(NAMESPACE + '.' + id.getName());
            }
        });
    }

    private final AvailableTags availableTags;

    private Metrics(AvailableTags availableTags) {
        this.availableTags = availableTags;
    }

    public class TaskMetrics {
        public final Counter tasksProcessed =
                Counter.builder("tasks.processed")
                       .description("The number of tasks processed")
                       .tags(availableTags.partitionScope())
                       .register(registry);

        public final Counter tasksDiscarded =
                Counter.builder("tasks.discarded")
                       .description("The number of tasks discarded")
                       .tags(availableTags.partitionScope())
                       .register(registry);

        public final Counter tasksError =
                Counter.builder("tasks.error")
                       .description("The number of tasks thrown exception by process")
                       .tags(availableTags.partitionScope())
                       .register(registry);
    }

    public class ProcessMetrics {
        public final Timer tasksCompleteDuration =
                Timer.builder("tasks.complete.duration")
                     .description("Time of a task taken to be completed")
                     .tags(availableTags.partitionScope())
                     .distributionStatisticExpiry(Duration.ofSeconds(60))
                     .publishPercentiles(0.5, 0.9, 0.99, 0.999)
                     .register(registry);

        public final Timer tasksProcessDuration =
                Timer.builder("tasks.process.duration")
                     .description("The time a task taken to be processed")
                     .tags(availableTags.partitionScope())
                     .distributionStatisticExpiry(Duration.ofSeconds(60))
                     .publishPercentiles(0.5, 0.9, 0.99, 0.999)
                     .register(registry);
    }

    public class ResourceUtilizationMetrics {
        public final Timer processorProcessedTime =
                Timer.builder("processor.processed.time")
                     .description("The accumulated time the processor were processing tasks")
                     .tags(availableTags.subpartitionScope())
                     .register(registry);

        public final Counter tasksQueued =
                Counter.builder("tasks.queued")
                       .description("The number of tasks queued per sub partitions")
                       .tags(availableTags.subpartitionScope())
                       .register(registry);
    }

    public class PartitionStateMetrics {
        public final ValueGauge tasksPending =
                ValueGauge.builder("tasks.pending")
                          .description("The number of pending tasks")
                          .tags(availableTags.partitionScope())
                          .register(registry);

        public final Timer partitionPausedTime =
                Timer.builder("partition.paused.time")
                     .description("The accumulated time the partition paused awaiting pending tasks' completion")
                     .tags(availableTags.partitionScope())
                     .register(registry);

        public final ValueGauge partitionsPaused =
                ValueGauge.builder("partitions.paused")
                          .description("The number of partitions currently paused for back pressure")
                          .tags(availableTags.topicScope())
                          .register(registry);
    }

    public class SchedulerMetrics {
        public final Timer tasksSchedulingDelay =
                Timer.builder("tasks.scheduling.delay")
                     .description("The time a task waiting for scheduled time")
                     .tags(availableTags.partitionScope())
                     .distributionStatisticExpiry(Duration.ofSeconds(60))
                     .publishPercentiles(0.5, 0.9, 0.99, 0.999)
                     .register(registry);

        public final Timer partitionThrottledTime =
                Timer.builder("partition.throttled.time")
                     .description("The accumulated time the partition throttled by rate limiter")
                     .tags(availableTags.partitionScope())
                     .register(registry);
    }

    public class RetryMetrics {
        public final Counter retryQueuedTasks =
                Counter.builder("retry.queued.tasks")
                       .description("The number of tasks queued to retry topic")
                       .tags(availableTags.subscriptionScope())
                       .register(registry);

        public final Counter retryQueueingFailed =
                Counter.builder("retry.queueing.failed")
                       .description("The number of tasks failed to enqueue in retry topic")
                       .tags(availableTags.subscriptionScope())
                       .register(registry);
    }

    public static Metrics withTags(String... keyValues) {
        return new Metrics(AvailableTags.of(keyValues));
    }

    public static void register(MeterRegistry registry) {
        Metrics.registry.add(registry);
    }

    public static MeterRegistry registry() {
        return registry;
    }
}
