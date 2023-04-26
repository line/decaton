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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;

import com.linecorp.decaton.processor.metrics.internal.AvailableTags;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
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

    /**
     * Superclass of all *Metrics classes managing lifecycle of meter instances.
     * This class implements refcounting for meters identified by {@link Meter.Id} and make sure to remove
     * the instance from {@link MeterRegistry} when the last reference to the meter closed and disappeared.
     */
    abstract static class AbstractMetrics implements AutoCloseable {
        static final Map<Id, AtomicInteger> meterRefCounts = new HashMap<>();
        private final List<Meter> meters = new ArrayList<>();

        <T extends Meter> T meter(Supplier<T> ctor) {
            synchronized (meterRefCounts) {
                T meter = ctor.get();
                meterRefCounts.computeIfAbsent(meter.getId(), key -> new AtomicInteger())
                              .incrementAndGet();
                meters.add(meter);
                return meter;
            }
        }

        @Override
        public void close() {
            synchronized (meterRefCounts) {
                // traverse from the end to avoid arrayCopy
                for (ListIterator<Meter> iterator = meters.listIterator(meters.size()); iterator.hasPrevious(); ) {
                    Meter meter = iterator.previous();
                    Id id = meter.getId();
                    AtomicInteger count = meterRefCounts.get(id);
                    if (count == null) {
                        throw new IllegalStateException("Missing reference to meter: " + id);
                    }
                    if (count.decrementAndGet() <= 0) {
                        meterRefCounts.remove(id);
                        registry.remove(meter);
                        meter.close();
                    }
                    // make close idempotent
                    iterator.remove();
                }
            }
        }
    }

    public class SubscriptionMetrics extends AbstractMetrics {
        volatile KafkaClientMetrics kafkaClientMetrics;

        public void bindClientMetrics(Consumer<byte[], byte[]> consumer) {
            kafkaClientMetrics = new KafkaClientMetrics(consumer, availableTags.subscriptionScope());
            kafkaClientMetrics.bindTo(registry);
        }

        private void closeClientMetrics() {
            if (kafkaClientMetrics != null) {
                kafkaClientMetrics.close();
                kafkaClientMetrics = null;
            }
        }

        private Timer processDuration(String section) {
            return meter(() -> Timer.builder("subscription.process.durations")
                                    .description(String.format(
                                            "Time spent for processing %s in consuming loop", section))
                                    .tags(availableTags.subscriptionScope().and("section", section))
                                    .distributionStatisticExpiry(Duration.ofSeconds(60))
                                    .publishPercentiles(0.5, 0.9, 0.99, 0.999)
                                    .register(registry));
        }

        public final Timer consumerPollTime = processDuration("poll");

        public final Timer handleRecordsTime = processDuration("records");

        public final Timer reloadContextsTime = processDuration("reload");

        public final Timer handlePausesTime = processDuration("pause");

        public final Timer commitOffsetTime = processDuration("commit");

        @Override
        public void close() {
            super.close();
            closeClientMetrics();
        }
    }

    public class TaskMetrics extends AbstractMetrics {
        public final Counter tasksProcessed =
                meter(() -> Counter.builder("tasks.processed")
                                   .description("The number of tasks processed")
                                   .tags(availableTags.partitionScope())
                                   .register(registry));

        public final Counter tasksDiscarded =
                meter(() -> Counter.builder("tasks.discarded")
                                   .description("The number of tasks discarded")
                                   .tags(availableTags.partitionScope())
                                   .register(registry));

        public final Counter tasksError =
                meter(() -> Counter.builder("tasks.error")
                                   .description("The number of tasks thrown exception by process")
                                   .tags(availableTags.partitionScope())
                                   .register(registry));
    }

    public class ProcessMetrics extends AbstractMetrics {
        public final Timer tasksCompleteDuration =
                meter(() -> Timer.builder("tasks.complete.duration")
                                 .description("Time of a task taken to be completed")
                                 .tags(availableTags.partitionScope())
                                 .distributionStatisticExpiry(Duration.ofSeconds(60))
                                 .publishPercentiles(0.5, 0.9, 0.99, 0.999)
                                 .register(registry));

        public final Timer tasksProcessDuration =
                meter(() -> Timer.builder("tasks.process.duration")
                                 .description("The time a task taken to be processed")
                                 .tags(availableTags.partitionScope())
                                 .distributionStatisticExpiry(Duration.ofSeconds(60))
                                 .publishPercentiles(0.5, 0.9, 0.99, 0.999)
                                 .register(registry));
    }

    public class CommitControlMetrics extends AbstractMetrics {
        public final Counter tasksTimeout =
                meter(() -> Counter.builder("tasks.timeout")
                                   .description("The number of tasks timed out and forcefully completed by deferred completion timeout")
                                   .tags(availableTags.partitionScope())
                                   .register(registry));
    }

    public class ResourceUtilizationMetrics extends AbstractMetrics {
        public final Timer processorProcessedTime =
                meter(() -> Timer.builder("processor.processed.time")
                                 .description("The accumulated time the processor were processing tasks")
                                 .tags(availableTags.subpartitionScope())
                                 .register(registry));

        public final Counter tasksQueued =
                meter(() -> Counter.builder("tasks.queued")
                                   .description("The number of tasks queued per sub partitions")
                                   .tags(availableTags.subpartitionScope())
                                   .register(registry));
    }

    public class PartitionStateMetrics extends AbstractMetrics {
        public final Gauge tasksPending;
        public final Gauge partitionPaused;
        public final Gauge lastCommittedOffset;
        public final Gauge latestConsumedOffset;

        public final Timer queueStarvedTime =
                meter(() -> Timer.builder("partition.queue.starved.time")
                                 .description("Total duration of time the partition's queue was starving")
                                 .tags(availableTags.partitionScope())
                                 .register(registry));

        public final Timer partitionPausedTime =
                meter(() -> Timer.builder("partition.paused.time")
                                 .description(
                                         "The accumulated time the partition paused awaiting pending tasks' completion")
                                 .tags(availableTags.partitionScope())
                                 .register(registry));


        public PartitionStateMetrics(Supplier<Number> pendingTasksFn,
                                     Supplier<Number> partitionPausedFn,
                                     Supplier<Number> lastCommittedOffsetFn,
                                     Supplier<Number> latestConsumedOffsetFn) {
            tasksPending = meter(() -> Gauge.builder("tasks.pending", pendingTasksFn)
                                            .description("The number of pending tasks")
                                            .tags(availableTags.partitionScope())
                                            .register(registry));
            partitionPaused = meter(() -> Gauge.builder("partition.paused", partitionPausedFn)
                                               .description("Whether the partition is paused. 1 if paused, 0 otherwise")
                                               .tags(availableTags.partitionScope())
                                               .register(registry));
            lastCommittedOffset = meter(() -> Gauge.builder("offset.last.committed", lastCommittedOffsetFn)
                                                   .description("The last committed offset")
                                                   .tags(availableTags.partitionScope())
                                                   .register(registry()));
            latestConsumedOffset = meter(() -> Gauge.builder("offset.latest.consumed", latestConsumedOffsetFn)
                                                    .description("The latest consumed offset")
                                                    .tags(availableTags.partitionScope())
                                                    .register(registry()));
        }
    }

    public class SchedulerMetrics extends AbstractMetrics {
        public final Timer tasksSchedulingDelay =
                meter(() -> Timer.builder("tasks.scheduling.delay")
                                 .description("The time a task waiting for scheduled time")
                                 .tags(availableTags.partitionScope())
                                 .distributionStatisticExpiry(Duration.ofSeconds(60))
                                 .publishPercentiles(0.5, 0.9, 0.99, 0.999)
                                 .register(registry));

        public final Timer partitionThrottledTime =
                meter(() -> Timer.builder("partition.throttled.time")
                                 .description("The accumulated time the partition throttled by rate limiter")
                                 .tags(availableTags.partitionScope())
                                 .register(registry));
    }

    public class RetryMetrics extends AbstractMetrics {
        public final Counter retryQueuedTasks =
                meter(() -> Counter.builder("retry.queued.tasks")
                                   .description("The number of tasks queued to retry topic")
                                   .tags(availableTags.subscriptionScope())
                                   .register(registry));

        public final Counter retryQueueingFailed =
                meter(() -> Counter.builder("retry.queueing.failed")
                                   .description("The number of tasks failed to enqueue in retry topic")
                                   .tags(availableTags.subscriptionScope())
                                   .register(registry));

        public final DistributionSummary retryTaskRetries =
                meter(() -> DistributionSummary.builder("retry.task.retries")
                                               .description("The number of times a task was retried")
                                               .tags(availableTags.subscriptionScope())
                                               .register(registry));
    }

    public class ShapingMetrics extends AbstractMetrics {
        public final Counter shapingQueuedTasks =
                meter(() -> Counter.builder("shaping.queued.tasks")
                                   .description("The number of tasks queued to shaping topic")
                                   .tags(availableTags.subscriptionScope())
                                   .register(registry));

        public final Counter shapingQueueingFailed =
                meter(() -> Counter.builder("shaping.queueing.failed")
                                   .description("The number of tasks failed to enqueue in shaping topic")
                                   .tags(availableTags.subscriptionScope())
                                   .register(registry));
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
