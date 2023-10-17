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

import static com.linecorp.decaton.processor.runtime.ProcessorProperties.CONFIG_PROCESSOR_THREADS_TERMINATION_TIMEOUT_MS;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.PerPartitionMetrics;
import com.linecorp.decaton.processor.runtime.AsyncClosable;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.SubPartitioner;

/**
 * This class is responsible for following portions:
 * - Create and manage configured number of {@link ProcessorUnit}s to parallel-ize processing of tasks received
 *   from single partition.
 * - Route fed task appropriately to one of belonging {@link ProcessorUnit}s, respecting task's key for keeping
 *   process locally and ordering.
 * - Manage lifecycle of {@link DecatonProcessor}s for each {@link ProcessorUnit}s.
 */
public class PartitionProcessor implements AsyncClosable {
    private static final Logger logger = LoggerFactory.getLogger(PartitionProcessor.class);

    private final PartitionScope scope;
    private final Processors<?> processors;

    private final Map<Integer, ProcessorUnit> units;

    private final SubPartitioner subPartitioner;

    // Sharing this limiter object with all processor threads
    // can make processing unfair but it likely gives better overall throughput
    private final RateLimiter rateLimiter;

    private final PerPartitionMetrics metrics;

    private final Property<Long> processorThreadTerminationTimeoutMillis;

    public PartitionProcessor(PartitionScope scope, Processors<?> processors) {
        this.scope = scope;
        this.processors = processors;

        // Create units with latest property value.
        //
        // NOTE: If the property value is changed multiple times at short intervals,
        // each partition processor can have different number of units temporarily.
        // But it's not a problem because all partitions will be kept paused until all reload requests done.
        // Let's see this by example:
        //   1. change concurrency from 1 to 5 => start reloading
        //   2. processor 1,2,3 are reloaded with 5
        //   3. change concurrency from 5 to 3 during reloading => request reloading again, so partitions will be kept paused
        //   4. at next subscription loop, all processors are reloaded with 3 again, then start processing
        int concurrency = scope.props().get(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY).value();
        units = new HashMap<>(concurrency);
        subPartitioner = scope.subPartitionerSupplier().get(concurrency);
        rateLimiter = new DynamicRateLimiter(rateProperty(scope));
        metrics = Metrics.withTags(
                "subscription", scope.subscriptionId(),
                "topic", scope.topic(),
                "partition", String.valueOf(scope.topicPartition().partition()))
                .new PerPartitionMetrics();
        processorThreadTerminationTimeoutMillis = scope.props()
                                                       .get(CONFIG_PROCESSOR_THREADS_TERMINATION_TIMEOUT_MS);
    }

    // visible for testing
    ProcessorUnit createUnit(int threadId) {
        ThreadScope threadScope = new ThreadScope(scope, threadId);
        ExecutionScheduler scheduler = new ExecutionScheduler(threadScope, rateLimiter);
        ProcessPipeline<?> pipeline = processors.newPipeline(threadScope, scheduler, metrics);
        return new ProcessorUnit(threadScope, pipeline);
    }

    // visible for testing
    static Property<Long> rateProperty(PartitionScope scope) {
        if (scope.isShapingTopic()) {
            return scope.props()
                        .tryGet(PerKeyQuotaConfig.shapingRateProperty(scope.topicPartition().topic()))
                        .orElse(scope.props().get(ProcessorProperties.CONFIG_PER_KEY_QUOTA_PROCESSING_RATE));
        }
        return scope.props().get(ProcessorProperties.CONFIG_PROCESSING_RATE);
    }

    public void addTask(TaskRequest request) {
        int subPartition = subPartitioner.subPartitionFor(request.key());
        units.computeIfAbsent(subPartition, key -> createUnit(subPartition)).putTask(request);
    }

    public void cleanup() {
        for (Entry<Integer, ProcessorUnit> entry : new HashSet<>(units.entrySet())) {
            Integer key = entry.getKey();
            ProcessorUnit unit = entry.getValue();
            if (!unit.hasPendingTasks()) {
                try {
                    units.remove(key).close();
                } catch (Exception e) {
                    logger.warn("Failed to close processor unit of {}", key, e);
                }
            }
        }
    }

    private void destroyThreadProcessor(int i) {
        processors.destroyThreadScope(scope.subscriptionId(), scope.topicPartition(), i);
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        CompletableFuture<Void> shutdownComplete = CompletableFuture.allOf(
                units.keySet().stream().map(i -> {
                    try {
                        return units.get(i).asyncClose()
                                    .completeOnTimeout(null, // TODO: we may wanna log if it has timed out
                                                       processorThreadTerminationTimeoutMillis.value(),
                                                       TimeUnit.MILLISECONDS)
                                    .whenComplete((ignored, e) -> destroyThreadProcessor(i));
                    } catch (RuntimeException e) {
                        logger.error("Processor unit threw exception on shutdown", e);
                        return null;
                    }
                }).filter(Objects::nonNull).toArray(CompletableFuture[]::new));
        try {
            rateLimiter.close();
        } catch (Exception e) {
            logger.error("Error thrown while closing rate limiter", e);
        }

        return shutdownComplete.whenComplete((ignored, ignored2) -> metrics.close());
    }
}
