/*
 * Copyright 2024 LY Corporation
 *
 * LY Corporation licenses this file to you under the Apache License,
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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.PerPartitionMetrics;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;

import lombok.extern.slf4j.Slf4j;

/**
 * This class is responsible for following portions:
 * - Create and manage configured number of {@link ProcessorUnit}s to parallel-ize processing of tasks received
 *   from single partition.
 * - Route fed task appropriately to one of belonging {@link ProcessorUnit}s, respecting task's key for keeping
 *   process locally and ordering.
 * - Manage lifecycle of {@link DecatonProcessor}s for each {@link ProcessorUnit}s.
 */
@Slf4j
public abstract class AbstractSubPartitions implements SubPartitions {
    private static final Object TIMEOUT_INDICATOR = new Object();

    protected final PartitionScope scope;
    protected final Processors<?> processors;
    private final Property<Long> shutdownTimeoutMillis;
    // Sharing this limiter object with all processor threads
    // can make processing unfair but it likely gives better overall throughput
    protected final RateLimiter rateLimiter;
    protected final PerPartitionMetrics metrics;

    public AbstractSubPartitions(PartitionScope scope, Processors<?> processors) {
        this.scope = scope;
        this.processors = processors;
        shutdownTimeoutMillis = scope.props().get(
                ProcessorProperties.CONFIG_PROCESSOR_THREADS_TERMINATION_TIMEOUT_MS);
        rateLimiter = new DynamicRateLimiter(rateProperty(scope));
        metrics = Metrics.withTags(
                "subscription", scope.subscriptionId(),
                "topic", scope.topic(),
                "partition", String.valueOf(scope.topicPartition().partition()))
                .new PerPartitionMetrics();
    }

    // visible for testing
    ProcessorUnit createUnit(int threadId, ExecutorService executor) {
        ThreadScope threadScope = new ThreadScope(scope, threadId);
        ExecutionScheduler scheduler = new ExecutionScheduler(threadScope, rateLimiter);
        ProcessPipeline<?> pipeline = processors.newPipeline(threadScope, scheduler, metrics);
        return new ProcessorUnit(threadScope, pipeline, executor);
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

    protected void destroyThreadProcessor(int threadId) {
        processors.destroyThreadScope(scope.subscriptionId(), scope.topicPartition(), threadId);
    }

    protected CompletableFuture<Void> closeProcessorUnitsAsync(Stream<ProcessorUnit> units) {
        return CompletableFuture.allOf(
                units.map(unit -> {
                    try {
                        return unit.asyncClose()
                                              .thenApply(ignored -> null) // To migrate type from Void to Object
                                              .completeOnTimeout(TIMEOUT_INDICATOR,
                                                                 shutdownTimeoutMillis.value(),
                                                                 TimeUnit.MILLISECONDS)
                                              .whenComplete((indicator, e) -> {
                                                  if (indicator == TIMEOUT_INDICATOR) {
                                                      log.error("Processor unit termination timed out {}", unit.id());
                                                  }
                                                  destroyThreadProcessor(unit.id());
                                              });
                    } catch (RuntimeException e) {
                        log.error("Processor unit threw exception on shutdown", e);
                        return null;
                    }
                }).filter(Objects::nonNull).toArray(CompletableFuture[]::new));
    }

    public CompletableFuture<Void> asyncClose(CompletableFuture<Void> unitsShutdown) {
        try {
            rateLimiter.close();
        } catch (Exception e) {
            log.error("Error thrown while closing rate limiter", e);
        }

        return unitsShutdown.whenComplete((ignored, ignored2) -> metrics.close());
    }
}
