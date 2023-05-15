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
import static java.util.stream.Collectors.toList;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.runtime.AsyncShutdownable;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.SubPartitioner;
import com.linecorp.decaton.processor.runtime.internal.Utils.Task;

/**
 * This class is responsible for following portions:
 * - Create and manage configured number of {@link ProcessorUnit}s to parallel-ize processing of tasks received
 *   from single partition.
 * - Route fed task appropriately to one of belonging {@link ProcessorUnit}s, respecting task's key for keeping
 *   process locally and ordering.
 * - Manage lifecycle of {@link DecatonProcessor}s for each {@link ProcessorUnit}s.
 */
public class PartitionProcessor implements AsyncShutdownable {
    private static final Logger logger = LoggerFactory.getLogger(PartitionProcessor.class);

    private final PartitionScope scope;
    private final Processors<?> processors;

    private final List<ProcessorUnit> units;

    private final SubPartitioner subPartitioner;

    // Sharing this limiter object with all processor threads
    // can make processing unfair but it likely gives better overall throughput
    private final RateLimiter rateLimiter;

    private final CompletionStage<Void> shutdownFuture;

    private final Property<Long> processorThreadTerminationTimeoutMillis;

    private Task destroyThreadProcessorTask(int i) {
        return () -> processors.destroyThreadScope(scope.subscriptionId(), scope.topicPartition(), i);
    }

    // Shutdown sequence:
    // 1. ProcessorUnit#initiateShutdown => termination flag turns on, new tasks inflow stops.
    // 2. ProcessorPipeline#close => termination flag turns on, it becomes ready to return immediately for
    // all following tasks that hasn't yet started processing user-supplied logic.
    // 3. ExecutionScheduler#close => terminates all pending schedule waits which are based on metadata.
    // 4. RateLimiter#close => terminates all pending waits for rate limiting, then returns immediately by
    // termination flag at ExecutionScheduler, ProcessorPipeline
    // ===========
    // 5. ProcessorUnit#awaitShutdown => synchronize on executor termination, ensure there's no remaining
    // running tasks.
    // 6. Destroy all thread-scoped processors.
    private CompletableFuture<Void> buildShutdownFuture() {
        return CompletableFuture.allOf(
                units.stream().map(ProcessorUnit::shutdownFuture)
                     .map(CompletionStage::toCompletableFuture)
                     .toArray(CompletableFuture[]::new)).thenCompose(
                v -> Utils.runInParallel("DestroyThreadScopedProcessors",
                                         IntStream.range(0, units.size())
                                                  .mapToObj(this::destroyThreadProcessorTask)
                                                  .collect(toList())));
    }

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
        units = new ArrayList<>(concurrency);
        subPartitioner = scope.subPartitionerSupplier().get(concurrency);
        rateLimiter = new DynamicRateLimiter(rateProperty(scope));
        processorThreadTerminationTimeoutMillis = scope.props()
                                                       .get(CONFIG_PROCESSOR_THREADS_TERMINATION_TIMEOUT_MS);

        try {
            for (int i = 0; i < concurrency; i++) {
                units.add(createUnit(i));
            }
        } catch (RuntimeException e) {
            // If exception occurred in the middle of instantiating processor units, we have to make sure
            // all the previously created units are destroyed before bubbling up the exception.
            initiateShutdown();
            try {
                buildShutdownFuture().get();
            } catch (Exception e1) {
                logger.warn("failed to cleanup intermediate states", e1);
            }
            throw e;
        }

        shutdownFuture = buildShutdownFuture();
    }

    // visible for testing
    ProcessorUnit createUnit(int threadId) {
        ThreadScope threadScope = new ThreadScope(scope, threadId);

        ExecutionScheduler scheduler = new ExecutionScheduler(threadScope, rateLimiter);

        TopicPartition tp = scope.topicPartition();
        Metrics metrics = Metrics.withTags(
                "subscription", scope.subscriptionId(),
                "topic", tp.topic(),
                "partition", String.valueOf(tp.partition()),
                "subpartition", String.valueOf(threadId));

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
        units.get(subPartition).putTask(request);
    }

    @Override
    public void initiateShutdown() {
        for (ProcessorUnit unit : units) {
            try {
                unit.initiateShutdown();
            } catch (RuntimeException e) {
                logger.error("Processor unit threw exception on shutdown", e);
            }
        }
        try {
            rateLimiter.close();
        } catch (Exception e) {
            logger.error("Error thrown while closing rate limiter", e);
        }
    }

    @Override
    public CompletionStage<Void> shutdownFuture() {
        return shutdownFuture;
    }

    @Override
    public void awaitShutdown() throws InterruptedException, ExecutionException {
        try {
            awaitShutdown(Duration.ofMillis(processorThreadTerminationTimeoutMillis.value()));
        } catch (TimeoutException e) {
            logger.warn("awaitShutdown failed due to timeout in {} ms",
                        processorThreadTerminationTimeoutMillis.value(), e);
        }
    }
}
