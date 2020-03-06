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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.ProcessorProperties;
import com.linecorp.decaton.processor.Property;
import com.linecorp.decaton.processor.runtime.Utils.Task;

public class PartitionContexts {
    private static final Logger logger = LoggerFactory.getLogger(PartitionContexts.class);

    private final SubscriptionScope scope;
    private final Processors<?> processors;
    private final Property<Long> processingRateProp;
    private final int maxPendingRecords;
    private final Map<TopicPartition, PartitionContext> contexts;

    private final AtomicBoolean reloadRequested;

    public PartitionContexts(SubscriptionScope scope, Processors<?> processors) {
        this.scope = scope;
        this.processors = processors;

        processingRateProp = scope.props().get(ProcessorProperties.CONFIG_PROCESSING_RATE);
        // We don't support dynamic reload of this value so fix at the time of boot-up.
        maxPendingRecords = scope.props().get(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS).value();
        contexts = new HashMap<>();
        reloadRequested = new AtomicBoolean(false);

        scope.props().get(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY).listen((oldVal, newVal) -> {
            // This listener will be called at listener registration.
            // It's not necessary to reload contexts at listener registration because PartitionContexts hasn't been instantiated at that time.
            if (oldVal == null) {
                return;
            }

            if (!reloadRequested.getAndSet(true)) {
                logger.info("Requested reload partition.concurrency oldValue={}, newValue={}", oldVal, newVal);
            }
        });
    }

    public PartitionContext get(TopicPartition tp) {
        return contexts.get(tp);
    }

    /**
     * Instantiate new {@link PartitionContext} and put it to contexts
     *
     * @param tp partition to be instantiated
     * @param paused denotes the instantiated partition should be paused
     * @return instantiated context
     */
    public PartitionContext initContext(TopicPartition tp, boolean paused) {
        PartitionContext context = instantiateContext(tp);
        if (paused) {
            context.pause();
        }
        contexts.put(tp, context);
        return context;
    }

    /**
     * Destroy processors for passed partitions and remove those from contexts
     * @param partitions partitions to be destroyed
     */
    public void dropContexts(Collection<TopicPartition> partitions) {
        destroyProcessors(partitions);
        for (TopicPartition tp : partitions) {
            contexts.remove(tp).resume(); // Partition might have been paused to resume to cleanup some states.
        }
    }

    /**
     * Destroy all processors without removing context from contexts
     */
    public void destroyAllProcessors() {
        destroyProcessors(contexts.keySet());
    }

    // visible for testing
    public Map<TopicPartition, OffsetAndMetadata> commitOffsets() {
        return contexts.values().stream()
                       .filter(c -> c.commitReadyOffset() > 0)
                       .collect(Collectors.toMap(PartitionContext::topicPartition,
                                                 // Committing offset tells "the offset I expected to fetch next", so need to add one for the
                                                 // offset that we've finished processing.
                                                 c -> new OffsetAndMetadata(c.commitReadyOffset() + 1, null)));
    }

    public int totalPendingTasks() {
        return contexts.values().stream()
                       .mapToInt(PartitionContext::pendingTasksCount)
                       .sum();
    }

    public void updateHighWatermarks() {
        contexts.values().forEach(PartitionContext::updateHighWatermark);
    }

    private boolean shouldPartitionPaused(int pendingRecords) {
        return pendingRecords > maxPendingRecords;
    }

    // visible for testing
    PartitionContext instantiateContext(TopicPartition tp) {
        PartitionScope partitionScope = new PartitionScope(scope, tp);
        return new PartitionContext(partitionScope, processors, maxPendingRecords);
    }

    // visible for testing
    boolean pausingAllProcessing() {
        return processingRateProp.value() == RateLimiter.PAUSED || reloadRequested.get();
    }

    public Collection<TopicPartition> partitionsNeedsPause() {
        boolean pausingAll = pausingAllProcessing();
        return contexts.values().stream()
                       .filter(c -> !c.paused())
                       .filter(c -> pausingAll || shouldPartitionPaused(c.pendingTasksCount()))
                       .map(PartitionContext::topicPartition)
                       .collect(toList());
    }

    public Collection<TopicPartition> partitionsNeedsResume() {
        boolean pausingAll = pausingAllProcessing();
        return contexts.values().stream()
                       .filter(PartitionContext::paused)
                       .filter(c -> !pausingAll && !shouldPartitionPaused(c.pendingTasksCount()))
                       .map(PartitionContext::topicPartition)
                       .collect(toList());
    }

    public Set<TopicPartition> pausedPartitions() {
        return contexts.values().stream().filter(PartitionContext::paused).map(PartitionContext::topicPartition)
                       .collect(toSet());
    }

    /**
     * Waits for all pending tasks if property-reload is requested, then recreate all partition contexts with latest property values.
     * This method must be called from only subscription thread.
     */
    public void maybeHandlePropertyReload() {
        if (reloadRequested.get()) {
            if (totalPendingTasks() > 0) {
                logger.debug("Waiting pending tasks for property reload.");
                return;
            }
            // it's ok to check-and-set reloadRequested without synchronization
            // because this field is set to false only in this method, and this method is called from only subscription thread.
            reloadRequested.set(false);
            logger.info("Completed waiting pending tasks. Start reloading partition contexts");
            reloadContexts();
        }
    }

    private void reloadContexts() {
        // Save current topicPartitions into copy to update contexts map while iterating over this copy.
        Set<TopicPartition> topicPartitions = new HashSet<>(contexts.keySet());

        logger.info("Start dropping partition contexts");
        dropContexts(topicPartitions);
        logger.info("Finished dropping partition contexts. Start recreating partition contexts");
        for (TopicPartition tp : topicPartitions) {
            initContext(tp, true);
        }
        logger.info("Completed reloading property");
    }

    private void destroyProcessors(Collection<TopicPartition> partitions) {
        Utils.runInParallel("DestroyProcessors",
                            partitions.stream().map(contexts::get).map(context -> (Task) () -> {
                                try {
                                    context.destroyProcessors();
                                } catch (Exception e) {
                                    logger.error("Failed to close partition context for {}", context.topicPartition(), e);
                                }
                            }).collect(toList()))
             .join();
    }
}
