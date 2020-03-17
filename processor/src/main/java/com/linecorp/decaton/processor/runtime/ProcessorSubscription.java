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

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.ProcessorProperties;
import com.linecorp.decaton.processor.Property;
import com.linecorp.decaton.processor.SubscriptionStateListener;
import com.linecorp.decaton.processor.runtime.Utils.Timer;

public class ProcessorSubscription extends Thread implements AsyncShutdownable {
    private static final Logger logger = LoggerFactory.getLogger(ProcessorSubscription.class);

    private static final long POLL_TIMEOUT_MILLIS = 100;

    private final SubscriptionScope scope;
    private final Supplier<Consumer<String, byte[]>> consumerSupplier;
    private final AtomicBoolean terminated;
    private final BlacklistedKeysFilter blacklistedKeysFilter;
    private final PartitionContexts contexts;
    private final Processors<?> processors;
    private final Property<Long> commitIntervalMillis;
    private final Property<Long> rebalanceTimeoutMillis;
    private final SubscriptionStateListener stateListener;

    public ProcessorSubscription(SubscriptionScope scope,
                                 Supplier<Consumer<String, byte[]>> consumerSupplier,
                                 Processors<?> processors,
                                 ProcessorProperties props,
                                 SubscriptionStateListener stateListener) {
        this.scope = scope;
        this.consumerSupplier = consumerSupplier;
        this.processors = processors;
        this.stateListener = stateListener;
        terminated = new AtomicBoolean();
        contexts = new PartitionContexts(scope, processors);

        blacklistedKeysFilter = new BlacklistedKeysFilter(props);
        commitIntervalMillis = props.get(ProcessorProperties.CONFIG_COMMIT_INTERVAL_MS);
        rebalanceTimeoutMillis = props.get(ProcessorProperties.CONFIG_GROUP_REBALANCE_TIMEOUT_MS);

        setName(String.format("DecatonSubscriptionThread-%s", scope));
    }

    private void updateState(SubscriptionStateListener.State newState) {
        logger.info("ProcessorSubscription transitioned to state: {}", newState);
        if (stateListener != null) {
            try {
                stateListener.onChange(newState);
            } catch (Exception e) {
                logger.warn("State listener threw an exception", e);
            }
        }
    }

    private Set<String> subscribeTopics() {
        return Stream.of(Optional.of(scope.topic()),
                         scope.retryTopic())
                     .filter(Optional::isPresent)
                     .map(Optional::get)
                     .collect(Collectors.toSet());
    }

    private void commitCompletedOffsets(Consumer<?, ?> consumer) {
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = contexts.commitOffsets();
        if (commitOffsets.isEmpty()) {
            logger.debug("No new offsets to commit, skipping commit");
            return;
        }
        logger.debug("Committing offsets: {}", commitOffsets);
        consumer.commitSync(commitOffsets);
        contexts.updateCommittedOffsets(commitOffsets);
    }

    private void waitForRemainingTasksCompletion(long timeoutMillis) {
        final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        Timer timer = Utils.timer();

        while (true) {
            contexts.updateHighWatermarks();

            final int pendingTasksCount = contexts.totalPendingTasks();
            Duration elapsed = timer.duration();
            if (pendingTasksCount == 0) {
                logger.debug("waiting for task completion is successful {} ns \\(^^)/",
                             Utils.formatNanos(elapsed));
                break;
            }

            if (elapsed.toNanos() >= timeoutNanos) {
                logger.debug(
                        "waiting for task completion timed out {} ns. {} tasks are likely to be duplicated",
                        Utils.formatNanos(elapsed), pendingTasksCount);
                break;
            }

            final long timeRemainingMills = TimeUnit.NANOSECONDS.toMillis(timeoutNanos - elapsed.toNanos());
            try {
                Thread.sleep(Math.min(200L, timeRemainingMills));
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void partitionsRevoked(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }

        Timer timer = Utils.timer();
        contexts.dropContexts(partitions);
        logger.info("took {} ms to revoke {} partitions", timer.elapsedMillis(), partitions.size());
    }

    private void partitionsAssigned(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }

        Timer timer = Utils.timer();
        for (TopicPartition partition : partitions) {
            contexts.initContext(partition, false);
        }
        logger.info("took {} ms to assign {} partitions", timer.elapsedMillis(), partitions.size());
    }

    @Override
    public void run() {
        updateState(SubscriptionStateListener.State.INITIALIZING);

        Consumer<String, byte[]> consumer = consumerSupplier.get();

        final Collection<TopicPartition> currentAssignment = new HashSet<>();
        try {
            consumer.subscribe(subscribeTopics(), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    updateState(SubscriptionStateListener.State.REBALANCING);

                    waitForRemainingTasksCompletion(rebalanceTimeoutMillis.value());
                    try {
                        commitCompletedOffsets(consumer);
                    } catch (CommitFailedException | TimeoutException e) {
                        logger.warn("Offset commit failed at group rebalance", e);
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    logger.debug("assigning({}): {}, currently assigned({}): {}", partitions.size(), partitions,
                                 currentAssignment.size(), currentAssignment);

                    final Collection<TopicPartition> revokedPartitions = new HashSet<>(currentAssignment);
                    revokedPartitions.removeAll(partitions);
                    logger.debug("revoked partitions({}): {}", revokedPartitions.size(), revokedPartitions);
                    partitionsRevoked(revokedPartitions);

                    final Collection<TopicPartition> newlyAssignedPartitions = new HashSet<>(partitions);
                    newlyAssignedPartitions.removeAll(currentAssignment);
                    logger.debug("newly assigned partitions({}): {}", newlyAssignedPartitions.size(),
                                 newlyAssignedPartitions);
                    partitionsAssigned(newlyAssignedPartitions);

                    final Collection<TopicPartition> regressed =
                            partitions.stream()
                                      .filter(tp -> contexts.get(tp).isOffsetRegressing(consumer.position(tp)))
                                      .collect(toList());
                    if (!regressed.isEmpty()) {
                        logger.debug("regression on {}, so resetting all internal states", regressed);
                        partitionsRevoked(regressed);
                        partitionsAssigned(regressed);
                    }

                    // Consumer rebalance resets all pause states of assigned partitions even though they
                    // haven't moved over from/to different consumer instance.
                    // Need to re-call pause with originally paused partitions to bring state back consistent.
                    consumer.pause(contexts.pausedPartitions());

                    // Set currentAssignment to latest partitions set.
                    currentAssignment.clear();
                    currentAssignment.addAll(partitions);

                    updateState(SubscriptionStateListener.State.RUNNING);
                }
            });

            long lastCommittedMillis = System.currentTimeMillis();
            while (!terminated.get()) {
                pollOnce(consumer);
                if (System.currentTimeMillis() - lastCommittedMillis >= commitIntervalMillis.value()) {
                    try {
                        commitCompletedOffsets(consumer);
                    } catch (CommitFailedException | TimeoutException e) {
                        logger.warn("Offset commit failed, but continuing to consume", e);
                        // Continue processing, assuming commit will be handled successfully in next attempt.
                    }
                    lastCommittedMillis = System.currentTimeMillis();
                }
            }
        } catch (RuntimeException e) {
            logger.error("ProcessorSubscription {} got exception while consuming, currently assigned: {}",
                         scope, currentAssignment, e);
        } finally {
            updateState(SubscriptionStateListener.State.SHUTTING_DOWN);

            Timer timer = Utils.timer();
            contexts.destroyAllProcessors();
            try {
                commitCompletedOffsets(consumer);
            } catch (RuntimeException e) {
                logger.error("Offset commit failed before closing consumer", e);
            }

            processors.destroySingletonScope(scope.subscriptionId());
            consumer.close();
            logger.info("ProcessorSubscription {} terminated in {} ms", scope,
                        timer.elapsedMillis());

            updateState(SubscriptionStateListener.State.TERMINATED);
        }
    }

    private void pollOnce(Consumer<String, byte[]> consumer) {
        ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT_MILLIS);

        records.forEach(record -> {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            PartitionContext context = contexts.get(tp);
            DeferredCompletion completion = context.registerOffset(record.offset());

            if (blacklistedKeysFilter.shouldTake(record)) {
                TaskRequest taskRequest =
                        new TaskRequest(tp, record.offset(), completion, record.key(), record.value());
                context.addRequest(taskRequest);
            } else {
                completion.complete();
            }
        });
        contexts.updateHighWatermarks();
        contexts.maybeHandlePropertyReload();
        pausePartitions(consumer);
        resumePartitions(consumer);
    }

    private void pausePartitions(Consumer<?, ?> consumer) {
        Collection<TopicPartition> pausedPartitions = contexts.partitionsNeedsPause();
        if (pausedPartitions.isEmpty()) {
            return;
        }

        logger.debug("pausing partitions: {}", pausedPartitions);
        consumer.pause(pausedPartitions);
        pausedPartitions.forEach(tp -> contexts.get(tp).pause());
    }

    private void resumePartitions(Consumer<?, ?> consumer) {
        Collection<TopicPartition> resumedPartitions = contexts.partitionsNeedsResume();
        if (resumedPartitions.isEmpty()) {
            return;
        }

        logger.debug("resuming partitions: {}", resumedPartitions);
        consumer.resume(resumedPartitions);
        resumedPartitions.forEach(tp -> contexts.get(tp).resume());
    }

    @Override
    public void initiateShutdown() {
        logger.info("Initiating shutdown of subscription thread: {}", getName());
        terminated.set(true);
    }

    @Override
    public void awaitShutdown() throws InterruptedException {
        join();
        logger.info("Subscription thread terminated: {}", getName());
    }
}
