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

import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.ProcessorProperties;
import com.linecorp.decaton.processor.Property;
import com.linecorp.decaton.processor.SubscriptionStateListener;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.SubscriptionMetrics;
import com.linecorp.decaton.processor.runtime.Utils.Timer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessorSubscription extends Thread implements AsyncShutdownable {
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
    private final SubscriptionMetrics metrics;
    private boolean asyncCommitInFlight;

    ProcessorSubscription(SubscriptionScope scope,
                          Supplier<Consumer<String, byte[]>> consumerSupplier,
                          Processors<?> processors,
                          ProcessorProperties props,
                          SubscriptionStateListener stateListener,
                          PartitionContexts contexts) {
        this.scope = scope;
        this.consumerSupplier = consumerSupplier;
        this.processors = processors;
        this.stateListener = stateListener;
        this.contexts = contexts;
        terminated = new AtomicBoolean();
        metrics = Metrics.withTags("subscription", scope.subscriptionId()).new SubscriptionMetrics();

        blacklistedKeysFilter = new BlacklistedKeysFilter(props);
        commitIntervalMillis = props.get(ProcessorProperties.CONFIG_COMMIT_INTERVAL_MS);
        rebalanceTimeoutMillis = props.get(ProcessorProperties.CONFIG_GROUP_REBALANCE_TIMEOUT_MS);

        setName(String.format("DecatonSubscriptionThread-%s", scope));
    }

    public ProcessorSubscription(SubscriptionScope scope,
                                 Supplier<Consumer<String, byte[]>> consumerSupplier,
                                 Processors<?> processors,
                                 ProcessorProperties props,
                                 SubscriptionStateListener stateListener) {
        this(scope, consumerSupplier, processors, props, stateListener,
             new PartitionContexts(scope, processors));
    }

    private void updateState(SubscriptionStateListener.State newState) {
        log.info("ProcessorSubscription transitioned to state: {}", newState);
        if (stateListener != null) {
            try {
                stateListener.onChange(newState);
            } catch (Exception e) {
                log.warn("State listener threw an exception", e);
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

    // visible for testing
    void commitCompletedOffsets(Consumer<?, ?> consumer, boolean sync) {
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = contexts.commitOffsets();
        if (commitOffsets.isEmpty()) {
            log.debug("No new offsets to commit, skipping commit");
            return;
        }
        log.debug("Committing offsets {}: {}", sync ? "SYNC" : "ASYNC", commitOffsets);
        if (sync) {
            consumer.commitSync(commitOffsets);
            contexts.updateCommittedOffsets(commitOffsets);
        } else {
            if (asyncCommitInFlight) {
                // We would end up making multiple OffsetCommit requests containing same set of offsets if we proceed
                // another async commit without waiting the first one to complete.
                // This would be harmless if `decaton.commit.interval.ms` is set to sufficiently large, but otherwise
                // we would make abusive offset commits despite there's no progress in processing.
                log.debug("Skipping commit due to another async commit is currently in-flight");
                return;
            }

            Thread pollThread = Thread.currentThread();
            consumer.commitAsync(commitOffsets, (offsets, exception) -> {
                asyncCommitInFlight = false;
                if (exception != null) {
                    log.warn("Offset commit failed asynchronously", exception);
                    return;
                }
                if (Thread.currentThread() != pollThread) {
                    // This isn't expected to happen (see below comment) but we check it with cheap cost
                    // just in case to avoid silent corruption.
                    log.error("BUG: commitAsync callback triggered by an unexpected thread: {}." +
                              " Please report this to Decaton developers", Thread.currentThread());
                    return;
                }

                // Exception raised from the commitAsync callback bubbles up to Consumer.poll() so it kills this
                // subscription thread.
                // Basically the exception thrown from here is considered a bug, but failing to commit offset itself
                // isn't usually critical for consumer's continuation availability so we grab it here widely.
                try {
                    // Some thread and timing safety consideration about below operation.
                    // Thread safety:
                    // Below operation touches some thread-unsafe resources such as ProcessingContexts map and
                    // variable storing committed offset in ProcessingContext but we can assume this callback is
                    // thread safe because in all cases a callback for commitAsync is called from inside of
                    // Consumer.poll() which is called only by this (subscription) thread.
                    // Timing safety:
                    // At the time this callback is triggered there might be a change in ProcessingContexts map
                    // underlying contexts. It would occur in two cases:
                    // 1. When group rebalance is triggered and the context is dropped by revoke at partitionsRevoked().
                    // 2. When dynamic processor reload is triggered and the context is renewed by
                    // PartitionContexts.maybeHandlePropertyReload.
                    // The case 2 is safe (safe to keep updating committed offset in renewed PartitionContext) because
                    // it caries previously consuming offset without reset.
                    // The case 1 is somewhat suspicious but should still be safe, because whenever partition revoke
                    // happens it calls commitSync() through onPartitionsRevoked(). According to the document of
                    // commitAsync(), it is guaranteed that its callback is called before subsequent commitSync()
                    // returns. So when a context is dropped from PartitionContexts, there should be no in-flight
                    // commitAsync().
                    contexts.updateCommittedOffsets(commitOffsets);
                } catch (RuntimeException e) {
                    log.error("Unexpected exception caught while updating committed offset", e);
                }
            });
            asyncCommitInFlight = true;
        }
    }

    private void waitForRemainingTasksCompletion(long timeoutMillis) {
        final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        Timer timer = Utils.timer();

        while (true) {
            contexts.updateHighWatermarks();

            final int pendingTasksCount = contexts.totalPendingTasks();
            Duration elapsed = timer.duration();
            if (pendingTasksCount == 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Waiting for task completion is successful {} ms",
                              Utils.formatNum(elapsed.toMillis()));
                }
                break;
            }

            if (elapsed.toNanos() >= timeoutNanos) {
                log.warn("Timed out waiting {} tasks to complete after {} ms",
                         pendingTasksCount, Utils.formatNum(elapsed.toMillis()));
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
        if (log.isInfoEnabled()) {
            log.info("Processed revoke {} partitions in {} ms",
                     partitions.size(), Utils.formatNum(timer.elapsedMillis()));
        }
    }

    private void partitionsAssigned(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }

        Timer timer = Utils.timer();
        for (TopicPartition partition : partitions) {
            contexts.initContext(partition, false);
        }
        if (log.isInfoEnabled()) {
            log.info("Processed assign {} partitions in {} ms",
                     partitions.size(), Utils.formatNum(timer.elapsedMillis()));
        }
    }

    @Override
    public void run() {
        updateState(SubscriptionStateListener.State.INITIALIZING);

        Consumer<String, byte[]> consumer = consumerSupplier.get();
        AtomicBoolean consumerClosing = new AtomicBoolean(false);
        final Collection<TopicPartition> currentAssignment = new HashSet<>();
        try {
            consumer.subscribe(subscribeTopics(), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // KafkaConsumer#close has been changed to invoke onPartitionRevoked since Kafka 2.4.0.
                    // Since we're doing cleanup procedure on shutdown manually
                    // so just immediately return if consumer is already closing
                    if (consumerClosing.get()) {
                        return;
                    }
                    updateState(SubscriptionStateListener.State.REBALANCING);

                    waitForRemainingTasksCompletion(rebalanceTimeoutMillis.value());
                    try {
                        commitCompletedOffsets(consumer, true);
                    } catch (CommitFailedException | TimeoutException e) {
                        log.warn("Offset commit failed at group rebalance", e);
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    log.debug("Assigning({}): {}, currently assigned({}): {}", partitions.size(), partitions,
                              currentAssignment.size(), currentAssignment);

                    final Collection<TopicPartition> revokedPartitions = new HashSet<>(currentAssignment);
                    revokedPartitions.removeAll(partitions);
                    log.debug("Revoked partitions({}): {}", revokedPartitions.size(), revokedPartitions);
                    partitionsRevoked(revokedPartitions);

                    final Collection<TopicPartition> newlyAssignedPartitions = new HashSet<>(partitions);
                    newlyAssignedPartitions.removeAll(currentAssignment);
                    log.debug("Newly assigned partitions({}): {}", newlyAssignedPartitions.size(),
                              newlyAssignedPartitions);
                    partitionsAssigned(newlyAssignedPartitions);

                    final Collection<TopicPartition> regressed =
                            partitions.stream()
                                      .filter(tp -> contexts.get(tp).isOffsetRegressing(consumer.position(tp)))
                                      .collect(toList());
                    if (!regressed.isEmpty()) {
                        log.debug("Offset regression on {}, so resetting all internal states", regressed);
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
                Timer timer = Utils.timer();
                lastCommittedMillis = commitOffsetsIfNecessary(consumer, lastCommittedMillis);
                metrics.commitOffsetTime.record(timer.duration());
            }
        } catch (RuntimeException e) {
            log.error("ProcessorSubscription {} got exception while consuming, currently assigned: {}",
                      scope, currentAssignment, e);
        } finally {
            updateState(SubscriptionStateListener.State.SHUTTING_DOWN);

            Timer timer = Utils.timer();
            contexts.destroyAllProcessors();

            // Since kafka-clients version 2.4.0 ConsumerRebalanceListener#onPartitionsRevoked gets triggered
            // at close so the same thing is supposed to happen, but its not true for older kafka-clients version
            // so we're manually handling this here instead of relying on rebalance listener for better compatibility
            contexts.updateHighWatermarks();
            try {
                commitCompletedOffsets(consumer, true);
            } catch (RuntimeException e) {
                log.error("Offset commit failed before closing consumer", e);
            }

            processors.destroySingletonScope(scope.subscriptionId());

            consumerClosing.set(true);
            consumer.close();
            log.info("ProcessorSubscription {} terminated in {} ms", scope,
                     timer.elapsedMillis());

            updateState(SubscriptionStateListener.State.TERMINATED);
        }
    }

    private void pollOnce(Consumer<String, byte[]> consumer) {
        Timer timer = Utils.timer();
        ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT_MILLIS);
        metrics.consumerPollTime.record(timer.duration());

        timer = Utils.timer();
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
        metrics.handleRecordsTime.record(timer.duration());

        timer = Utils.timer();
        contexts.maybeHandlePropertyReload();
        metrics.reloadContextsTime.record(timer.duration());

        contexts.updateHighWatermarks();

        timer = Utils.timer();
        pausePartitions(consumer);
        resumePartitions(consumer);
        metrics.handlePausesTime.record(timer.duration());
    }

    private void pausePartitions(Consumer<?, ?> consumer) {
        Collection<TopicPartition> pausedPartitions = contexts.partitionsNeedsPause();
        if (pausedPartitions.isEmpty()) {
            return;
        }

        log.debug("Pausing partitions: {}", pausedPartitions);
        consumer.pause(pausedPartitions);
        pausedPartitions.forEach(tp -> contexts.get(tp).pause());
    }

    private void resumePartitions(Consumer<?, ?> consumer) {
        Collection<TopicPartition> resumedPartitions = contexts.partitionsNeedsResume();
        if (resumedPartitions.isEmpty()) {
            return;
        }

        log.debug("Resuming partitions: {}", resumedPartitions);
        consumer.resume(resumedPartitions);
        resumedPartitions.forEach(tp -> contexts.get(tp).resume());
    }

    private long commitOffsetsIfNecessary(Consumer<String, byte[]> consumer, long lastCommittedMillis) {
        if (System.currentTimeMillis() - lastCommittedMillis >= commitIntervalMillis.value()) {
            try {
                commitCompletedOffsets(consumer, false);
            } catch (CommitFailedException | TimeoutException e) {
                log.warn("Offset commit failed, but continuing to consume", e);
                // Continue processing, assuming commit will be handled successfully in next attempt.
            }
            lastCommittedMillis = System.currentTimeMillis();
        }
        return lastCommittedMillis;
    }

    @Override
    public void initiateShutdown() {
        log.info("Initiating shutdown of subscription thread: {}", getName());
        terminated.set(true);
    }

    @Override
    public void awaitShutdown() throws InterruptedException {
        join();
        metrics.close();
        log.info("Subscription thread terminated: {}", getName());
    }
}
