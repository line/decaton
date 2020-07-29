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

import java.time.Duration;
import java.util.Collection;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;

import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.ProcessorProperties;
import com.linecorp.decaton.processor.Property;
import com.linecorp.decaton.processor.SubscriptionStateListener;
import com.linecorp.decaton.processor.TracingProvider;
import com.linecorp.decaton.processor.TracingProvider.RecordTraceHandle;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.SubscriptionMetrics;
import com.linecorp.decaton.processor.TracingProvider.TraceHandle;
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
    private final Property<Long> rebalanceTimeoutMillis;
    private final SubscriptionStateListener stateListener;
    private final SubscriptionMetrics metrics;
    private final ProcessorProperties props;

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
        this.props = props;
        terminated = new AtomicBoolean();
        metrics = Metrics.withTags("subscription", scope.subscriptionId()).new SubscriptionMetrics();

        blacklistedKeysFilter = new BlacklistedKeysFilter(props);
        rebalanceTimeoutMillis = props.get(ProcessorProperties.CONFIG_GROUP_REBALANCE_TIMEOUT_MS);

        setName(String.format("DecatonSubscriptionThread-%s", scope));
    }

    public ProcessorSubscription(SubscriptionScope scope,
                                 Supplier<Consumer<String, byte[]>> consumerSupplier,
                                 Processors<?> processors,
                                 ProcessorProperties props,
                                 SubscriptionStateListener stateListener) {
        this(scope, consumerSupplier, processors, props, stateListener, new PartitionContexts(scope, processors));
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

    @Override
    public void run() {
        updateState(SubscriptionStateListener.State.INITIALIZING);

        Consumer<String, byte[]> consumer = consumerSupplier.get();
        AtomicBoolean consumerClosing = new AtomicBoolean(false);
        CommitManager commitManager = new CommitManager(
                consumer, props.get(ProcessorProperties.CONFIG_COMMIT_INTERVAL_MS), contexts);
        AssignmentManager assignManager = new AssignmentManager(contexts);
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
                        commitManager.commitSync();
                    } catch (CommitFailedException | TimeoutException e) {
                        log.warn("Offset commit failed at group rebalance", e);
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    assignManager.assign(partitions);

                    // Consumer rebalance resets all pause states of assigned partitions even though they
                    // haven't moved over from/to different consumer instance.
                    // Need to re-call pause with originally paused partitions to bring state back consistent.
                    consumer.pause(contexts.pausedPartitions());

                    updateState(SubscriptionStateListener.State.RUNNING);
                }
            });

            while (!terminated.get()) {
                pollOnce(consumer, assignManager);
                Timer timer = Utils.timer();
                commitManager.maybeCommitAsync();
                metrics.commitOffsetTime.record(timer.duration());
            }
        } catch (RuntimeException e) {
            log.error("Unknown exception thrown at subscription loop, thread will be terminated: {}", scope, e);
        } finally {
            updateState(SubscriptionStateListener.State.SHUTTING_DOWN);

            Timer timer = Utils.timer();
            contexts.destroyAllProcessors();

            // Since kafka-clients version 2.4.0 ConsumerRebalanceListener#onPartitionsRevoked gets triggered
            // at close so the same thing is supposed to happen, but its not true for older kafka-clients version
            // so we're manually handling this here instead of relying on rebalance listener for better compatibility
            contexts.updateHighWatermarks();
            try {
                commitManager.commitSync();
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

    private void pollOnce(Consumer<String, byte[]> consumer,
                          AssignmentManager assignManager) {
        Timer timer = Utils.timer();
        ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT_MILLIS);
        metrics.consumerPollTime.record(timer.duration());

        timer = Utils.timer();
        records.forEach(record -> {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            PartitionContext context = contexts.get(tp);

            DeferredCompletion offsetCompletion;
            try {
                offsetCompletion = context.registerOffset(record.offset());
            } catch (OffsetRegressionException e) {
                log.warn("Offset regression at partition {}", tp);
                assignManager.repair(tp);
                // If it fails even at 2nd attempt... no idea let it die.
                offsetCompletion = context.registerOffset(record.offset());
            }
            TracingProvider provider = scope.tracingProvider();
            RecordTraceHandle trace = provider.traceFor(record, scope.subscriptionId());
            final DeferredCompletion tracingCompletion = () -> {
                try {
                    trace.processingCompletion();
                } catch (Exception e) {
                    log.error("Exception from tracing", e);
                }
            };
            DeferredCompletion completion = DeferredCompletion.combine(offsetCompletion, tracingCompletion);

            if (blacklistedKeysFilter.shouldTake(record)) {
                TaskRequest taskRequest =
                        new TaskRequest(tp, record.offset(), completion, record.key(),
                                        record.headers(), trace, record.value());
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
