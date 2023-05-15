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

import static com.linecorp.decaton.processor.runtime.ProcessorProperties.CONFIG_BIND_CLIENT_METRICS;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.TimeoutException;

import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.SubscriptionMetrics;
import com.linecorp.decaton.processor.runtime.internal.AssignmentManager;
import com.linecorp.decaton.processor.runtime.internal.BlacklistedKeysFilter;
import com.linecorp.decaton.processor.runtime.internal.CommitManager;
import com.linecorp.decaton.processor.runtime.internal.ConsumeManager;
import com.linecorp.decaton.processor.runtime.internal.ConsumeManager.ConsumerHandler;
import com.linecorp.decaton.processor.runtime.internal.OffsetRegressionException;
import com.linecorp.decaton.processor.runtime.internal.OffsetState;
import com.linecorp.decaton.processor.runtime.internal.PartitionContext;
import com.linecorp.decaton.processor.runtime.internal.PartitionContexts;
import com.linecorp.decaton.processor.runtime.internal.Processors;
import com.linecorp.decaton.processor.runtime.internal.QuotaApplier;
import com.linecorp.decaton.processor.runtime.internal.SubscriptionScope;
import com.linecorp.decaton.processor.runtime.internal.Utils;
import com.linecorp.decaton.processor.runtime.internal.Utils.Timer;
import com.linecorp.decaton.processor.tracing.TracingProvider;
import com.linecorp.decaton.processor.tracing.TracingProvider.RecordTraceHandle;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessorSubscription extends Thread implements AsyncShutdownable {
    private final SubscriptionScope scope;
    private final BlacklistedKeysFilter blacklistedKeysFilter;
    final PartitionContexts contexts;
    private final Processors<?> processors;
    private final Property<Long> rebalanceTimeoutMillis;
    private final SubscriptionStateListener stateListener;
    private final SubscriptionMetrics metrics;
    private final CommitManager commitManager;
    private final AssignmentManager assignManager;
    private final ConsumeManager consumeManager;
    private final QuotaApplier quotaApplier;
    private final CompletableFuture<Void> loopTerminateFuture;
    private final CompletableFuture<Void> shutdownFuture;
    private volatile boolean started;
    private volatile boolean terminated;

    class Handler implements ConsumerHandler {
        @Override
        public void prepareForRebalance(Collection<TopicPartition> revokingPartitions) {
            updateState(SubscriptionStateListener.State.REBALANCING);

            waitForRemainingTasksCompletion(rebalanceTimeoutMillis.value());
            try {
                commitManager.commitSync();
            } catch (CommitFailedException | TimeoutException | RebalanceInProgressException e) {
                log.warn("Offset commit failed at group rebalance", e);
            } catch (RuntimeException e) {
                // Even when offset commit failed due to unknown reason,
                // we just log error here and don't kill the subscription because
                // we suppose the only problem to do so is more task-duplications after the rebalance,
                // which is not considered as fatal in at-least-once processing Decaton guarantees.
                log.error("Offset commit failed at group rebalance by unexpected reason", e);
            }

            contexts.markRevoking(revokingPartitions);
        }

        @Override
        public void updateAssignment(Collection<TopicPartition> newAssignment) {
            assignManager.assign(newAssignment);

            updateState(SubscriptionStateListener.State.RUNNING);
        }

        @Override
        public void receive(ConsumerRecord<byte[], byte[]> record) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            PartitionContext context = contexts.get(tp);

            OffsetState offsetState;
            try {
                offsetState = context.registerOffset(record.offset());
            } catch (OffsetRegressionException e) {
                log.warn("Offset regression at partition {}", tp);
                assignManager.repair(tp);
                context = contexts.get(tp);
                // If it fails even at 2nd attempt... no idea let it die.
                offsetState = context.registerOffset(record.offset());
            }

            TracingProvider provider = scope.tracingProvider();
            RecordTraceHandle trace = provider.traceFor(record, scope.subscriptionId());
            offsetState.completion().asFuture().whenComplete((unused, throwable) -> {
                try {
                    trace.processingCompletion();
                } catch (Exception e) {
                    log.error("Exception from tracing", e);
                }
            });

            if (blacklistedKeysFilter.shouldTake(record)) {
                context.addRecord(record, offsetState, trace, quotaApplier);
            } else {
                offsetState.completion().complete();
            }
        }
    }

    ProcessorSubscription(SubscriptionScope scope,
                          Consumer<byte[], byte[]> consumer,
                          QuotaApplier quotaApplier,
                          Processors<?> processors,
                          ProcessorProperties props,
                          SubscriptionStateListener stateListener,
                          PartitionContexts contexts) {
        this.scope = scope;
        this.processors = processors;
        this.stateListener = stateListener;
        this.contexts = contexts;
        this.quotaApplier = quotaApplier;
        metrics = Metrics.withTags("subscription", scope.subscriptionId()).new SubscriptionMetrics();

        if (props.get(CONFIG_BIND_CLIENT_METRICS).value()) {
            metrics.bindClientMetrics(consumer);
        }
        consumeManager = new ConsumeManager(consumer, contexts, new Handler(), metrics);
        commitManager = new CommitManager(
                consumer, props.get(ProcessorProperties.CONFIG_COMMIT_INTERVAL_MS), contexts);
        assignManager = new AssignmentManager(contexts);
        blacklistedKeysFilter = new BlacklistedKeysFilter(props);
        rebalanceTimeoutMillis = props.get(ProcessorProperties.CONFIG_GROUP_REBALANCE_TIMEOUT_MS);

        loopTerminateFuture = new CompletableFuture<>();
        shutdownFuture = loopTerminateFuture.whenComplete((unused, throwable) -> cleanUp());

        setName(String.format("DecatonSubscriptionThread-%s", scope));
    }

    public ProcessorSubscription(SubscriptionScope scope,
                                 Consumer<byte[], byte[]> consumer,
                                 QuotaApplier quotaApplier,
                                 Processors<?> processors,
                                 ProcessorProperties props,
                                 SubscriptionStateListener stateListener) {
        this(scope, consumer, quotaApplier, processors, props, stateListener,
             new PartitionContexts(scope, processors));
    }

    private void waitForRemainingTasksCompletion(long timeoutMillis) {
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
                return;
            }

            long remainingMs = timeoutMillis - elapsed.toMillis();
            if (remainingMs <= 0) {
                log.warn("Timed out waiting {} tasks to complete after {} ms",
                         pendingTasksCount, Utils.formatNum(elapsed.toMillis()));
                break;
            }

            try {
                Thread.sleep(Math.min(200L, remainingMs));
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
                break;
            }
        }
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
        return Stream.concat(
                Stream.of(Optional.of(scope.topic()), scope.retryTopic())
                      .filter(Optional::isPresent)
                      .map(Optional::get),
                scope.shapingTopics().stream()).collect(Collectors.toSet());
    }

    @Override
    public void run() {
        started = true;
        try {
            if (!terminated) {
                updateState(SubscriptionStateListener.State.INITIALIZING);
                consumeManager.init(subscribeTopics());
                consumeLoop();
            }
        } finally {
            loopTerminateFuture.complete(null);
        }
    }

    private void consumeLoop() {
        try {
            while (!terminated) {
                consumeManager.poll();

                Timer timer = Utils.timer();
                contexts.maybeHandlePropertyReload();
                metrics.reloadContextsTime.record(timer.duration());

                timer = Utils.timer();
                commitManager.maybeCommitAsync();
                metrics.commitOffsetTime.record(timer.duration());
            }
            updateState(SubscriptionStateListener.State.SHUTTING_DOWN);
            final long timeoutMillis =
                    scope.props().get(ProcessorProperties.CONFIG_SHUTDOWN_TIMEOUT_MS).value();
            if (timeoutMillis > 0) {
                waitForRemainingTasksCompletion(timeoutMillis);
            }
        } catch (RuntimeException e) {
            log.error("Unknown exception thrown at subscription loop, thread will be terminated: {}", scope, e);
            updateState(SubscriptionStateListener.State.SHUTTING_DOWN);
        } finally {
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
            log.info("ProcessorSubscription {} terminated in {} ms", scope, timer.elapsedMillis());
        }
    }

    @Override
    public void initiateShutdown() {
        log.info("Initiating shutdown of subscription thread: {}", getName());
        terminated = true;
        if (!started) {
            loopTerminateFuture.complete(null);
        }
    }

    private void cleanUp() {
        contexts.close();
        consumeManager.close();
        quotaApplier.close();
        metrics.close();
        updateState(SubscriptionStateListener.State.TERMINATED);
    }

    @Override
    public CompletionStage<Void> shutdownFuture() {
        return shutdownFuture;
    }
}
