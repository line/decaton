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

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import com.linecorp.decaton.processor.Completion;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.LoggingContext;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.metrics.Metrics.TaskMetrics;
import com.linecorp.decaton.processor.runtime.ConsumedRecord;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import com.linecorp.decaton.processor.runtime.internal.Utils.Timer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessPipeline<T> implements AutoCloseable {
    private final PartitionScope scope;
    private final List<DecatonProcessor<T>> processors;
    private final DecatonProcessor<byte[]> retryProcessor;
    private final TaskExtractor<T> taskExtractor;
    private final ExecutionScheduler scheduler;
    private final TaskMetrics metrics;
    private final Clock clock;
    private volatile boolean terminated;

    ProcessPipeline(ThreadScope scope,
                    List<DecatonProcessor<T>> processors,
                    DecatonProcessor<byte[]> retryProcessor,
                    TaskExtractor<T> taskExtractor,
                    ExecutionScheduler scheduler,
                    TaskMetrics metrics,
                    Clock clock) {
        this.scope = scope;
        this.processors = Collections.unmodifiableList(processors);
        this.retryProcessor = retryProcessor;
        this.taskExtractor = taskExtractor;
        this.scheduler = scheduler;
        this.metrics = metrics;
        this.clock = clock;
    }

    public ProcessPipeline(ThreadScope scope,
                           List<DecatonProcessor<T>> processors,
                           DecatonProcessor<byte[]> retryProcessor,
                           TaskExtractor<T> taskExtractor,
                           ExecutionScheduler scheduler,
                           TaskMetrics metrics) {
        this(scope, processors, retryProcessor, taskExtractor, scheduler, metrics, Clock.systemDefaultZone());
    }

    public CompletionStage<Void> scheduleThenProcess(TaskRequest request) throws InterruptedException {
        OffsetState offsetState = request.offsetState();
        final DecatonTask<T> extracted;
        try {
            extracted = extract(request);
        } catch (Exception e) {
            // Catching Exception instead of RuntimeException, since
            // Kotlin-implemented extractor would throw checked exceptions

            // Complete the offset to forward offsets
            offsetState.completion().complete();

            log.error("Dropping not-deserializable task [{}]", request.id(), e);
            metrics.tasksDiscarded.increment();
            return CompletableFuture.completedFuture(null);
        }

        scheduler.schedule(extracted.metadata());
        if (terminated) {
            log.debug("Returning after schedule due to termination");
            return CompletableFuture.completedFuture(null);
        }

        final long now = System.currentTimeMillis();
        if (extracted.metadata().timestampMillis() > 0
                && now - extracted.metadata().timestampMillis() >= 0) {
            metrics.tasksDeliveryLatency.record(
                    now - extracted.metadata().timestampMillis(),
                    TimeUnit.MILLISECONDS
            );
        }
        if (extracted.metadata().scheduledTimeMillis() > 0
                && now - extracted.metadata().scheduledTimeMillis() >= 0) {
            metrics.tasksScheduledDelay.record(
                    now - extracted.metadata().scheduledTimeMillis(),
                    TimeUnit.MILLISECONDS
            );
        }

        Completion result = process(request, extracted);
        offsetState.completion().completeWith(result);
        long timeoutMs = scope.props().get(ProcessorProperties.CONFIG_DEFERRED_COMPLETE_TIMEOUT_MS).value();
        if (timeoutMs >= 0) {
            long expireAt = clock.millis() + timeoutMs;
            offsetState.setTimeout(expireAt);
        }
        return result.asFuture();
    }

    // visible for testing
    DecatonTask<T> extract(TaskRequest request) {
        // This is a workaround to pass the config to TaskExtractor
        // since it doesn't have a reference to ProcessorProperties.
        DefaultTaskExtractor.setParseAsLegacyWhenHeaderMissing(
                scope.props().get(ProcessorProperties.CONFIG_PARSE_AS_LEGACY_FORMAT_WHEN_HEADER_MISSING).value());

        final DecatonTask<T> extracted;
        extracted = taskExtractor.extract(
                ConsumedRecord.builder()
                              .recordTimestamp(request.recordTimestamp())
                              .headers(request.headers())
                              .key(request.key())
                              .value(request.rawRequestBytes())
                              .build());
        if (!validateTask(extracted)) {
            throw new RuntimeException("Invalid task");
        }
        request.purgeRawRequestBytes();

        return extracted;
    }

    // visible for testing
    Completion process(TaskRequest request, DecatonTask<T> task) throws InterruptedException {
        ProcessingContext<T> context =
                new ProcessingContextImpl<>(scope.subscriptionId(), request, task, processors, retryProcessor,
                                            scope.props());

        Timer timer = Utils.timer();
        Completion processResult;
        try (LoggingContext ignored = context.loggingContext()) {
            processResult = context.push(task.taskData());
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            processResult = CompletionImpl.failedCompletion(e);

        } finally {
            Duration elapsed = timer.duration();
            if (log.isTraceEnabled()) {
                log.trace("Processing task [{}] took {} ns", request.id(), Utils.formatNanos(elapsed));
            }
            metrics.tasksProcessed.increment();
            metrics.tasksProcessDuration.record(elapsed);
        }

        processResult.asFuture().whenComplete((r, e) -> {
            // Performance logging and metrics update
            Duration completeDuration = timer.duration();
            if (log.isTraceEnabled()) {
                log.trace("Completing task [{}] took {} ns",
                          request.id(), Utils.formatNanos(completeDuration));
            }
            metrics.tasksCompleteDuration.record(completeDuration);

            if (e != null) {
                log.error("Uncaught exception thrown by processor {} for task {}", scope, request, e);
                metrics.tasksError.increment();
            }
        });
        return processResult;
    }

    private boolean validateTask(DecatonTask<T> task) {
        return task != null &&
               task.metadata() != null &&
               task.taskData() != null &&
               task.taskDataBytes() != null;
    }

    @Override
    public void close() {
        terminated = true;
        scheduler.close();
    }
}
