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

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.Completion;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.ProcessMetrics;
import com.linecorp.decaton.processor.metrics.Metrics.TaskMetrics;
import com.linecorp.decaton.processor.LoggingContext;
import com.linecorp.decaton.processor.runtime.internal.Utils.Timer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessPipeline<T> implements AutoCloseable {
    private final ThreadScope scope;
    private final List<DecatonProcessor<T>> processors;
    private final DecatonProcessor<byte[]> retryProcessor;
    private final TaskExtractor<T> taskExtractor;
    private final ExecutionScheduler scheduler;
    private final TaskMetrics taskMetrics;
    private final ProcessMetrics processMetrics;
    private final Clock clock;
    private volatile boolean terminated;

    ProcessPipeline(ThreadScope scope,
                    List<DecatonProcessor<T>> processors,
                    DecatonProcessor<byte[]> retryProcessor,
                    TaskExtractor<T> taskExtractor,
                    ExecutionScheduler scheduler,
                    Metrics metrics,
                    Clock clock) {
        this.scope = scope;
        this.processors = Collections.unmodifiableList(processors);
        this.retryProcessor = retryProcessor;
        this.taskExtractor = taskExtractor;
        this.scheduler = scheduler;
        this.clock = clock;

        taskMetrics = metrics.new TaskMetrics();
        processMetrics = metrics.new ProcessMetrics();
    }

    public ProcessPipeline(ThreadScope scope,
                           List<DecatonProcessor<T>> processors,
                           DecatonProcessor<byte[]> retryProcessor,
                           TaskExtractor<T> taskExtractor,
                           ExecutionScheduler scheduler,
                           Metrics metrics) {
        this(scope, processors, retryProcessor, taskExtractor, scheduler, metrics, Clock.systemDefaultZone());
    }

    public void scheduleThenProcess(TaskRequest request) throws InterruptedException {
        OffsetState offsetState = request.offsetState();
        final DecatonTask<T> extracted;
        try {
            extracted = extract(request);
        } catch (RuntimeException e) {
            // Complete the offset to forward offsets
            offsetState.completion().complete();

            log.error("Dropping not-deserializable task [{}]", request.id(), e);
            taskMetrics.tasksDiscarded.increment();
            return;
        }

        scheduler.schedule(extracted.metadata());
        if (terminated) {
            log.debug("Returning after schedule due to termination");
            return;
        }

        Completion result = process(request, extracted);
        offsetState.completion().completeWith(result);
        long timeoutMs = scope.props().get(ProcessorProperties.CONFIG_DEFERRED_COMPLETE_TIMEOUT_MS).value();
        if (timeoutMs >= 0) {
            long expireAt = clock.millis() + timeoutMs;
            offsetState.setTimeout(expireAt);
        }
    }

    // visible for testing
    DecatonTask<T> extract(TaskRequest request) {
        final DecatonTask<T> extracted;
        extracted = taskExtractor.extract(request.rawRequestBytes());
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
            taskMetrics.tasksProcessed.increment();
            processMetrics.tasksProcessDuration.record(elapsed);
        }

        processResult.asFuture().whenComplete((r, e) -> {
            // Performance logging and metrics update
            Duration completeDuration = timer.duration();
            if (log.isTraceEnabled()) {
                log.trace("Completing task [{}] took {} ns",
                          request.id(), Utils.formatNanos(completeDuration));
            }
            processMetrics.tasksCompleteDuration.record(completeDuration);

            if (e != null) {
                log.error("Uncaught exception thrown by processor {} for task {}", scope, request, e);
                taskMetrics.tasksError.increment();
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
        processMetrics.close();
        taskMetrics.close();
    }
}
