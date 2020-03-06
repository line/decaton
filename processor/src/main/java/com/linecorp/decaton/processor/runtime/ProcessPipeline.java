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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.DecatonTask;
import com.linecorp.decaton.processor.TaskExtractor;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.ProcessMetrics;
import com.linecorp.decaton.processor.metrics.Metrics.TaskMetrics;
import com.linecorp.decaton.processor.runtime.Utils.Timer;

public class ProcessPipeline<T> implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ProcessPipeline.class);

    private final ThreadScope scope;
    private final List<DecatonProcessor<T>> processors;
    private final DecatonProcessor<byte[]> retryProcessor;
    private final TaskExtractor<T> taskExtractor;
    private final ExecutionScheduler scheduler;
    private final TaskMetrics taskMetrics;
    private final ProcessMetrics processMetrics;

    public ProcessPipeline(ThreadScope scope,
                           List<DecatonProcessor<T>> processors,
                           DecatonProcessor<byte[]> retryProcessor,
                           TaskExtractor<T> taskExtractor,
                           ExecutionScheduler scheduler,
                           Metrics metrics) {
        this.scope = scope;
        this.processors = Collections.unmodifiableList(processors);
        this.retryProcessor = retryProcessor;
        this.taskExtractor = taskExtractor;
        this.scheduler = scheduler;

        taskMetrics = metrics.new TaskMetrics();
        processMetrics = metrics.new ProcessMetrics();
    }

    public CompletableFuture<Void> scheduleThenProcess(TaskRequest request) throws InterruptedException {
        DecatonTask<T> extracted = extract(request);

        scheduler.schedule(extracted.metadata());

        return process(request, extracted);
    }

    // visible for testing
    DecatonTask<T> extract(TaskRequest request) {
        final DecatonTask<T> extracted;
        try {
            extracted = taskExtractor.extract(request.rawRequestBytes());
            if (!validateTask(extracted)) {
                throw new RuntimeException("Invalid task");
            }
        } catch (RuntimeException e) {
            logger.error("Dropping failed to deserialized task for [{}]", request.id(), e);
            taskMetrics.tasksDiscarded.increment();
            throw e;
        }

        request.purgeRawRequestBytes();

        return extracted;
    }

    // visible for testing
    CompletableFuture<Void> process(TaskRequest request, DecatonTask<T> task) throws InterruptedException {
        ProcessingContext<T> context =
                new ProcessingContextImpl<>(scope.subscriptionId(), request, task, processors, retryProcessor);

        Timer timer = Utils.timer();
        final CompletableFuture<Void> processResult;
        final Duration elapsed;
        try (LoggingContext ignored = context.loggingContext()) {
            processResult = context.push(task.taskData());
        } catch (Exception e) {
            taskMetrics.tasksError.increment();
            throw e;
        } finally {
            elapsed = timer.duration();
            if (logger.isTraceEnabled()) {
                logger.trace("processing task of [{}] took {} ns", request.id(), Utils.formatNanos(elapsed));
            }
            taskMetrics.tasksProcessed.increment();
            processMetrics.tasksProcessDuration.record(elapsed);
        }

        return processResult.whenComplete((r, e) -> {
            // Performance logging and metrics update
            Duration completeDuration = timer.duration();
            if (logger.isTraceEnabled()) {
                logger.trace("completing task of [{}] took {} ns",
                             request.id(), Utils.formatNanos(completeDuration));
            }
            processMetrics.tasksCompleteDuration.record(completeDuration);
        });
    }

    private boolean validateTask(DecatonTask<T> task) {
        return task != null &&
               task.metadata() != null &&
               task.taskData() != null &&
               task.taskDataBytes() != null;
    }

    @Override
    public void close() {
        scheduler.close();
    }
}
