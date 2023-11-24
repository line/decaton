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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.TopicPartition;

import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.ResourceUtilizationMetrics;
import com.linecorp.decaton.processor.runtime.AsyncClosable;
import com.linecorp.decaton.processor.runtime.internal.Utils.Timer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessorUnit implements AsyncClosable {
    private final long id;
    private final ProcessPipeline<?> pipeline;
    private final ExecutorService executor;
    private final ReentrantLock serialExecutionLock;
    private final ResourceUtilizationMetrics metrics;
    private final AtomicInteger pendingTask;

    private volatile boolean terminated;

    public ProcessorUnit(ThreadScope scope, ProcessPipeline<?> pipeline) {
        this.id = scope.threadId();
        this.pipeline = pipeline;


        executor = Executors.newVirtualThreadPerTaskExecutor();
        serialExecutionLock = new ReentrantLock(true);
//        executor = Executors.newSingleThreadExecutor(
//                Utils.namedVirtualThreadFactory("PartitionProcessorThread-" + scope));
//        executor.execute(() -> log.debug("Thread ID MAP {} => {}", Thread.currentThread().threadId(), id));
//        executor = Executors.newSingleThreadExecutor(
//                Utils.namedThreadFactory("PartitionProcessorThread-" + scope));
        pendingTask = new AtomicInteger();
        TopicPartition tp = scope.topicPartition();
        metrics = Metrics.withTags("subscription", scope.subscriptionId(),
                                   "topic", tp.topic(),
                                   "partition", String.valueOf(tp.partition()),
                                   "subpartition", String.valueOf(scope.threadId()))
                .new ResourceUtilizationMetrics();
    }

    public void putTask(TaskRequest request) {
        metrics.tasksQueued.increment();
        executor.execute(() -> {
            serialExecutionLock.lock();
            try {
                processTask(request);
            } finally {
                serialExecutionLock.unlock();
            }
        });
        pendingTask.incrementAndGet();
    }

    private void processTask(TaskRequest request) {
        if (terminated) {
            // There's a chance that some tasks leftover in executor's queue are still attempted to be processed
            // even after this unit enters shutdown sequences.
            // In such case we should ignore all following tasks to quickly complete shutdown sequence.
            return;
        }

        Timer timer = Utils.timer();
        CompletionStage<Void> processCompletion = CompletableFuture.completedFuture(null);
        try {
            processCompletion = pipeline.scheduleThenProcess(request);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("Error while processing request {}. Corresponding offset will be left uncommitted.",
                      request, e);
        } finally {
            // This metric measures the total amount of time the processors were processing tasks including time
            // for scheduling those tasks and is used to refer processor threads utilization, so it needs to measure
            // entire time of schedule and process.
            metrics.processorProcessedTime.record(timer.duration());
        }
        processCompletion.whenComplete((ignored, ignoredE) -> pendingTask.decrementAndGet());
    }

    public boolean hasPendingTasks() {
        return pendingTask.get() > 0;
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        terminated = true;
        pipeline.close();
        CompletableFuture<Void> shutdownComplete = new CompletableFuture<>();
        executor.submit(() -> {
            shutdownComplete.complete(null);
            log.debug("ProcessorUnit {} SHUTDOWN", id);
        });
        executor.shutdown();
        return shutdownComplete.whenComplete((unused, unused2) -> metrics.close());
    }
}
