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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;

import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.ResourceUtilizationMetrics;
import com.linecorp.decaton.processor.runtime.Utils.Timer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessorUnit implements AsyncShutdownable {
    private final ProcessPipeline<?> pipeline;
    private final ExecutorService executor;

    private final ResourceUtilizationMetrics metrics;

    private volatile boolean terminated;

    public ProcessorUnit(ThreadScope scope, ProcessPipeline<?> pipeline) {
        this.pipeline = pipeline;

        executor = Executors.newSingleThreadExecutor(
                Utils.namedThreadFactory("PartitionProcessorThread-" + scope));
        TopicPartition tp = scope.topicPartition();
        metrics = Metrics.withTags("subscription", scope.subscriptionId(),
                                   "topic", tp.topic(),
                                   "partition", String.valueOf(tp.partition()),
                                   "subpartition", String.valueOf(scope.threadId()))
                .new ResourceUtilizationMetrics();
    }

    public void putTask(TaskRequest request) {
        metrics.tasksQueued.increment();
        executor.execute(() -> processTask(request));
    }

    private void processTask(TaskRequest request) {
        if (terminated) {
            // There's a chance that some tasks leftover in executor's queue are still attempted to be processed
            // even after this unit enters shutdown sequences.
            // In such case we should ignore all following tasks to quickly complete shutdown sequence.
            return;
        }

        Timer timer = Utils.timer();
        try {
            pipeline.scheduleThenProcess(request);
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
    }

    @Override
    public void initiateShutdown() {
        terminated = true;
        pipeline.close();
        executor.shutdown();
    }

    @Override
    public void awaitShutdown() throws InterruptedException {
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        metrics.close();
    }
}
