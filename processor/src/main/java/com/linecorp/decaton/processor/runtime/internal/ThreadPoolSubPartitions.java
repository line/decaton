/*
 * Copyright 2024 LY Corporation
 *
 * LY Corporation licenses this file to you under the Apache License,
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

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.ThreadUtilizationMetrics;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.SubPartitioner;
import com.linecorp.decaton.processor.runtime.internal.Utils.Timer;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class ThreadPoolSubPartitions extends AbstractSubPartitions {
    @AllArgsConstructor
    private static class SubPartition {
        ProcessorUnit unit;
        ThreadUtilizationMetrics threadUtilMetrics;
    }

    private final SubPartitioner subPartitioner;
    private final SubPartition[] subPartitions;

    public ThreadPoolSubPartitions(PartitionScope scope, Processors<?> processors) {
        super(scope, processors);
        // Create units with latest property value.
        //
        // NOTE: If the property value is changed multiple times at short intervals,
        // each partition processor can have different number of units temporarily.
        // But it's not a problem because all partitions will be kept paused until all reload requests done.
        // Let's see this by example:
        //   1. change concurrency from 1 to 5 => start reloading
        //   2. processor 1,2,3 are reloaded with 5
        //   3. change concurrency from 5 to 3 during reloading => request reloading again, so partitions will be kept paused
        //   4. at next subscription loop, all processors are reloaded with 3 again, then start processing
        int concurrency = scope.props().get(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY).value();
        subPartitioner = scope.subPartitionerSupplier().get(concurrency);
        subPartitions = new SubPartition[concurrency];
    }

    @Override
    public void addTask(TaskRequest request) {
        int threadId = subPartitioner.subPartitionFor(request.key());
        SubPartition subPartition = subPartitions[threadId];
        if (subPartition == null) {
            ThreadScope threadScope = new ThreadScope(scope, threadId);
            TopicPartition topicPartition = threadScope.topicPartition();
            ThreadUtilizationMetrics metrics =
                    Metrics.withTags("subscription", threadScope.subscriptionId(),
                                     "topic", topicPartition.topic(),
                                     "partition", String.valueOf(topicPartition.partition()),
                                     "subpartition", String.valueOf(threadId))
                            .new ThreadUtilizationMetrics();
            ExecutorService executor = createExecutorService(threadScope, metrics);
            ProcessorUnit unit = createUnit(threadScope, executor);
            subPartition = subPartitions[threadId] = new SubPartition(unit, metrics);
        }
        subPartition.threadUtilMetrics.tasksQueued.increment();
        subPartition.unit.putTask(request);
    }

    private static ExecutorService createExecutorService(ThreadScope scope, ThreadUtilizationMetrics metrics) {
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                                      new LinkedBlockingQueue<>(),
                                      Utils.namedThreadFactory("PartitionProcessorThread-" + scope)) {
            @Override
            public void execute(Runnable command) {
                Timer timer = Utils.timer();
                try {
                    super.execute(command);
                } finally {
                    metrics.processorProcessedTime.record(timer.duration());
                }
            }
        };
    }

    @Override
    public void cleanup() {
        // noop
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        return asyncClose(CompletableFuture.allOf(
                Arrays.stream(subPartitions)
                      .filter(Objects::nonNull)
                      .map(subPartition -> closeUnitAsync(subPartition.unit).whenComplete(
                              (ignored, ignoredE) -> subPartition.threadUtilMetrics.close()))
                      .toArray(CompletableFuture[]::new)));
    }
}
