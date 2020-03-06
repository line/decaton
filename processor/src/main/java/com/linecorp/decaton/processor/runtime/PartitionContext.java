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

import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;

import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.PartitionStateMetrics;

/**
 * Represents all states of one partition assigned to this subscription instance.
 */
public class PartitionContext {
    private final PartitionScope scope;
    private final PartitionProcessor partitionProcessor;
    private final OutOfOrderCommitControl commitControl;
    private final Processors<?> processors;
    private final PartitionStateMetrics metrics;

    private long pausedTimeNanos;

    public PartitionContext(PartitionScope scope, Processors<?> processors, int maxPendingRecords) {
        this.scope = scope;
        this.processors = processors;
        partitionProcessor = new PartitionProcessor(scope, processors);

        int capacity = maxPendingRecords + ConsumerSupplier.MAX_MAX_POLL_RECORDS;
        commitControl = new OutOfOrderCommitControl(scope.topicPartition(), capacity);

        TopicPartition tp = scope.topicPartition();
        metrics = Metrics.withTags("subscription", scope.subscriptionId(),
                                   "topic", tp.topic(),
                                   "partition", String.valueOf(tp.partition()))
                .new PartitionStateMetrics();

        pausedTimeNanos = -1;
    }

    public long commitReadyOffset() {
        return commitControl.commitReadyOffset();
    }

    public TopicPartition topicPartition() {
        return scope.topicPartition();
    }

    public void updateHighWatermark() {
        commitControl.updateHighWatermark();
        metrics.tasksPending.set(commitControl.pendingOffsetsCount());
    }

    public int pendingTasksCount() {
        return commitControl.pendingOffsetsCount();
    }

    public void destroyProcessors() throws Exception {
        partitionProcessor.close();
        processors.destroyPartitionScope(scope.subscriptionId(), scope.topicPartition());
    }

    public boolean isOffsetRegressing(long offset) {
        return commitControl.isRegressing(offset);
    }

    public void addRequest(TaskRequest request) {
        partitionProcessor.addTask(request);
    }

    public DeferredCompletion registerOffset(long offset) {
        return commitControl.reportFetchedOffset(offset);
    }

    public boolean paused() {
        return pausedTimeNanos >= 0;
    }

    public void pause() {
        if (paused()) {
            return;
        }
        pausedTimeNanos = System.nanoTime();
        metrics.partitionsPaused.increment();
    }

    public void resume() {
        if (!paused()) {
            return;
        }
        long pausedNanos = System.nanoTime() - pausedTimeNanos;
        pausedTimeNanos = -1;
        metrics.partitionPausedTime.record(pausedNanos, TimeUnit.NANOSECONDS);
        metrics.partitionsPaused.decrement();
    }
}
