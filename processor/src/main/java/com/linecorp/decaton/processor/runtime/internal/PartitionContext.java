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

import java.util.Collection;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.PartitionStateMetrics;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.QuotaUsage;
import com.linecorp.decaton.processor.tracing.TracingProvider.RecordTraceHandle;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Represents all states of one partition assigned to this subscription instance.
 */
@Accessors(fluent = true)
public class PartitionContext implements AutoCloseable {
    private final PartitionScope scope;
    private final PartitionProcessor partitionProcessor;
    private final OutOfOrderCommitControl commitControl;
    private final PerKeyQuotaManager perKeyQuotaManager;
    private final Processors<?> processors;
    private final PartitionStateMetrics metrics;

    // The offset committed successfully at last commit
    private volatile long lastCommittedOffset;
    private volatile long pausedTimeNanos;
    private long lastQueueStarvedTime;
    /**
     * Indicates that this context is about to be revoked.
     * This is used to prevent committing or pausing/resuming consumer for to-be-revoked partitions
     * (that throws RuntimeException) before invoking {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)}
     * after {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)} is invoked.
     */
    @Getter
    @Setter
    private boolean revoking;
    private volatile long latestConsumedOffset;

    /**
     * Indicates that if true, reloading is requested and not completed.
     * This is used to perform reloading processing for each partition.
     */
    @Getter
    @Setter
    private volatile boolean reloadRequested;

    public PartitionContext(PartitionScope scope, Processors<?> processors, int maxPendingRecords) {
        this(scope, processors, new PartitionProcessor(scope, processors), maxPendingRecords);
    }

    // visible for testing
    PartitionContext(PartitionScope scope,
                     Processors<?> processors,
                     PartitionProcessor partitionProcessor,
                     int maxPendingRecords) {
        this.scope = scope;
        this.processors = processors;
        this.partitionProcessor = partitionProcessor;

        int capacity = maxPendingRecords + scope.maxPollRecords();
        TopicPartition tp = scope.topicPartition();
        Metrics metricsCtor = Metrics.withTags("subscription", scope.subscriptionId(),
                                               "topic", tp.topic(),
                                               "partition", String.valueOf(tp.partition()));
        OffsetStateReaper offsetStateReaper = new OffsetStateReaper(
                scope.props().get(ProcessorProperties.CONFIG_DEFERRED_COMPLETE_TIMEOUT_MS),
                metricsCtor.new CommitControlMetrics());
        commitControl = new OutOfOrderCommitControl(scope.topicPartition(), capacity, offsetStateReaper);
        if (scope.perKeyQuotaConfig().isPresent() && scope.topic().equals(scope.topicPartition().topic())) {
            perKeyQuotaManager = PerKeyQuotaManager.create(scope);
        } else {
            perKeyQuotaManager = null;
        }

        metrics = metricsCtor.new PartitionStateMetrics(
                commitControl::pendingOffsetsCount, () -> paused() ? 1 : 0,
                () -> lastCommittedOffset, () -> latestConsumedOffset);
        lastCommittedOffset = -1;
        pausedTimeNanos = -1;
        lastQueueStarvedTime = -1;
    }

    /**
     * Returns the largest offset waiting to be committed, if exists.
     *
     * It returns non-empty value with offset which is larger than the offset
     * reported last by {@link #updateCommittedOffset(long)}.
     *
     * @return optional long value representing an offset waiting for commit.
     */
    public OptionalLong offsetWaitingCommit() {
        long readyOffset = commitControl.commitReadyOffset();
        if (readyOffset > lastCommittedOffset) {
            return OptionalLong.of(readyOffset);
        }
        return OptionalLong.empty();
    }

    /**
     * Report the largest offset that has successfully committed into Kafka.
     *
     * @param offset successfully committed offset.
     */
    public void updateCommittedOffset(long offset) {
        lastCommittedOffset = offset;
    }

    public TopicPartition topicPartition() {
        return scope.topicPartition();
    }

    public void updateHighWatermark() {
        commitControl.updateHighWatermark();
        int pendingCount = commitControl.pendingOffsetsCount();
        if (pendingCount == 0 && lastQueueStarvedTime < 0) {
            lastQueueStarvedTime = System.nanoTime();
        }
    }

    public int pendingTasksCount() {
        return commitControl.pendingOffsetsCount();
    }

    public void destroyProcessors() throws Exception {
        partitionProcessor.close();
        processors.destroyPartitionScope(scope.subscriptionId(), scope.topicPartition());
        metrics.close();
    }

    public void addRecord(ConsumerRecord<byte[], byte[]> record,
                          OffsetState offsetState,
                          RecordTraceHandle traceHandle,
                          QuotaApplier quotaApplier) {
        if (!quotaApplier.apply(record, offsetState, maybeRecordQuotaUsage(record.key()))) {
            TaskRequest request = new TaskRequest(
                    scope.topicPartition(), record.offset(), offsetState, record.key(),
                    record.headers(), traceHandle, record.value(), maybeRecordQuotaUsage(record.key()));
            partitionProcessor.addTask(request);
        }

        if (lastQueueStarvedTime > 0) {
            metrics.queueStarvedTime.record(System.nanoTime() - lastQueueStarvedTime, TimeUnit.NANOSECONDS);
            lastQueueStarvedTime = -1;
        }
    }

    public OffsetState registerOffset(long offset) {
        latestConsumedOffset = offset;
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
    }

    public void resume() {
        if (!paused()) {
            return;
        }
        long pausedNanos = System.nanoTime() - pausedTimeNanos;
        pausedTimeNanos = -1;
        metrics.partitionPausedTime.record(pausedNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void close() throws Exception {
        resume();
        commitControl.close();
    }

    // visible for testing
    QuotaUsage maybeRecordQuotaUsage(byte[] key) {
        if (perKeyQuotaManager == null) {
            return null;
        }
        return perKeyQuotaManager.record(key);
    }
}
