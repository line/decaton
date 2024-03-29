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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.SubPartitionRuntime;
import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.UsageType;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider.NoopTrace;

@ExtendWith(MockitoExtension.class)
public class PartitionContextTest {
    private static final Property<Integer> MAX_PENDING_RECORDS =
            Property.ofStatic(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS, 10);

    private static PartitionScope scope(String topic, Optional<PerKeyQuotaConfig> perKeyQuotaConfig) {
        return new PartitionScope(
                new SubscriptionScope("subscription", "topic",
                                      SubPartitionRuntime.THREAD_POOL,
                                      Optional.empty(), perKeyQuotaConfig,
                                      ProcessorProperties.builder().set(MAX_PENDING_RECORDS).build(),
                                      NoopTracingProvider.INSTANCE,
                                      ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                                      DefaultSubPartitioner::new),
                new TopicPartition(topic, 0));
    }

    @Mock
    private Processors<?> processors;
    @Mock
    private SubPartitions subPartitions;
    @Mock
    private ConsumerRecord<byte[], byte[]> record;

    @Test
    public void testOffsetWaitingCommit() {
        PartitionContext context = new PartitionContext(scope("topic", Optional.empty()), processors);
        assertFalse(context.offsetWaitingCommit().isPresent());

        OffsetState state = context.registerOffset(100);
        assertFalse(context.offsetWaitingCommit().isPresent());

        state.completion().complete();
        context.updateHighWatermark();
        assertEquals(OptionalLong.of(100), context.offsetWaitingCommit());

        context.updateCommittedOffset(100);
        assertFalse(context.offsetWaitingCommit().isPresent());
    }

    @Test
    public void testShouldPausePartition() {
        PartitionContext context = new PartitionContext(scope("topic", Optional.empty()), processors);
        assertFalse(context.shouldPausePartition());

        // Register MAX-1 records, which should not pause the partition.
        int limit = MAX_PENDING_RECORDS.value();
        List<OffsetState> states = new ArrayList<>();
        for (int i = 0; i < limit - 1; i++) {
            states.add(context.registerOffset(100 + i));
        }
        assertEquals(limit - 1, context.pendingTasksCount());
        assertFalse(context.shouldPausePartition());

        // Register one more record, requiring to pause the partition.
        states.add(context.registerOffset(200));
        assertEquals(limit, context.pendingTasksCount());
        assertTrue(context.shouldPausePartition());

        // Complete the first record, allowing resume.
        states.get(0).completion().complete();
        context.updateHighWatermark();
        assertEquals(limit - 1, context.pendingTasksCount());
        assertFalse(context.shouldPausePartition());

        // Complete all records.
        states.forEach(state -> state.completion().complete());
        context.updateHighWatermark();
        assertEquals(0, context.pendingTasksCount());
        assertFalse(context.shouldPausePartition());
    }

    @Test
    public void testQuotaUsage() {
        PartitionContext context = new PartitionContext(
                scope("topic", Optional.of(PerKeyQuotaConfig.shape())), processors);
        assertEquals(UsageType.COMPLY, context.maybeRecordQuotaUsage(new byte[0]).type());
    }

    @Test
    public void testQuotaUsageWhenDisabled() {
        PartitionContext context = new PartitionContext(
                scope("topic", Optional.empty()), processors);
        assertNull(context.maybeRecordQuotaUsage(new byte[0]));
    }

    @Test
    public void testQuotaUsageNonTargetTopic() {
        PartitionContext context = new PartitionContext(
                scope("topic-shaping", Optional.of(PerKeyQuotaConfig.shape())), processors);
        assertNull(context.maybeRecordQuotaUsage(new byte[0]));

        context = new PartitionContext(
                scope("topic-retry", Optional.of(PerKeyQuotaConfig.shape())), processors);
        assertNull(context.maybeRecordQuotaUsage(new byte[0]));
    }

    @Test
    public void testQuotaApplied() {
        PartitionContext context = new PartitionContext(
                scope("topic-shaping", Optional.of(PerKeyQuotaConfig.shape())),
                processors,
                subPartitions);

        context.addRecord(record, new OffsetState(42L), NoopTrace.INSTANCE, (r, o, q) -> true);
        verify(subPartitions, never()).addTask(any());
    }

    @Test
    public void testQuotaNotApplied() {
        PartitionContext context = new PartitionContext(
                scope("topic-shaping", Optional.of(PerKeyQuotaConfig.shape())),
                processors,
                subPartitions);

        context.addRecord(record, new OffsetState(42L), NoopTrace.INSTANCE, (r, o, q) -> false);
        verify(subPartitions, times(1)).addTask(any());
    }
}
