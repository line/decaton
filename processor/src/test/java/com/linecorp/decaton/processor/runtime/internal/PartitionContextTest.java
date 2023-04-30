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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Optional;
import java.util.OptionalLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.UsageType;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider.NoopTrace;

public class PartitionContextTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    private static PartitionScope scope(String topic, Optional<PerKeyQuotaConfig> perKeyQuotaConfig) {
        return new PartitionScope(
                new SubscriptionScope("subscription", "topic",
                                      Optional.empty(), perKeyQuotaConfig, ProcessorProperties.builder().build(),
                                      NoopTracingProvider.INSTANCE,
                                      ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                                      DefaultSubPartitioner::new),
                new TopicPartition(topic, 0));
    }

    @Mock
    private Processors<?> processors;
    @Mock
    private PartitionProcessor partitionProcessor;
    @Mock
    ConsumerRecord<byte[], byte[]> record;

    private static final int MAX_PENDING_RECORDS = 100;

    @Test
    public void testOffsetWaitingCommit() {
        PartitionContext context = new PartitionContext(scope("topic", Optional.empty()), processors, MAX_PENDING_RECORDS);
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
    public void testQuotaUsage() {
        PartitionContext context = new PartitionContext(
                scope("topic", Optional.of(PerKeyQuotaConfig.shape())), processors, MAX_PENDING_RECORDS);
        assertEquals(UsageType.COMPLY, context.maybeRecordQuotaUsage(new byte[0]).type());
    }

    @Test
    public void testQuotaUsageWhenDisabled() {
        PartitionContext context = new PartitionContext(
                scope("topic", Optional.empty()), processors, MAX_PENDING_RECORDS);
        assertNull(context.maybeRecordQuotaUsage(new byte[0]));
    }

    @Test
    public void testQuotaUsageNonTargetTopic() {
        PartitionContext context = new PartitionContext(
                scope("topic-shaping", Optional.of(PerKeyQuotaConfig.shape())), processors, MAX_PENDING_RECORDS);
        assertNull(context.maybeRecordQuotaUsage(new byte[0]));

        context = new PartitionContext(
                scope("topic-retry", Optional.of(PerKeyQuotaConfig.shape())), processors, MAX_PENDING_RECORDS);
        assertNull(context.maybeRecordQuotaUsage(new byte[0]));
    }

    @Test
    public void testQuotaApplied() {
        PartitionContext context = new PartitionContext(
                scope("topic-shaping", Optional.of(PerKeyQuotaConfig.shape())),
                processors,
                partitionProcessor,
                MAX_PENDING_RECORDS);

        context.addRecord(record, new OffsetState(42L), NoopTrace.INSTANCE, (r, o, q) -> true);
        verify(partitionProcessor, never()).addTask(any());
    }

    @Test
    public void testQuotaNotApplied() {
        PartitionContext context = new PartitionContext(
                scope("topic-shaping", Optional.of(PerKeyQuotaConfig.shape())),
                processors,
                partitionProcessor,
                MAX_PENDING_RECORDS);

        context.addRecord(record, new OffsetState(42L), NoopTrace.INSTANCE, (r, o, q) -> false);
        verify(partitionProcessor, times(1)).addTask(any());
    }
}
