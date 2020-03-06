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

import static com.linecorp.decaton.processor.ProcessorProperties.CONFIG_PARTITION_CONCURRENCY;
import static com.linecorp.decaton.processor.ProcessorProperties.CONFIG_PROCESSING_RATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.DynamicProperty;
import com.linecorp.decaton.processor.ProcessorProperties;
import com.linecorp.decaton.processor.Property;

public class PartitionContextsTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    private static final int PENDING_RECORDS_TO_PAUSE = 10;

    private final DynamicProperty<Integer> partitionConcurrencyProperty =
            new DynamicProperty<>(CONFIG_PARTITION_CONCURRENCY);

    private final DynamicProperty<Long> processingRateProp = new DynamicProperty<>(CONFIG_PROCESSING_RATE);

    private final ProcessorProperties props = ProcessorProperties
            .builder()
            .set(Property.ofStatic(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS, PENDING_RECORDS_TO_PAUSE))
            .set(partitionConcurrencyProperty)
            .set(processingRateProp)
            .build();

    private final PartitionScope scope = new PartitionScope(
            new SubscriptionScope("subscription", "topic",
                                  Optional.empty(), props),
            new TopicPartition("topic", 0));

    @Mock
    private Processors<?> processors;

    private PartitionContexts contexts;

    private List<PartitionContext> putContexts(int count) {
        List<PartitionContext> cts = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            TopicPartition tp = new TopicPartition("topic", i);
            PartitionContext context = mock(PartitionContext.class);
            doReturn(tp).when(context).topicPartition();
            doReturn(context).when(contexts).instantiateContext(tp);
            cts.add(contexts.initContext(tp, false));
        }
        return cts;
    }

    @Before
    public void setup() {
        partitionConcurrencyProperty.set(1);
        contexts = spy(new PartitionContexts(scope, processors));
    }

    @Test
    public void testCommittedOffsetsValue() {
        PartitionContext context = putContexts(1).get(0);

        long offset = 1L;
        doReturn(offset).when(context).commitReadyOffset();

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = contexts.commitOffsets();
        assertEquals(offset + 1, committedOffsets.get(context.topicPartition()).offset());
    }

    @Test
    public void testCommittedOffsetsNeverReturnsZero() {
        List<PartitionContext> cts = putContexts(2);

        doReturn(0L).when(cts.get(0)).commitReadyOffset();
        doReturn(0L).when(cts.get(1)).commitReadyOffset();

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = contexts.commitOffsets();
        // No record has been committed so returned map should be empty
        assertTrue(committedOffsets.isEmpty());

        doReturn(1L).when(cts.get(0)).commitReadyOffset();
        committedOffsets = contexts.commitOffsets();
        // No record has been committed for tp1 so the returned map shouldn't contain entry for it.
        assertEquals(1, committedOffsets.size());
        Entry<TopicPartition, OffsetAndMetadata> entry = committedOffsets.entrySet().iterator().next();
        assertEquals(cts.get(0).topicPartition(), entry.getKey());
    }

    @Test
    public void testPartitionsNeedsPause() {
        List<PartitionContext> cts = putContexts(2);

        doReturn(0).when(cts.get(0)).pendingTasksCount();
        doReturn(0).when(cts.get(1)).pendingTasksCount();

        Collection<TopicPartition> needPause = contexts.partitionsNeedsPause();
        assertTrue(needPause.isEmpty());

        // Pause 1 partition.
        doReturn(PENDING_RECORDS_TO_PAUSE + 1).when(cts.get(0)).pendingTasksCount();
        needPause = contexts.partitionsNeedsPause();
        assertEquals(1, needPause.size());
        assertEquals(cts.get(0).topicPartition(), needPause.iterator().next());

        // Mark as paused so it should disappear from the response.
        doReturn(true).when(cts.get(0)).paused();
        needPause = contexts.partitionsNeedsPause();
        assertTrue(needPause.isEmpty());

        // All processing paused by rate limiting.
        // Those which are already paused, should be in response. Others should be all in the response.
        doReturn(true).when(contexts).pausingAllProcessing();
        needPause = contexts.partitionsNeedsPause();
        assertEquals(1, needPause.size());
        assertEquals(cts.get(1).topicPartition(), needPause.iterator().next());

        // Unpause all so nothing should be in the response again.
        doReturn(false).when(contexts).pausingAllProcessing();
        needPause = contexts.partitionsNeedsPause();
        assertTrue(needPause.isEmpty());

        // Resume 1 partition but the task count again exceeding the limit.
        doReturn(false).when(cts.get(0)).paused();
        needPause = contexts.partitionsNeedsPause();
        assertEquals(1, needPause.size());
        assertEquals(cts.get(0).topicPartition(), needPause.iterator().next());
    }

    @Test
    public void testPartitionsNeedsResume() {
        List<PartitionContext> cts = putContexts(2);

        doReturn(0).when(cts.get(0)).pendingTasksCount();
        doReturn(0).when(cts.get(1)).pendingTasksCount();

        Collection<TopicPartition> needsResume = contexts.partitionsNeedsResume();
        assertTrue(needsResume.isEmpty());

        // Pause 1 partition.
        doReturn(PENDING_RECORDS_TO_PAUSE + 1).when(cts.get(0)).pendingTasksCount();
        needsResume = contexts.partitionsNeedsResume();
        assertTrue(needsResume.isEmpty());

        // Mark as paused. Still shouldn't appear in resume list.
        doReturn(true).when(cts.get(0)).paused();
        needsResume = contexts.partitionsNeedsResume();
        assertTrue(needsResume.isEmpty());

        // Task count is now lower than threshold. Should appear in resume response.
        doReturn(PENDING_RECORDS_TO_PAUSE).when(cts.get(0)).pendingTasksCount();
        needsResume = contexts.partitionsNeedsResume();
        assertEquals(1, needsResume.size());
        assertEquals(cts.get(0).topicPartition(), needsResume.iterator().next());

        // All processing paused by rate limiting.
        // All partitions should be disappear from resume response.
        doReturn(true).when(contexts).pausingAllProcessing();
        doReturn(true).when(cts.get(1)).paused();
        needsResume = contexts.partitionsNeedsResume();
        assertTrue(needsResume.isEmpty());

        // All pause finished. Now only partitions should be appear in resume response.
        doReturn(false).when(contexts).pausingAllProcessing();
        needsResume = contexts.partitionsNeedsResume();
        assertEquals(cts.stream().map(PartitionContext::topicPartition).collect(Collectors.toSet()),
                     new HashSet<>(needsResume));

        // Mark as resumed then disappear from the response.
        doReturn(false).when(cts.get(0)).paused();
        needsResume = contexts.partitionsNeedsResume();
        assertEquals(1, needsResume.size());
        assertNotEquals(cts.get(0).topicPartition(), needsResume.iterator().next());
    }

    @Test
    public void testPausingAllProcessing() {
        PartitionContexts context = new PartitionContexts(scope, processors);

        processingRateProp.set(RateLimiter.UNLIMITED);
        assertFalse(context.pausingAllProcessing());

        processingRateProp.set(1L);
        assertFalse(context.pausingAllProcessing());

        processingRateProp.set(RateLimiter.PAUSED);
        assertTrue(context.pausingAllProcessing());
    }

    @Test
    public void testPausingAllProcessingByPropertyReload() {
        assertFalse(contexts.pausingAllProcessing());
        partitionConcurrencyProperty.set(42);
        assertTrue(contexts.pausingAllProcessing());
    }

    @Test
    public void testMaybeHandlePropertyReload() {
        putContexts(12);

        clearInvocations(contexts);

        PartitionContext context = mock(PartitionContext.class);
        doReturn(context).when(contexts).instantiateContext(any());

        // there are some pending tasks
        doReturn(100).when(contexts).totalPendingTasks();

        contexts.maybeHandlePropertyReload();
        // property reload is not requested yet
        verify(contexts, never()).instantiateContext(any());

        partitionConcurrencyProperty.set(42);
        contexts.maybeHandlePropertyReload();
        // property reload is requested, but there are pending tasks
        verify(contexts, never()).instantiateContext(any());

        // pending tasks done
        doReturn(0).when(contexts).totalPendingTasks();
        contexts.maybeHandlePropertyReload();

        verify(contexts, times(12)).instantiateContext(any());
    }
}
