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

import static com.linecorp.decaton.processor.runtime.ProcessorProperties.CONFIG_PARTITION_CONCURRENCY;
import static com.linecorp.decaton.processor.runtime.ProcessorProperties.CONFIG_PROCESSING_RATE;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.DynamicProperty;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;

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
                                  Optional.empty(), Optional.empty(), props, NoopTracingProvider.INSTANCE,
                                  ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                                  DefaultSubPartitioner::new),
            new TopicPartition("topic", 0));

    @Mock
    private Processors<?> processors;

    private PartitionContexts contexts;

    private List<PartitionContext> putContexts(int count) {
        List<PartitionContext> cts = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            TopicPartition tp = tp(i);
            PartitionContext context = mock(PartitionContext.class);
            doReturn(tp).when(context).topicPartition();
            doReturn(context).when(contexts).instantiateContext(tp);
            cts.add(contexts.initContext(tp, false));
        }
        return cts;
    }

    private static TopicPartition tp(int partition) {
        return new TopicPartition("topic", partition);
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
        doReturn(OptionalLong.of(offset)).when(context).offsetWaitingCommit();

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = contexts.commitReadyOffsets();
        assertEquals(offset + 1, committedOffsets.get(context.topicPartition()).offset());
    }

    @Test
    public void testCommittedOffsetsNeverReturnsZero() {
        List<PartitionContext> cts = putContexts(2);

        doReturn(OptionalLong.empty()).when(cts.get(0)).offsetWaitingCommit();
        doReturn(OptionalLong.empty()).when(cts.get(1)).offsetWaitingCommit();

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = contexts.commitReadyOffsets();
        // No record has been committed so returned map should be empty
        assertTrue(committedOffsets.isEmpty());

        doReturn(OptionalLong.of(1)).when(cts.get(0)).offsetWaitingCommit();
        committedOffsets = contexts.commitReadyOffsets();
        // No record has been committed for tp1 so the returned map shouldn't contain entry for it.
        assertEquals(1, committedOffsets.size());
        Entry<TopicPartition, OffsetAndMetadata> entry = committedOffsets.entrySet().iterator().next();
        assertEquals(cts.get(0).topicPartition(), entry.getKey());
    }

    @Test
    public void testUpdateCommittedOffset() {
        List<PartitionContext> ctxs = putContexts(2);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(ctxs.get(0).topicPartition(), new OffsetAndMetadata(101));
        offsets.put(ctxs.get(1).topicPartition(), new OffsetAndMetadata(201));

        contexts.storeCommittedOffsets(offsets);

        // PartitionContext manages their "completed" offset so its minus 1 from committed offset
        // which indicates the offset to "fetch next".
        verify(ctxs.get(0), times(1)).updateCommittedOffset(100);
        verify(ctxs.get(1), times(1)).updateCommittedOffset(200);
    }

    @Test
    public void testPartitionsNeedsPause() {
        List<PartitionContext> cts = putContexts(2);

        doReturn(0).when(cts.get(0)).pendingTasksCount();
        doReturn(0).when(cts.get(1)).pendingTasksCount();
        doReturn(false).when(cts.get(0)).reloadRequested();
        doReturn(false).when(cts.get(1)).reloadRequested();

        Collection<TopicPartition> needPause = contexts.partitionsNeedsPause();
        assertTrue(needPause.isEmpty());

        // Pause all partitions by reloading
        partitionConcurrencyProperty.set(42);
        doReturn(true).when(cts.get(0)).reloadRequested();
        doReturn(true).when(cts.get(1)).reloadRequested();
        needPause = contexts.partitionsNeedsPause();
        assertEquals(2, needPause.size());

        // Resume 1 partition by finishing reloading
        doReturn(false).when(cts.get(0)).reloadRequested();
        needPause = contexts.partitionsNeedsPause();
        assertEquals(1, needPause.size());
        assertEquals(cts.get(1).topicPartition(), needPause.iterator().next());

        // Resume all partitions by finishing reloading
        doReturn(false).when(cts.get(1)).reloadRequested();
        needPause = contexts.partitionsNeedsPause();
        assertTrue(needPause.isEmpty());

        // Pause 1 partition.
        doReturn(PENDING_RECORDS_TO_PAUSE).when(cts.get(0)).pendingTasksCount();
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
        doReturn(PENDING_RECORDS_TO_PAUSE).when(cts.get(0)).pendingTasksCount();
        needsResume = contexts.partitionsNeedsResume();
        assertTrue(needsResume.isEmpty());

        // Mark as paused. Still shouldn't appear in resume list.
        doReturn(true).when(cts.get(0)).paused();
        needsResume = contexts.partitionsNeedsResume();
        assertTrue(needsResume.isEmpty());

        // Task count is now lower than threshold. Should appear in resume response.
        doReturn(PENDING_RECORDS_TO_PAUSE - 1).when(cts.get(0)).pendingTasksCount();
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
    public void testShouldNotBePausingAllProcessingByPropertyReload() {
        assertFalse(contexts.pausingAllProcessing());
        partitionConcurrencyProperty.set(42);
        assertFalse(contexts.pausingAllProcessing());
    }

    @Test
    public void testMaybeHandlePropertyReload() {
        int count = 12;
        List<PartitionContext> allContexts = putContexts(count);
        List<PartitionContext> pendingContexts = new ArrayList<>();
        List<PartitionContext> reloadableContexts = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            PartitionContext context = allContexts.get(i);
            if (i % 3 == 0) {
                doReturn(100).when(context).pendingTasksCount();
                pendingContexts.add(context);
            } else {
                doReturn(0).when(context).pendingTasksCount();
                reloadableContexts.add(context);
            }
        }
        clearInvocations(contexts);

        contexts.maybeHandlePropertyReload();
        // property reload is not requested yet
        verify(contexts, never()).instantiateContext(any());

        partitionConcurrencyProperty.set(42);
        for (PartitionContext context: allContexts) {
            doReturn(true).when(context).reloadRequested();
        }
        contexts.maybeHandlePropertyReload();

        // property reload is requested, but there are pending tasks
        verify(contexts, times(reloadableContexts.size())).instantiateContext(any());
        for (PartitionContext context: reloadableContexts) {
            doReturn(false).when(context).reloadRequested();
        }

        for (PartitionContext context: pendingContexts) {
            doReturn(0).when(context).pendingTasksCount();
        }
        contexts.maybeHandlePropertyReload();
        // completed reloading request
        verify(contexts, times(count)).instantiateContext(any());
    }

    @Test
    public void testMarkRevoking() {
        putContexts(2);
        contexts.markRevoking(asList(tp(0)));
        verify(contexts.get(tp(0)), times(1)).revoking(true);
        verify(contexts.get(tp(1)), never()).revoking(anyBoolean());

        contexts.unmarkRevoking(asList(tp(0)));
        verify(contexts.get(tp(0)), times(1)).revoking(false);
        verify(contexts.get(tp(1)), never()).revoking(anyBoolean());
    }

    @Test
    public void testPauseResumeOnlyNonRevoking() {
        List<PartitionContext> cts = putContexts(3);
        // mark revoking
        doReturn(true).when(cts.get(0)).revoking();

        // pausing all partitions, but revoking partition should be excluded
        doReturn(true).when(contexts).pausingAllProcessing();
        List<TopicPartition> partitions = contexts.partitionsNeedsPause();
        assertEquals(new HashSet<>(asList(tp(1), tp(2))), new HashSet<>(partitions));

        for (PartitionContext c : cts) {
            doReturn(true).when(c).paused();
        }
        // resuming all partitions, but revoking partition should be excluded
        doReturn(false).when(contexts).pausingAllProcessing();
        partitions = contexts.partitionsNeedsResume();
        assertEquals(new HashSet<>(asList(tp(1), tp(2))), new HashSet<>(partitions));

        // unmark revoking
        doReturn(false).when(cts.get(0)).revoking();

        // unmarked partition should be included
        partitions = contexts.partitionsNeedsResume();
        assertEquals(new HashSet<>(asList(tp(0), tp(1), tp(2))), new HashSet<>(partitions));
    }

    @Test
    public void testCommitReadyOnlyNonRevoking() {
        List<PartitionContext> cts = putContexts(3);
        // mark revoking
        doReturn(true).when(cts.get(0)).revoking();

        doReturn(OptionalLong.of(0)).when(cts.get(0)).offsetWaitingCommit();
        doReturn(OptionalLong.of(1)).when(cts.get(1)).offsetWaitingCommit();
        doReturn(OptionalLong.empty()).when(cts.get(2)).offsetWaitingCommit();
        Map<TopicPartition, OffsetAndMetadata> readyOffsets = contexts.commitReadyOffsets();

        assertEquals(1, readyOffsets.size());
        assertEquals(2, readyOffsets.get(tp(1)).offset());

        // unmark revoking
        doReturn(false).when(cts.get(0)).revoking();
        doReturn(OptionalLong.empty()).when(cts.get(1)).offsetWaitingCommit();
        readyOffsets = contexts.commitReadyOffsets();
        assertEquals(1, readyOffsets.size());
        assertEquals(1, readyOffsets.get(tp(0)).offset());
    }
}
