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

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.SubscriptionMetrics;
import com.linecorp.decaton.processor.runtime.internal.ConsumeManager.ConsumerHandler;
import com.linecorp.decaton.processor.runtime.internal.ConsumeManager.PartitionStates;

public class ConsumeManagerTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    static final String TOPIC = "topic";

    @Mock
    Consumer<byte[], byte[]> consumer;

    @Captor
    ArgumentCaptor<ConsumerRecord<byte[], byte[]>> recordsCaptor;

    @Mock
    PartitionStates states;

    @Mock
    ConsumerHandler handler;

    SubscriptionMetrics metrics;

    private ConsumerRebalanceListener rebalanceListener;

    private ConsumeManager consumeManager;

    @Before
    public void setUp() {
        metrics = Metrics.withTags("subscription", "subsc").new SubscriptionMetrics();
        consumeManager = new ConsumeManager(consumer, states, handler, metrics);
        doAnswer(invocation -> {
            rebalanceListener = invocation.getArgument(1);
            return null;
        }).when(consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
        consumeManager.init(singletonList(TOPIC));
    }

    private static TopicPartition tp(int partition) {
        return new TopicPartition(TOPIC, partition);
    }

    private static Set<TopicPartition> tpSet(int... partitions) {
        return Arrays.stream(partitions).mapToObj(p -> new TopicPartition(TOPIC, p)).collect(toSet());
    }

    @Test
    public void poll() {
        byte[] key = { 'k', 'e', 'y' };
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                new ConsumerRecord<>(TOPIC, 1, 100, key, new byte[0]),
                new ConsumerRecord<>(TOPIC, 1, 101, key, new byte[0]),
                new ConsumerRecord<>(TOPIC, 1, 102, key, new byte[0]));
        ConsumerRecords<byte[], byte[]> consumerRecords =
                new ConsumerRecords<>(Collections.singletonMap(new TopicPartition(TOPIC, 1), records));
        doReturn(consumerRecords).when(consumer).poll(any());

        List<TopicPartition> partitionsNeedsPause = new ArrayList<>(Arrays.asList(tp(1)));
        List<TopicPartition> partitionsNeedsResume = new ArrayList<>(Arrays.asList(tp(2)));
        List<TopicPartition> partitionsPaused = new ArrayList<>(Arrays.asList(tp(2)));

        doAnswer(invocation -> {
            partitionsNeedsPause.add(tp(3));
            return null;
        }).when(states).updatePartitionsStatus();
        doReturn(partitionsNeedsPause).when(states).partitionsNeedsPause();
        doReturn(partitionsNeedsResume).when(states).partitionsNeedsResume();
        doAnswer(invocation -> {
            partitionsPaused.addAll(invocation.getArgument(0));
            return null;
        }).when(states).partitionsPaused(any());
        doAnswer(invocation -> {
            partitionsPaused.removeAll(invocation.getArgument(0));
            return null;
        }).when(states).partitionsResumed(any());

        consumeManager.poll();

        // All records were passed to handler's receive
        verify(handler, times(records.size())).receive(recordsCaptor.capture());
        assertEquals(records, recordsCaptor.getAllValues());

        // Pause/resume handling
        verify(consumer, times(1)).pause(Arrays.asList(tp(1), tp(3)));
        verify(consumer, times(1)).resume(Arrays.asList(tp(2)));
        assertEquals(Arrays.asList(tp(1), tp(3)), partitionsPaused);
    }

    @Test
    public void pauseStateHandlingAtRebalance() {
        doReturn(tpSet(1, 2)).when(consumer).paused();
        doAnswer(invocation -> {
            // initial partitions: [1, 2]
            // paused partition: [1, 2]
            // revoked partitions: [2]
            // newly assigned partition: [3]
            rebalanceListener.onPartitionsRevoked(singletonList(tp(2)));
            rebalanceListener.onPartitionsAssigned(Arrays.asList(tp(1), tp(3)));
            return ConsumerRecords.empty();
        }).when(consumer).poll(any());

        Set<TopicPartition> pausedPartitions = new HashSet<>();
        doAnswer(invocation -> {
            pausedPartitions.clear();
            pausedPartitions.addAll(invocation.getArgument(0));
            return null;
        }).when(consumer).pause(any());
        doReturn(new HashSet<>(Arrays.asList(tp(1), tp(3)))).when(consumer).assignment();

        consumeManager.poll();
        // Do not call pause for the revoked partition "2".
        assertEquals(singleton(tp(1)), pausedPartitions);

        doReturn(emptySet()).when(consumer).paused();
        consumeManager.poll();
        assertEquals(emptySet(), pausedPartitions);
    }
}
