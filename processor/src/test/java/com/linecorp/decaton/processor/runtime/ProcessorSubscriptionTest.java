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

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.runtime.SubscriptionStateListener.State;
import com.linecorp.decaton.processor.runtime.internal.SubscriptionScope;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;

public class ProcessorSubscriptionTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    Consumer<String, byte[]> consumer;

    /**
     * A mock consumer which exposes rebalance listener so that can be triggered manually
     * ({@link MockConsumer} doesn't simulate rebalance listener invocation. refs: KAFKA-6968).
     */
    private static class DecatonMockConsumer extends MockConsumer<String, byte[]> {
        private volatile ConsumerRebalanceListener rebalanceListener;

        private DecatonMockConsumer() {
            super(OffsetResetStrategy.LATEST);
        }

        @Override
        public synchronized void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
            rebalanceListener = listener;
            super.subscribe(topics, listener);
        }
    }

    private static SubscriptionScope scope(String topic) {
        return new SubscriptionScope(
                "subscription",
                topic,
                Optional.empty(),
                ProcessorProperties.builder().build(),
                NoopTracingProvider.INSTANCE);
    }

    private static ProcessorSubscription subscription(Consumer<String, byte[]> consumer,
                                                      SubscriptionStateListener listener,
                                                      TopicPartition tp,
                                                      DecatonProcessor<String> processor) {
        SubscriptionScope scope = scope(tp.topic());
        ProcessorsBuilder<String> builder =
                ProcessorsBuilder.consuming(scope.topic(),
                                            (byte[] bytes) -> new DecatonTask<>(
                                                    TaskMetadata.builder().build(),
                                                    new String(bytes), bytes));
        if (processor != null) {
            builder.thenProcess(processor);
        }
        return new ProcessorSubscription(
                scope,
                () -> consumer,
                builder.build(null),
                scope.props(),
                listener);
    }

    @Test(timeout = 10000L)
    public void testStateTransition() throws Exception {
        TopicPartition tp = new TopicPartition("topic", 0);
        DecatonMockConsumer consumer = new DecatonMockConsumer();
        List<State> states = Collections.synchronizedList(new ArrayList<>());

        CountDownLatch pollLatch = new CountDownLatch(1);
        consumer.schedulePollTask(() -> {
            consumer.rebalanceListener.onPartitionsAssigned(consumer.assignment());
            pollLatch.countDown();
        });

        ProcessorSubscription subscription = subscription(consumer, states::add, tp, null);

        subscription.start();
        pollLatch.await();

        assertEquals(Arrays.asList(State.INITIALIZING,
                                   State.RUNNING), states);

        subscription.close();
        assertEquals(Arrays.asList(State.INITIALIZING,
                                   State.RUNNING,
                                   State.SHUTTING_DOWN,
                                   State.TERMINATED), states);
    }

    @Test(timeout = 5000)
    public void testOffsetRegression() throws Exception {
        TopicPartition tp = new TopicPartition("topic", 0);
        AtomicReference<ConsumerRebalanceListener> listener = new AtomicReference<>();
        doAnswer(invocation -> {
            listener.set(invocation.getArgument(1));
            return null;
        }).when(consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));

        BlockingQueue<Long> feedOffsets = new ArrayBlockingQueue<>(4);
        feedOffsets.add(100L);
        feedOffsets.add(99L);
        feedOffsets.add(100L);
        feedOffsets.add(101L);
        CountDownLatch processLatch = new CountDownLatch(1);
        ProcessorSubscription subscription = subscription(consumer, ignored -> {}, tp, (context, task) -> {
            if ("101".equals(task)) {
                processLatch.countDown();
            }
        });

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        Answer<?> storeCommitOffsets = invocation -> {
            committedOffsets.putAll(invocation.getArgument(0));
            return null;
        };
        doAnswer(storeCommitOffsets).when(consumer).commitSync(any(Map.class));
        doAnswer(storeCommitOffsets).when(consumer).commitAsync(any(Map.class), any());

        AtomicBoolean first = new AtomicBoolean();
        doAnswer(invocation -> {
            if (first.compareAndSet(false, true)) {
                listener.get().onPartitionsAssigned(singleton(tp));
            }
            Long offset = feedOffsets.poll();
            if (offset != null) {
                return new ConsumerRecords<>(singletonMap(tp, Collections.singletonList(
                        // Feed one record, then a subsequent record of the regressing offset.
                        new ConsumerRecord<>(tp.topic(), tp.partition(), offset, "abc",
                                             String.valueOf(offset).getBytes()))));
            } else {
                Thread.sleep(invocation.getArgument(0));
                return ConsumerRecords.empty();
            }
        }).when(consumer).poll(anyLong());

        subscription.start();
        processLatch.await();
        subscription.close();

        OffsetAndMetadata offset = committedOffsets.get(tp);
        // 101 + 1 is committed when offset=101 is completed.
        assertEquals(102L, offset.offset());
    }
}
