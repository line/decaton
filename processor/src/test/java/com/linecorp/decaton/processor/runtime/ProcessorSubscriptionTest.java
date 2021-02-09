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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.runtime.SubscriptionStateListener.State;
import com.linecorp.decaton.processor.runtime.internal.SubscriptionScope;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;

public class ProcessorSubscriptionTest {
    public static final byte[] NO_DATA = {};

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    Consumer<String, byte[]> consumer;

    @Captor
    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsetsCaptor;

    /**
     * A mock consumer which exposes rebalance listener so that can be triggered manually
     * ({@link MockConsumer} doesn't simulate rebalance listener invocation. refs: KAFKA-6968).
     */
    private static class DecatonMockConsumer extends MockConsumer<String, byte[]> {
        volatile ConsumerRebalanceListener rebalanceListener;

        private DecatonMockConsumer() {
            super(OffsetResetStrategy.LATEST);
        }

        @Override
        public synchronized void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
            rebalanceListener = listener;
            super.subscribe(topics, listener);
        }

        @Override
        public synchronized void close(long timeout, TimeUnit unit) {}
    }

    private static SubscriptionScope scope(String topic, Long waitForProcessingOnClose) {
        return new SubscriptionScope(
                "subscription",
                topic,
                Optional.empty(),
                ProcessorProperties.builder().set(Property.ofStatic(
                        ProcessorProperties.CONFIG_SHUTDOWN_TIMEOUT_MS, waitForProcessingOnClose)).build(),
                NoopTracingProvider.INSTANCE);
    }

    private static ProcessorSubscription subscription(Consumer<String, byte[]> consumer,
                                                      SubscriptionStateListener listener,
                                                      TopicPartition tp) {
        SubscriptionScope scope = scope(tp.topic(), 0L);
        return new ProcessorSubscription(
                scope,
                () -> consumer,
                ProcessorsBuilder.consuming(scope.topic(),
                                            (byte[] bytes) -> new DecatonTask<>(
                                                    TaskMetadata.builder().build(), "dummy", bytes))
                                 .build(null),
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

        ProcessorSubscription subscription = subscription(consumer, states::add, tp);

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

        ProcessorSubscription subscription = subscription(consumer, ignored -> {}, tp);
        AtomicBoolean first = new AtomicBoolean();
        CountDownLatch pollLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            if (first.compareAndSet(false, true)) {
                listener.get().onPartitionsAssigned(singleton(tp));
                return new ConsumerRecords<>(singletonMap(tp, Arrays.asList(
                        // Feed one record, then a subsequent record of the regressing offset.
                        new ConsumerRecord<>(tp.topic(), tp.partition(), 100L, "abc", new byte[0]),
                        new ConsumerRecord<>(tp.topic(), tp.partition(), 99L, "abc", new byte[0]))));
            } else {
                pollLatch.countDown();
                return ConsumerRecords.empty();
            }
        }).when(consumer).poll(anyLong());

        subscription.start();
        pollLatch.await();
        subscription.close();

        verify(consumer, times(1)).commitAsync(offsetsCaptor.capture(), any());
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsCaptor.getValue();
        OffsetAndMetadata offset = offsets.get(tp);
        assertEquals(100L, offset.offset());
    }

    @Test(timeout = 10000L)
    public void testTerminateAsync() throws Exception {
        TopicPartition tp = new TopicPartition("topic", 0);
        DecatonMockConsumer consumer = new DecatonMockConsumer() {
            @Override
            public synchronized ConsumerRecords<String, byte[]> poll(Duration timeout) {
                rebalanceListener.onPartitionsAssigned(assignment());
                return super.poll(timeout);
            }
        };
        consumer.updateEndOffsets(singletonMap(tp, 10L));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch subscribed = new CountDownLatch(1);
        CountDownLatch asyncProcessingStarted = new CountDownLatch(1);
        CountDownLatch letTasksComplete = new CountDownLatch(1);
        DecatonProcessor<String> processor = (context, task) -> {
            final DeferredCompletion completion = context.deferCompletion();
            executor.submit(() -> {
                try {
                    letTasksComplete.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                completion.complete();
            });
            asyncProcessingStarted.countDown();
        };
        SubscriptionScope scope = scope(tp.topic(), 9000L);
        final ProcessorSubscription subscription = new ProcessorSubscription(
                scope,
                () -> consumer,
                ProcessorsBuilder.consuming(scope.topic(),
                                            (byte[] bytes) -> new DecatonTask<>(
                                                    TaskMetadata.builder().build(), "dummy", bytes))
                                 .thenProcess(processor)
                                 .build(null),
                scope.props(),
                newState -> {
                    if (newState == State.RUNNING) {
                        subscribed.countDown();
                    }
                });
        subscription.start();
        subscribed.await();
        consumer.rebalance(singleton(tp));
        consumer.addRecord(new ConsumerRecord<>(tp.topic(), tp.partition(), 10, "", NO_DATA));
        consumer.addRecord(new ConsumerRecord<>(tp.topic(), tp.partition(), 11, "", NO_DATA));
        asyncProcessingStarted.await();
        subscription.initiateShutdown();
        assertTrue(consumer.committed(singleton(tp)).isEmpty());
        letTasksComplete.countDown();
        subscription.awaitShutdown();
        assertEquals(12, consumer.committed(singleton(tp)).get(tp).offset());
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }
}
