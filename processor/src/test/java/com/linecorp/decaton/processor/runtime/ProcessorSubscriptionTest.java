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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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
import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.runtime.SubscriptionStateListener.State;
import com.linecorp.decaton.processor.runtime.internal.ConsumerSupplier;
import com.linecorp.decaton.processor.runtime.internal.SubscriptionScope;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;

public class ProcessorSubscriptionTest {
    public static final byte[] NO_DATA = {};

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    Consumer<byte[], byte[]> consumer;

    /**
     * A mock consumer which exposes rebalance listener so that can be triggered manually
     * ({@link MockConsumer} doesn't simulate rebalance listener invocation. refs: KAFKA-6968).
     */
    private static class DecatonMockConsumer extends MockConsumer<byte[], byte[]> {
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
        public void close(Duration timeout) {}
    }

    private static SubscriptionScope scope(String topic, long waitForProcessingOnClose) {
        return new SubscriptionScope(
                "subscription",
                topic,
                Optional.empty(),
                ProcessorProperties.builder().set(Property.ofStatic(
                        ProcessorProperties.CONFIG_SHUTDOWN_TIMEOUT_MS, waitForProcessingOnClose)).build(),
                NoopTracingProvider.INSTANCE,
                ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                DefaultSubPartitioner::new);
    }

    private static ProcessorSubscription subscription(Consumer<byte[], byte[]> consumer,
                                                      SubscriptionStateListener listener,
                                                      TopicPartition tp,
                                                      DecatonProcessor<String> processor) {
        SubscriptionScope scope = scope(tp.topic(), 0L);
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
        ProcessorSubscription subscription = subscription(consumer, ignored -> {
        }, tp, (context, task) -> {
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
                        new ConsumerRecord<>(tp.topic(), tp.partition(), offset, "abc".getBytes(StandardCharsets.UTF_8),
                                             String.valueOf(offset).getBytes()))));
            } else {
                Duration timeout = invocation.getArgument(0);
                Thread.sleep(timeout.toMillis());
                return ConsumerRecords.empty();
            }
        }).when(consumer).poll(any());
        doReturn(singleton(tp)).when(consumer).assignment();

        subscription.start();
        processLatch.await();
        subscription.close();

        OffsetAndMetadata offset = committedOffsets.get(tp);
        // 101 + 1 is committed when offset=101 is completed.
        assertEquals(102L, offset.offset());
    }

    @Test(timeout = 10000L)
    public void testTerminateAsync() throws Exception {
        TopicPartition tp = new TopicPartition("topic", 0);
        DecatonMockConsumer consumer = new DecatonMockConsumer() {
            @Override
            public synchronized ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
                rebalanceListener.onPartitionsAssigned(assignment());
                return super.poll(timeout);
            }
        };
        consumer.updateEndOffsets(singletonMap(tp, 10L));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch subscribed = new CountDownLatch(1);
        Semaphore letTaskFinishBlocking = new Semaphore(1, true);
        CountDownLatch asyncProcessingStarted = new CountDownLatch(1);
        CountDownLatch letTasksComplete = new CountDownLatch(1);
        DecatonProcessor<String> processor = (context, task) -> {
            letTaskFinishBlocking.acquire();
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
        // First task finishes synchronous part of processing, starts async processing
        // Second task blocks during synchronous part of processing
        // Third task will be queued behind it
        consumer.addRecord(new ConsumerRecord<>(tp.topic(), tp.partition(), 10, new byte[0], NO_DATA));
        consumer.addRecord(new ConsumerRecord<>(tp.topic(), tp.partition(), 11, new byte[0], NO_DATA));
        consumer.addRecord(new ConsumerRecord<>(tp.topic(), tp.partition(), 12, new byte[0], NO_DATA));
        asyncProcessingStarted.await();
        subscription.initiateShutdown();
        assertTrue(consumer.committed(singleton(tp)).isEmpty());
        assertEquals(3, subscription.contexts.totalPendingTasks());
        letTasksComplete.countDown();
        letTaskFinishBlocking.release(2);
        subscription.awaitShutdown();
        assertEquals(13, consumer.committed(singleton(tp)).get(tp).offset());
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    @Test(timeout = 5000)
    public void closeWithoutStart() throws Exception {
        TopicPartition tp = new TopicPartition("topic", 0);
        ProcessorSubscription subscription = subscription(consumer, null, tp, (context, task) -> {
        });
        // The main point is that the below close returns within timeout.
        subscription.close();
        verify(consumer).close();
    }
}
