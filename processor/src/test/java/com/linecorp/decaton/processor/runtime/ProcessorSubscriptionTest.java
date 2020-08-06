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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.DecatonTask;
import com.linecorp.decaton.processor.ProcessorProperties;
import com.linecorp.decaton.processor.ProcessorsBuilder;
import com.linecorp.decaton.processor.SubscriptionStateListener;
import com.linecorp.decaton.processor.SubscriptionStateListener.State;
import com.linecorp.decaton.processor.TaskMetadata;

public class ProcessorSubscriptionTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    /**
     * A mock consumer which exposes rebalance listener so that can be triggered manually
     * ({@link MockConsumer} doesn't simulate rebalance listener invocation. refs: KAFKA-6968).
     */
    private static class DecatonMockConsumer extends MockConsumer<byte[], byte[]> {
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

    @Mock
    private Consumer<byte[], byte[]> consumerMock;

    @Mock
    private PartitionContexts contextsMock;

    private static SubscriptionScope scope(String topic) {
        return new SubscriptionScope(
                "subscription",
                topic,
                Optional.empty(),
                ProcessorProperties.builder().build());
    }

    private static ProcessorSubscription subscription(Consumer<byte[], byte[]> consumer,
                                                      SubscriptionStateListener listener,
                                                      TopicPartition tp) {
        SubscriptionScope scope = scope(tp.topic());
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

    private ProcessorSubscription subscriptionForCommitTest(BlockingQueue<Runnable> tasks) {
        SubscriptionScope scope = scope("topic");
        ProcessorSubscription subscription = new ProcessorSubscription(
                scope,
                () -> consumerMock,
                null,
                scope.props(),
                null,
                contextsMock) {
            @Override
            public void run() {
                if (tasks == null) {
                    return;
                }
                while (true) {
                    try {
                        tasks.take().run();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        subscription.start();
        return subscription;
    }

    @Test(timeout = 5000)
    public void testCommitCompletedOffsetsSync() {
        ProcessorSubscription subscription = subscriptionForCommitTest(null);
        // When committed ended up successfully update committed offsets
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(
                new TopicPartition("topic", 0), new OffsetAndMetadata(1234, null));
        doReturn(offsets).when(contextsMock).commitOffsets();
        subscription.commitCompletedOffsets(consumerMock, true);
        verify(contextsMock, times(1)).updateCommittedOffsets(offsets);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 5000)
    public void testCommitCompletedOffsetsSync_NO_COMMIT() {
        ProcessorSubscription subscription = subscriptionForCommitTest(null);
        // When target offsets is empty do not attempt any commit
        doReturn(emptyMap()).when(contextsMock).commitOffsets();
        subscription.commitCompletedOffsets(consumerMock, true);
        verify(consumerMock, never()).commitSync(any(Map.class));
        verify(consumerMock, never()).commitAsync(any(Map.class), any());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 5000)
    public void testCommitCompletedOffsetsSync_FAIL() {
        ProcessorSubscription subscription = subscriptionForCommitTest(null);
        // When commit raised an exception do not update committed offsets
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(
                new TopicPartition("topic", 0), new OffsetAndMetadata(1234, null));
        doReturn(offsets).when(contextsMock).commitOffsets();
        doThrow(new RuntimeException("error")).when(consumerMock).commitSync(any(Map.class));
        try {
            subscription.commitCompletedOffsets(consumerMock, true);
        } catch (RuntimeException ignored) {
            // ignore
        }
        verify(contextsMock, never()).updateCommittedOffsets(any());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 5000)
    public void testCommitCompletedOffsetsAsync() throws InterruptedException {
        BlockingQueue<Runnable> tasks = new ArrayBlockingQueue<>(1);
        ProcessorSubscription subscription = subscriptionForCommitTest(tasks);
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(
                new TopicPartition("topic", 0), new OffsetAndMetadata(1234, null));
        doReturn(offsets).when(contextsMock).commitOffsets();
        AtomicReference<OffsetCommitCallback> cbRef = new AtomicReference<>();
        doAnswer(invocation -> {
            OffsetCommitCallback cb = invocation.getArgument(1);
            cbRef.set(cb);
            return null;
        }).when(consumerMock).commitAsync(any(Map.class), any());
        subscription.commitCompletedOffsets(consumerMock, false);

        // Committed offsets should not be updated yet here
        verify(consumerMock, times(1)).commitAsync(any(Map.class), any());
        verify(contextsMock, never()).updateCommittedOffsets(any());

        // Subsequent async commit attempt should be ignored until the in-flight one completes
        subscription.commitCompletedOffsets(consumerMock, false);
        verify(consumerMock, times(1)).commitAsync(any(Map.class), any());
        verify(contextsMock, never()).updateCommittedOffsets(any());

        // Committed offset should be updated once the in-flight request completes
        CountDownLatch latch = new CountDownLatch(1);
        tasks.put(() -> {
            // The callback is not thread-safe and is required to be called from the subscription thread.
            cbRef.get().onComplete(offsets, null);
            latch.countDown();
        });
        latch.await();
        verify(contextsMock, times(1)).updateCommittedOffsets(offsets);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 5000)
    public void testCommitCompletedOffsetsAsync_FAIL() throws InterruptedException {
        BlockingQueue<Runnable> tasks = new ArrayBlockingQueue<>(1);
        ProcessorSubscription subscription = subscriptionForCommitTest(tasks);
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(
                new TopicPartition("topic", 0), new OffsetAndMetadata(1234, null));
        doReturn(offsets).when(contextsMock).commitOffsets();
        AtomicReference<OffsetCommitCallback> cbRef = new AtomicReference<>();
        doAnswer(invocation -> {
            OffsetCommitCallback cb = invocation.getArgument(1);
            cbRef.set(cb);
            return null;
        }).when(consumerMock).commitAsync(any(Map.class), any());
        subscription.commitCompletedOffsets(consumerMock, false);
        // If async commit fails it should never update committed offset
        CountDownLatch latch = new CountDownLatch(1);
        tasks.put(() -> {
            // The callback is not thread-safe and is required to be called from the subscription thread.
            cbRef.get().onComplete(offsets, new RuntimeException("failure"));
            latch.countDown();
        });
        latch.await();
        verify(contextsMock, never()).updateCommittedOffsets(offsets);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 5000)
    public void testCommitCompletedOffsetsAsync_SUBSEQUENT_SYNC() {
        ProcessorSubscription subscription = subscriptionForCommitTest(null);
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(
                new TopicPartition("topic", 0), new OffsetAndMetadata(1234, null));
        doReturn(offsets).when(contextsMock).commitOffsets();

        // No one completes async commit underlying this.
        subscription.commitCompletedOffsets(consumerMock, false);

        // Subsequent sync commit can proceed regardless of in-flight async commit
        subscription.commitCompletedOffsets(consumerMock, true);
        verify(consumerMock, times(1)).commitSync(any(Map.class));
    }
}
