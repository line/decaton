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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import com.linecorp.decaton.processor.DecatonTask;
import com.linecorp.decaton.processor.ProcessorProperties;
import com.linecorp.decaton.processor.ProcessorsBuilder;
import com.linecorp.decaton.processor.SubscriptionStateListener;
import com.linecorp.decaton.processor.SubscriptionStateListener.State;
import com.linecorp.decaton.processor.TaskMetadata;

public class ProcessorSubscriptionTest {
    /**
     * A mock consumer which triggers rebalance listener in poll()
     * like real KafkaConsumer. (refs: KAFKA-6968)
     */
    private static class DecatonMockConsumer extends MockConsumer<String, byte[]> {
        private boolean needRebalance;
        private ConsumerRebalanceListener listener;
        private Collection<TopicPartition> previousAssignment;

        private DecatonMockConsumer() {
            super(OffsetResetStrategy.LATEST);
        }

        @Override
        public synchronized void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
            this.listener = listener;
            previousAssignment = assignment();
            needRebalance = true;
            super.subscribe(topics, listener);
        }

        @Override
        public synchronized ConsumerRecords<String, byte[]> poll(long timeout) {
            if (needRebalance) {
                listener.onPartitionsRevoked(previousAssignment);
                listener.onPartitionsAssigned(assignment());
                needRebalance = false;
            }
            return super.poll(timeout);
        }
    }

    private static ProcessorSubscription subscription(Consumer<String, byte[]> consumer,
                                                      SubscriptionStateListener listener,
                                                      TopicPartition tp) {
        SubscriptionScope scope = new SubscriptionScope(
                "subscription",
                tp.topic(),
                Optional.empty(),
                ProcessorProperties.builder().build());

        ProcessorSubscription subscription = new ProcessorSubscription(
                scope,
                () -> consumer,
                ProcessorsBuilder.consuming(scope.topic(),
                                            (byte[] bytes) -> new DecatonTask<>(
                                                    TaskMetadata.builder().build(), "dummy", bytes))
                                 .build(null),
                scope.props(),
                listener);
        return subscription;
    }

    @Test(timeout = 10000L)
    public void testStateTransition() throws Exception {
        TopicPartition tp = new TopicPartition("topic", 0);
        Consumer<String, byte[]> consumer = spy(new DecatonMockConsumer());
        List<State> states = Collections.synchronizedList(new ArrayList<>());

        CountDownLatch pollLatch = new CountDownLatch(1);
        doAnswer(inv -> {
            Object records = inv.callRealMethod();
            pollLatch.countDown();
            return records;
        }).when(consumer).poll(anyLong());

        ProcessorSubscription subscription = subscription(consumer, states::add, tp);

        subscription.start();
        pollLatch.await();

        assertEquals(Arrays.asList(State.INITIALIZING,
                                   State.REBALANCING,
                                   State.RUNNING), states);

        subscription.close();
        assertEquals(Arrays.asList(State.INITIALIZING,
                                   State.REBALANCING,
                                   State.RUNNING,
                                   State.SHUTTING_DOWN,
                                   State.TERMINATED), states);
    }
}
