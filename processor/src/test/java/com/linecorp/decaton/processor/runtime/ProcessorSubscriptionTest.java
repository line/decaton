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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
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
}
