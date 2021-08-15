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

package com.linecorp.decaton.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;
import com.linecorp.decaton.processor.runtime.SubscriptionStateListener;
import com.linecorp.decaton.processor.runtime.SubscriptionStateListener.State;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;
import com.linecorp.decaton.protocol.Sample.HelloTask;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.TestUtils;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class SubscriptionStateTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    @Test(timeout = 30000)
    public void testStateTransition() {
        Map<Integer, List<State>> subscriptionStates = new HashMap<>();
        ProcessorTestSuite
                .builder(rule)
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {}))
                .statesListener((instanceId, newState) -> {
                    synchronized (subscriptionStates) {
                        subscriptionStates.computeIfAbsent(instanceId, key -> new ArrayList<>()).add(newState);
                    }
                })
                .build()
                .run();

        subscriptionStates.forEach((instanceId, stateHistory) -> {
            assertEquals(State.INITIALIZING, stateHistory.get(0));
            assertEquals(State.TERMINATED, stateHistory.get(stateHistory.size() - 1));

            State state;
            Deque<State> states = new ArrayDeque<>(stateHistory);
            List<State> validTransition = Arrays.asList(State.INITIALIZING);
            while ((state = states.pollFirst()) != null) {
                if (!validTransition.contains(state)) {
                    fail(String.format("Invalid state transition %s on subscription-%d", stateHistory, instanceId));
                }
                switch (state) {
                    case INITIALIZING:
                    case REBALANCING:
                        validTransition = Arrays.asList(State.RUNNING);
                        break;
                    case RUNNING:
                        validTransition = Arrays.asList(State.REBALANCING, State.SHUTTING_DOWN);
                        break;
                    case SHUTTING_DOWN:
                        validTransition = Arrays.asList(State.TERMINATED);
                        break;
                    case TERMINATED:
                        // happen when subscription is restarted
                        validTransition = Arrays.asList(State.INITIALIZING);
                        break;
                }
            }
        });
    }

    @Test(timeout = 30000)
    public void testStuckProcessRecoverAfterRevoke() throws Exception {
        Metrics.register(new SimpleMeterRegistry());
        String topic = rule.admin().createRandomTopic(3, 3);

        CountDownLatch processALatch = new CountDownLatch(1);
        CountDownLatch processBLatch = new CountDownLatch(1);

        try (Producer<String, DecatonTaskRequest> producer = TestUtils.producer(rule.bootstrapServers());
             Subscription subA = new Subscription("A", topic, (ctx, task) -> processALatch.await());
             Subscription subB = new Subscription("B", topic, (ctx, task) -> processBLatch.await());
             Subscription subC = new Subscription("C", topic, (ctx, task) -> {})) {

            // Starts 2 subscriptions first.
            // The partition assignment will be subA[0,1], subB[2]
            CountDownLatch subAStartLatch = new CountDownLatch(1);
            CountDownLatch subBStartLatch = new CountDownLatch(1);
            subA.listenerRef.set(state -> { if (state == State.RUNNING) subAStartLatch.countDown(); });
            subB.listenerRef.set(state -> { if (state == State.RUNNING) subBStartLatch.countDown(); });
            subA.subscription.start();
            subB.subscription.start();

            // Produce single task for partition 1 and 2 respectively.
            // This will cause subA and subB's processing to be stuck, then partitions will be paused.
            produceTask(producer, topic, 1);
            produceTask(producer, topic, 2);
            TestUtils.awaitCondition("Partitions should be paused since process is stuck",
                                     () -> subA.partitionPaused(1) && subB.partitionPaused(2));

            CountDownLatch rebalanceLatch = new CountDownLatch(1);
            CountDownLatch runningLatch = new CountDownLatch(1);
            subA.listenerRef.set(state -> {
                switch (state) {
                    case REBALANCING: rebalanceLatch.countDown(); break;
                    case RUNNING: runningLatch.countDown(); break;
                    default: break;
                }
            });

            // Start one more instance subC
            // This will cause another rebalance and partition assignment will be subA[0], subB[1], subC[2]
            subC.subscription.start();

            // Solve subA's processing stuck after waiting rebalance starts.
            rebalanceLatch.await();
            processALatch.countDown();

            // Wait subA to transition to RUNNING state
            runningLatch.await();
        }
    }

    private static void produceTask(Producer<String, DecatonTaskRequest> producer, String topic, int partition) {
        ProducerRecord<String, DecatonTaskRequest> record = new ProducerRecord<>(
                topic, partition, null,
                DecatonTaskRequest
                        .newBuilder()
                        .setSerializedTask(HelloTask.getDefaultInstance().toByteString())
                        .build());
        producer.send(record);
    }

    private static class Subscription implements AutoCloseable {
        private final AtomicReference<SubscriptionStateListener> listenerRef = new AtomicReference<>();
        private final String id;
        private final String topic;
        private final ProcessorSubscription subscription;

        private Subscription(String id,
                             String topic,
                             DecatonProcessor<HelloTask> processor) {
            this.id = id;
            this.topic = topic;
            listenerRef.set(state -> {});

            Properties props = new Properties();
            SubscriptionBuilder builder = new SubscriptionBuilder(id);
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, rule.bootstrapServers());
            props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "test-" + id);
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            builder.consumerConfig(props);
            builder.properties(StaticPropertySupplier.of(
                    Property.ofStatic(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS, 1)));
            builder.stateListener(state -> listenerRef.get().onChange(state));
            builder.processorsBuilder(
                    ProcessorsBuilder.consuming(topic, new ProtocolBuffersDeserializer<>(HelloTask.parser()))
                                     .thenProcess(processor));
            subscription = builder.build();
        }

        boolean partitionPaused(int partition) {
            return Optional.ofNullable(
                    Metrics.registry()
                           .find("decaton.partition.paused")
                           .tags("subscription", id,
                                 "topic", topic,
                                 "partition", String.valueOf(partition))
                           .gauge()).map(Gauge::value).orElse(0.0) > 0;
        }

        @Override
        public void close() throws Exception {
            subscription.close();
        }
    }
}
