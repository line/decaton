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

package com.linecorp.decaton.testing.processor;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import com.google.protobuf.ByteString;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessorProperties;
import com.linecorp.decaton.processor.ProcessorsBuilder;
import com.linecorp.decaton.processor.PropertySupplier;
import com.linecorp.decaton.processor.SubscriptionStateListener;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;
import com.linecorp.decaton.protocol.Decaton.TaskMetadataProto;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.TestUtils;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee.GuaranteeType;
import com.linecorp.decaton.testing.processor.TestTask.TestTaskDeserializer;
import com.linecorp.decaton.testing.processor.TestTask.TestTaskSerializer;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Test suite that checks Decaton's core functionality, especially for processing semantics.
 *
 * Test suite will perform following procedure:
 *   1. Start multiple subscription instances
 *   2. Produce tasks
 *   3. When half of tasks are processed, restart subscriptions one by one
 *   4. Await all produced offsets are committed
 *   5. Assert processing semantics
 *
 * Expected processing guarantees can be customized according to each test instance.
 * (e.g. process ordering will be no longer kept if the processor uses retry-queueing feature)
 */
@Slf4j
@RequiredArgsConstructor
public class ProcessorTestSuite {
    private final KafkaClusterRule rule;
    private final Function<ProcessorsBuilder<TestTask>, ProcessorsBuilder<TestTask>> configureProcessorsBuilder;
    private final RetryConfig retryConfig;
    private final PropertySupplier propertySuppliers;
    private final Set<ProcessingGuarantee> semantics;
    private final SubscriptionStatesListener statesListener;

    private static final int NUM_TASKS = 10000;
    private static final int NUM_KEYS = 100;
    private static final int NUM_SUBSCRIPTION_INSTANCES = 3;
    private static final int NUM_PARTITIONS = 8;

    /**
     * An interface to listen multiple subscription's state changes
     */
    @FunctionalInterface
    public interface SubscriptionStatesListener {
        /**
         * Called at state transitioned to new state
         * @param instanceId id of the subscription instance which state has changed. Possible values are 0..{@link #NUM_SUBSCRIPTION_INSTANCES} - 1
         * @param newState new state of the subscription
         */
        void onChange(int instanceId, SubscriptionStateListener.State newState);
    }

    @Setter
    @Accessors(fluent = true)
    public static class Builder {
        private final KafkaClusterRule rule;

        private final Set<GuaranteeType> defaultSemantics = EnumSet.allOf(GuaranteeType.class);
        private final Set<ProcessingGuarantee> customSemantics = new HashSet<>();

        /**
         * Configure test-specific processing logic
         */
        private Function<ProcessorsBuilder<TestTask>, ProcessorsBuilder<TestTask>> configureProcessorsBuilder;
        /**
         * Configure retry-queueing feature for the subscription
         */
        private RetryConfig retryConfig;
        /**
         * Supply additional {@link ProcessorProperties} through {@link PropertySupplier}
         */
        private PropertySupplier propertySupplier;
        /**
         * Listen every subscription's state changes
         */
        private SubscriptionStatesListener statesListener;
        /**
         * Exclude semantics from assertion.
         * Intended to be used when we test a feature which breaks subset of semantics
         */
        public Builder excludeSemantics(GuaranteeType... guarantees) {
            for (GuaranteeType guarantee : guarantees) {
                defaultSemantics.remove(guarantee);
            }
            return this;
        }
        /**
         * Include additional semantics in assertion.
         * Feature-specific processing guarantee will be injected through this method
         */
        public Builder customSemantics(ProcessingGuarantee... guarantees) {
            customSemantics.addAll(Arrays.asList(guarantees));
            return this;
        }

        private Builder(KafkaClusterRule rule) {
            this.rule = rule;
        }

        public ProcessorTestSuite build() {
            Set<ProcessingGuarantee> semantics = new HashSet<>();
            for (GuaranteeType guaranteeType : defaultSemantics) {
                semantics.add(guaranteeType.get());
            }
            semantics.addAll(customSemantics);

            if (statesListener == null) {
                statesListener = (id, state) -> {};
            }

            return new ProcessorTestSuite(rule,
                                          configureProcessorsBuilder,
                                          retryConfig,
                                          propertySupplier,
                                          semantics,
                                          statesListener);
        }
    }

    public static Builder builder(KafkaClusterRule rule) {
        return new Builder(rule);
    }

    /**
     * Run the test.
     *
     * Can be called only once per {@link ProcessorTestSuite} instance since
     * running the test possibly mutates {@link ProcessingGuarantee}'s internal state
     */
    public void run() {
        String topic = rule.admin().createRandomTopic(NUM_PARTITIONS, 3);
        CountDownLatch rollingRestartLatch = new CountDownLatch(NUM_TASKS / 2);
        Producer<String, DecatonTaskRequest> producer = null;
        ProcessorSubscription[] subscriptions = new ProcessorSubscription[NUM_SUBSCRIPTION_INSTANCES];

        try {
            producer = TestUtils.producer(rule.bootstrapServers());
            for (int i = 0; i < subscriptions.length; i++) {
                subscriptions[i] = newSubscription(i, topic, Optional.of(rollingRestartLatch));
            }
            CompletableFuture<Map<Integer, Long>> produceFuture =
                    produceTasks(producer, topic, record -> semantics.forEach(g -> g.onProduce(record)));

            rollingRestartLatch.await();
            performRollingRestart(subscriptions, i -> newSubscription(i, topic, Optional.empty()));
            awaitAllOffsetsCommitted(topic, produceFuture);

            for (ProcessingGuarantee guarantee : semantics) {
                guarantee.doAssert();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            safeClose(producer);
            for (int i = 0; i < subscriptions.length; i++) {
                log.info("Closing subscription-{} (threadId: {})", i, subscriptions[i].getId());
                safeClose(subscriptions[i]);
            }
            rule.admin().deleteTopics(topic);
        }
    }

    private ProcessorSubscription newSubscription(int id, String topic, Optional<CountDownLatch> processLatch) {
        DecatonProcessor<TestTask> preprocessor = (context, task) -> {
            long startTime = System.nanoTime();
            try {
                context.deferCompletion().completeWith(context.push(task));
            } finally {
                ProcessedRecord record = new ProcessedRecord(context.key(), task, startTime, System.nanoTime());
                semantics.forEach(g -> g.onProcess(context.metadata(), record));
                processLatch.ifPresent(CountDownLatch::countDown);
            }
        };

        ProcessorsBuilder<TestTask> processorsBuilder =
                configureProcessorsBuilder.apply(
                        ProcessorsBuilder.consuming(topic, new TestTaskDeserializer())
                                         .thenProcess(preprocessor));

        return TestUtils.subscription("subscription-" + id,
                                      rule.bootstrapServers(),
                                      processorsBuilder,
                                      retryConfig,
                                      propertySuppliers,
                                      state -> statesListener.onChange(id, state));
    }

    private static void performRollingRestart(ProcessorSubscription[] subscriptions,
                                              Function<Integer, ProcessorSubscription> subscriptionConstructor)
            throws Exception {
        for (int i = 0; i < subscriptions.length; i++) {
            log.info("Start restarting subscription-{} (threadId: {})", i, subscriptions[i].getId());
            subscriptions[i].close();
            subscriptions[i] = subscriptionConstructor.apply(i);
            log.info("Finished restarting subscription-{} (threadId: {})", i, subscriptions[i].getId());
        }
    }

    private void awaitAllOffsetsCommitted(String topic,
                                          CompletableFuture<Map<Integer, Long>> produceFuture) {
        Map<Integer, Long> producedOffsets = produceFuture.join();
        TestUtils.awaitCondition("all produced offsets should be committed", () -> {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    rule.admin().consumerGroupOffsets(TestUtils.DEFAULT_GROUP_ID);

            for (Entry<Integer, Long> entry : producedOffsets.entrySet()) {
                int partition = entry.getKey();
                long produced = entry.getValue();

                OffsetAndMetadata metadata = committed.get(new TopicPartition(topic, partition));
                if (metadata == null || metadata.offset() <= produced) {
                    return false;
                }
            }
            return true;
        });
    }

    private static void safeClose(AutoCloseable resource) {
        try {
            if (resource != null) {
                resource.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close the resource", e);
        }
    }

    /**
     * Generate and produce {@link #NUM_TASKS} tasks
     * @param producer Producer instance to be used
     * @param topic Topic to be sent tasks
     * @param onProduce Callback which is called when a task is complete to be sent
     * @return A CompletableFuture of Map, which holds partition as the key and max offset as the value
     */
    private static CompletableFuture<Map<Integer, Long>> produceTasks(
            Producer<String, DecatonTaskRequest> producer,
            String topic,
            Consumer<ProducedRecord> onProduce) {
        @SuppressWarnings("unchecked")
        CompletableFuture<RecordMetadata>[] produceFutures = new CompletableFuture[NUM_TASKS];

        TestTaskSerializer serializer = new TestTaskSerializer();
        for (int i = 0; i < produceFutures.length; i++) {
            TestTask task = new TestTask(String.valueOf(i));
            String key = String.valueOf(i % NUM_KEYS);
            TaskMetadataProto taskMetadata =
                    TaskMetadataProto.newBuilder()
                                     .setTimestampMillis(System.currentTimeMillis())
                                     .setSourceApplicationId("test-application")
                                     .setSourceInstanceId("test-instance")
                                     .build();
            DecatonTaskRequest request =
                    DecatonTaskRequest.newBuilder()
                                      .setMetadata(taskMetadata)
                                      .setSerializedTask(ByteString.copyFrom(serializer.serialize(task)))
                                      .build();
            ProducerRecord<String, DecatonTaskRequest> record =
                    new ProducerRecord<>(topic, null, taskMetadata.getTimestampMillis(), key, request);
            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            produceFutures[i] = future;

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    future.complete(metadata);
                    onProduce.accept(new ProducedRecord(key,
                                                        new TopicPartition(metadata.topic(), metadata.partition()),
                                                        metadata.offset(),
                                                        task));
                } else {
                    future.completeExceptionally(exception);
                }
            });
        }

        return CompletableFuture.allOf(produceFutures).thenApply(notUsed -> {
            Map<Integer, Long> result = new HashMap<>();
            for (CompletableFuture<RecordMetadata> future : produceFutures) {
                RecordMetadata metadata = future.join();
                long offset = result.getOrDefault(metadata.partition(), -1L);
                if (offset < metadata.offset()) {
                    result.put(metadata.partition(), metadata.offset());
                }
            }
            return result;
        });
    }
}
