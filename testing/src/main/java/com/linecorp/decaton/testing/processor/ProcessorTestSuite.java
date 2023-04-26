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

import static com.linecorp.decaton.testing.TestUtils.DEFINITELY_TOO_SLOW;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.google.protobuf.ByteString;

import com.linecorp.decaton.client.DecatonClientBuilder.DefaultKafkaProducerSupplier;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.PerKeyQuotaConfigBuilder;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.PropertySupplier;
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.processor.runtime.RetryConfig.RetryConfigBuilder;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.runtime.SubscriptionStateListener;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import com.linecorp.decaton.processor.runtime.internal.RateLimiter;
import com.linecorp.decaton.processor.tracing.TracingProvider;
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
    private final int numTasks;
    private final Function<ProcessorsBuilder<TestTask>, ProcessorsBuilder<TestTask>> configureProcessorsBuilder;
    private final RetryConfig retryConfig;
    private final PerKeyQuotaConfig perKeyQuotaConfig;
    private final Properties consumerConfig;
    private final PropertySupplier propertySuppliers;
    private final Set<ProcessingGuarantee> semantics;
    private final SubscriptionStatesListener statesListener;
    private final TracingProvider tracingProvider;
    private final Function<String, Producer<byte[], DecatonTaskRequest>> producerSupplier;
    private final TaskExtractor<TestTask> customTaskExtractor;

    private static final int DEFAULT_NUM_TASKS = 10000;
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
         * Number of tasks to produce.
         */
        private int numTasks = DEFAULT_NUM_TASKS;
        /**
         * Configure test-specific processing logic
         */
        private Function<ProcessorsBuilder<TestTask>, ProcessorsBuilder<TestTask>> configureProcessorsBuilder;
        /**
         * Configure retry-queueing feature for the subscription
         */
        private RetryConfig retryConfig;
        /**
         * Configure per-key quota feature for the subscription
         */
        private PerKeyQuotaConfig perKeyQuotaConfig;
        /**
         * Supply additional {@link ProcessorProperties} through {@link PropertySupplier}
         */
        private PropertySupplier propertySupplier;
        /**
         * Supply additional {@link Properties} to be passed to instantiate {@link KafkaConsumer}
         */
        private Properties consumerConfig;
        /**
         * Listen every subscription's state changes
         */
        private SubscriptionStatesListener statesListener;
        /**
         * Supply {@link TracingProvider} to enable tracing for the subscription
         */
        private TracingProvider tracingProvider;
        /**
         * Specify custom supplier to instantiate {@link Producer}.
         * Expected use case:
         *   supply a producer which adds tracing id to each message to test tracing-functionality in e2e
         */
        private Function<String, Producer<byte[], DecatonTaskRequest>> producerSupplier = TestUtils::producer;
        /**
         * Supply custom {@link TaskExtractor} to be used to extract a task.
         */
        private TaskExtractor<TestTask> customTaskExtractor;

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
                                          numTasks,
                                          configureProcessorsBuilder,
                                          retryConfig,
                                          perKeyQuotaConfig,
                                          consumerConfig,
                                          propertySupplier,
                                          semantics,
                                          statesListener,
                                          tracingProvider,
                                          producerSupplier,
                                          customTaskExtractor);
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
    public void run() throws InterruptedException, ExecutionException, TimeoutException {
        String topic = rule.admin().createRandomTopic(NUM_PARTITIONS, 3);
        CountDownLatch rollingRestartLatch = new CountDownLatch(numTasks / 2);
        ProcessorSubscription[] subscriptions = new ProcessorSubscription[NUM_SUBSCRIPTION_INSTANCES];

        try (Producer<byte[], DecatonTaskRequest> producer = producerSupplier.apply(rule.bootstrapServers())) {
            ConcurrentMap<TopicPartition, Long> queuedTaskOffsets = new ConcurrentHashMap<>();
            for (int i = 0; i < subscriptions.length; i++) {
                subscriptions[i] = newSubscription(i, topic, Optional.of(rollingRestartLatch), queuedTaskOffsets);
            }
            CompletableFuture<Map<TopicPartition, Long>> produceFuture =
                    produceTasks(producer, topic, record -> semantics.forEach(g -> g.onProduce(record)));

            if (!rollingRestartLatch.await(DEFINITELY_TOO_SLOW.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("Rolling restart did not complete within " + DEFINITELY_TOO_SLOW);
            }
            performRollingRestart(subscriptions, i -> newSubscription(i, topic, Optional.empty(), queuedTaskOffsets));
            awaitAllOffsetsCommitted(produceFuture.get(DEFINITELY_TOO_SLOW.toMillis(), TimeUnit.MILLISECONDS));
            if (!queuedTaskOffsets.isEmpty()) {
                awaitAllOffsetsCommitted(queuedTaskOffsets);
            }

            for (ProcessingGuarantee guarantee : semantics) {
                guarantee.doAssert();
            }
        } finally {
            for (int i = 0; i < subscriptions.length; i++) {
                log.info("Closing subscription-{} (threadId: {})", i, subscriptions[i].getId());
                subscriptions[i].initiateShutdown();
            }
            for (ProcessorSubscription subscription : subscriptions) {
                try {
                    subscription.awaitShutdown(DEFINITELY_TOO_SLOW);
                } catch (TimeoutException | ExecutionException e) {
                    log.warn("Failed to close the resource within {}", DEFINITELY_TOO_SLOW, e);
                }
            }
            rule.admin().deleteTopics(true, topic);
        }
    }

    private ProcessorSubscription newSubscription(
            int id,
            String topic,
            Optional<CountDownLatch> processLatch,
            ConcurrentMap<TopicPartition, Long> queuedTaskOffsets) throws InterruptedException, TimeoutException {
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

        final ProcessorsBuilder<TestTask> sourceBuilder;
        if (customTaskExtractor == null) {
            sourceBuilder = ProcessorsBuilder.consuming(topic, new TestTaskDeserializer());
        } else {
            sourceBuilder = ProcessorsBuilder.consuming(topic, customTaskExtractor);
        }
        ProcessorsBuilder<TestTask> processorsBuilder =
                configureProcessorsBuilder.apply(sourceBuilder.thenProcess(preprocessor));

        return TestUtils.subscription(
                "subscription-" + id,
                rule.bootstrapServers(),
                builder -> {
                    builder.processorsBuilder(processorsBuilder);
                    if (retryConfig != null) {
                        RetryConfigBuilder retryConfigBuilder = retryConfig.toBuilder();
                        retryConfigBuilder.producerSupplier(props -> {
                            final Producer<byte[], DecatonTaskRequest> producer;
                            if (retryConfig.producerSupplier() != null) {
                                producer = retryConfig.producerSupplier().getProducer(props);
                            } else {
                                producer = new DefaultKafkaProducerSupplier().getProducer(props);
                            }
                            return new InterceptingProducer<>(producer, recordingInterceptor(queuedTaskOffsets));
                        });
                        builder.enableRetry(retryConfigBuilder.build());
                    }
                    if (perKeyQuotaConfig != null) {
                        PerKeyQuotaConfigBuilder perKeyQuotaConfigBuilder = perKeyQuotaConfig.toBuilder();
                        perKeyQuotaConfigBuilder.producerSupplier(props -> {
                            final Producer<byte[], byte[]> producer;
                            if (perKeyQuotaConfig.producerSupplier() != null) {
                                producer = perKeyQuotaConfig.producerSupplier().apply(props);
                            } else {
                                producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
                            }
                            return new InterceptingProducer<>(producer, recordingInterceptor(queuedTaskOffsets));
                        });
                        builder.enablePerKeyQuota(perKeyQuotaConfigBuilder.build());
                        String shapingTopic = topic + "-shaping";
                        builder.overrideShapingRate(shapingTopic, StaticPropertySupplier.of(
                                Property.ofStatic(PerKeyQuotaConfig.shapingRateProperty(shapingTopic), RateLimiter.UNLIMITED)
                        ));
                    }
                    if (propertySuppliers != null) {
                        builder.addProperties(propertySuppliers);
                    }
                    if (tracingProvider != null) {
                        builder.enableTracing(tracingProvider);
                    }
                    builder.stateListener(state -> statesListener.onChange(id, state));
                },
                consumerConfig);
    }

    @FunctionalInterface
    interface SubscriptionConstructor {
        ProcessorSubscription apply(int index) throws InterruptedException, TimeoutException;
    }

    private static Consumer<RecordMetadata> recordingInterceptor(ConcurrentMap<TopicPartition, Long> queuedTaskOffsets) {
        return meta -> queuedTaskOffsets.compute(new TopicPartition(meta.topic(), meta.partition()),
                                                 (k, v) -> Math.max(v == null ? -1L : v, meta.offset()));
    }

    private static void performRollingRestart(ProcessorSubscription[] subscriptions,
                                              SubscriptionConstructor subscriptionConstructor)
            throws ExecutionException, InterruptedException, TimeoutException {
        for (int i = 0; i < subscriptions.length; i++) {
            log.info("Start restarting subscription-{} (threadId: {})", i, subscriptions[i].getId());
            subscriptions[i].initiateShutdown();
            subscriptions[i].awaitShutdown(DEFINITELY_TOO_SLOW);
            subscriptions[i] = subscriptionConstructor.apply(i);
            log.info("Finished restarting subscription-{} (threadId: {})", i, subscriptions[i].getId());
        }
    }

    private void awaitAllOffsetsCommitted(Map<TopicPartition, Long> producedOffsets)
            throws InterruptedException {
        TestUtils.awaitCondition("all produced offsets should be committed", () -> {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    rule.admin().consumerGroupOffsets(TestUtils.DEFAULT_GROUP_ID);

            for (Entry<TopicPartition, Long> entry : producedOffsets.entrySet()) {
                long produced = entry.getValue();
                OffsetAndMetadata metadata = committed.get(entry.getKey());
                if (metadata == null || metadata.offset() <= produced) {
                    return false;
                }
            }
            return true;
        }, DEFINITELY_TOO_SLOW.toMillis());
    }

    /**
     * Generate and produce {@link #numTasks} tasks
     * @param producer Producer instance to be used
     * @param topic Topic to be sent tasks
     * @param onProduce Callback which is called when a task is complete to be sent
     * @return A CompletableFuture of Map, which holds partition as the key and max offset as the value
     */
    private CompletableFuture<Map<TopicPartition, Long>> produceTasks(
            Producer<byte[], DecatonTaskRequest> producer,
            String topic,
            Consumer<ProducedRecord> onProduce) {
        @SuppressWarnings("unchecked")
        CompletableFuture<RecordMetadata>[] produceFutures = new CompletableFuture[numTasks];

        TestTaskSerializer serializer = new TestTaskSerializer();
        for (int i = 0; i < produceFutures.length; i++) {
            TestTask task = new TestTask(String.valueOf(i));
            byte[] key = String.valueOf(i % NUM_KEYS).getBytes(StandardCharsets.UTF_8);
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
            ProducerRecord<byte[], DecatonTaskRequest> record =
                    new ProducerRecord<>(topic, null, taskMetadata.getTimestampMillis(), key, request);
            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            produceFutures[i] = future;

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    future.complete(metadata);
                    onProduce.accept(new ProducedRecord(key,
                                                        new TopicPartition(metadata.topic(),
                                                                           metadata.partition()),
                                                        metadata.offset(),
                                                        task,
                                                        record.headers()));
                } else {
                    future.completeExceptionally(exception);
                }
            });
        }

        return CompletableFuture.allOf(produceFutures).thenApply(notUsed -> {
            Map<TopicPartition, Long> result = new HashMap<>();
            for (CompletableFuture<RecordMetadata> future : produceFutures) {
                RecordMetadata metadata = future.join();
                result.compute(new TopicPartition(metadata.topic(), metadata.partition()),
                               (k, v) -> Math.max(v == null ? -1L : v, metadata.offset()));
            }
            return result;
        });
    }

    private static class InterceptingProducer<V> extends ProducerAdaptor<byte[], V> {
        private final Consumer<RecordMetadata> interceptor;

        InterceptingProducer(Producer<byte[], V> delegate,
                             Consumer<RecordMetadata> interceptor) {
            super(delegate);
            this.interceptor = interceptor;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], V> record,
                                           Callback callback) {
            return super.send(record, (meta, e) -> {
                if (meta != null) {
                    interceptor.accept(meta);
                }
                callback.onCompletion(meta, e);
            });
        }
    }
}
