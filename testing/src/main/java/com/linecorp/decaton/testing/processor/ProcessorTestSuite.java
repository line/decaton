package com.linecorp.decaton.testing.processor;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
 * Expected semantics can be customized according to each test instance.
 * (e.g. process ordering will be no longer kept if the processor uses retry-queueing feature)
 */
@Slf4j
@RequiredArgsConstructor
public class ProcessorTestSuite {
    private final KafkaClusterRule rule;
    private final Function<ProcessorsBuilder<TestTask>, ProcessorsBuilder<TestTask>> configureProcessorsBuilder;
    private final RetryConfig retryConfig;
    private final PropertySupplier propertySuppliers;
    private final EnumSet<GuaranteeType> semantics;

    private static final int NUM_TASKS = 10000;
    private static final int NUM_KEYS = 100;
    private static final int NUM_SUBSCRIPTION_INSTANCES = 3;
    private static final int NUM_PARTITIONS = 8;

    @Setter
    @Accessors(fluent = true)
    public static class Builder {
        private final KafkaClusterRule rule;

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
         * Processing semantics the processor should satisfy
         */
        private EnumSet<GuaranteeType> semantics = ProcessingGuarantee.defaultSemantics();

        private Builder(KafkaClusterRule rule) {
            this.rule = rule;
        }

        public ProcessorTestSuite build() {
            return new ProcessorTestSuite(rule,
                                          configureProcessorsBuilder,
                                          retryConfig,
                                          propertySupplier,
                                          semantics);
        }
    }

    public static Builder builder(KafkaClusterRule rule) {
        return new Builder(rule);
    }

    public void run() {
        String topic = rule.admin().createRandomTopic(NUM_PARTITIONS, 3);
        CountDownLatch rollingRestartLatch = new CountDownLatch(NUM_TASKS / 2);
        List<ProcessingGuarantee> semantics = this.semantics.stream()
                                                            .map(GuaranteeType::get)
                                                            .collect(Collectors.toList());

        DecatonProcessor<TestTask> preprocessor = (context, task) -> {
            long startTime = System.nanoTime();
            context.deferCompletion()
                   .completeWith(context.push(task))
                   .whenComplete((r, e) -> {
                       semantics.forEach(
                               g -> g.onProcess(new ProcessedRecord(context.key(),
                                                                    task,
                                                                    startTime,
                                                                    System.nanoTime())));
                   });
            rollingRestartLatch.countDown();
        };

        Supplier<ProcessorSubscription> subscriptionSupplier = () -> {
            ProcessorsBuilder<TestTask> processorsBuilder =
                    configureProcessorsBuilder.apply(
                            ProcessorsBuilder.consuming(topic, new TestTaskDeserializer())
                                             .thenProcess(preprocessor));

            return TestUtils.subscription(rule.bootstrapServers(),
                                          processorsBuilder,
                                          retryConfig,
                                          propertySuppliers);
        };

        Producer<String, DecatonTaskRequest> producer = null;
        ProcessorSubscription[] subscriptions = new ProcessorSubscription[NUM_SUBSCRIPTION_INSTANCES];
        try {
            producer = TestUtils.producer(rule.bootstrapServers());
            for (int i = 0; i < subscriptions.length; i++) {
                subscriptions[i] = subscriptionSupplier.get();
            }
            CompletableFuture<Map<Integer, Long>> produceFuture =
                    produceTasks(producer, topic, record -> semantics.forEach(g -> g.onProduce(record)));

            // perform rolling restart
            rollingRestartLatch.await();
            for (int i = 0; i < subscriptions.length; i++) {
                log.info("Start restarting subscription-{} (threadId: {})", i, subscriptions[i].getId());
                subscriptions[i].close();
                subscriptions[i] = subscriptionSupplier.get();
                log.info("Finished restarting subscription-{} (threadId: {})", i, subscriptions[i].getId());
            }

            // await all produced offsets are committed
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

            for (int i = 0; i < subscriptions.length; i++) {
                log.info("Closing subscription-{} (threadId: {})", i, subscriptions[i].getId());
                subscriptions[i].close();
            }

            for (ProcessingGuarantee guarantee : semantics) {
                guarantee.doAssert();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            safeClose(producer);
            for (ProcessorSubscription subscription : subscriptions) {
                if (subscription != null && subscription.isAlive()) {
                    safeClose(subscription);
                }
            }
            rule.admin().deleteTopics(topic);
        }
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
