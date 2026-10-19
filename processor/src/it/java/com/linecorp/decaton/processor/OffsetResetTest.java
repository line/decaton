/*
 * Copyright 2026 LY Corporation
 *
 * LY Corporation licenses this file to you under the Apache License,
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;
import com.linecorp.decaton.processor.runtime.internal.RateLimiter;
import com.linecorp.decaton.testing.KafkaClusterExtension;
import com.linecorp.decaton.testing.TestUtils;

/**
 * Verifies how partitions without committed offsets are consumed,
 * comparing Decaton's default auto.offset.reset (earliest) with explicitly configured latest.
 */
public class OffsetResetTest {
    @RegisterExtension
    public static KafkaClusterExtension rule = new KafkaClusterExtension();

    // All tasks share the same key so that they are processed in order
    private static final String KEY = "key";

    enum OffsetReset {
        DEFAULT(null),
        LATEST("latest");

        private final String value;

        OffsetReset(String value) {
            this.value = value;
        }

        boolean readsFromBeginning() {
            return this == DEFAULT;
        }
    }

    private final List<String> topics = new ArrayList<>();
    private String topic;
    private String groupId;
    private Producer<String, String> producer;

    @BeforeEach
    public void setUp() {
        topic = createTopic("test-" + UUID.randomUUID());
        groupId = "offset-reset-test-" + UUID.randomUUID();
        producer = TestUtils.producer(rule.bootstrapServers(), new StringSerializer(), new StringSerializer());
    }

    @AfterEach
    public void tearDown() {
        producer.close();
        rule.admin().deleteTopics(true, topics.toArray(new String[0]));
    }

    @ParameterizedTest
    @EnumSource(OffsetReset.class)
    @Timeout(60)
    public void testTaskProducedBeforeSubscriptionStarts(OffsetReset offsetReset) throws Exception {
        // scenario:
        //   * a task is produced before the subscription of a new consumer group starts
        //     (e.g. producer is deployed before processor)
        TopicPartition partition = new TopicPartition(topic, 0);
        produce(partition, "produced-before-start");

        Set<String> processed = ConcurrentHashMap.newKeySet();
        try (ProcessorSubscription subscription = subscription(offsetReset, recording(processed))) {
            produceMarkersUntilProcessed(partition, processed);
        }

        assertProcessedOnlyIfReadsFromBeginning(offsetReset, processed,
                                                Collections.singleton("produced-before-start"));
    }

    @ParameterizedTest
    @EnumSource(OffsetReset.class)
    @Timeout(60)
    public void testTaskProducedToAddedPartition(OffsetReset offsetReset) throws Exception {
        // scenario:
        //   * the consumer group has committed offset for the existing partition
        //   * a partition is added, and a task is produced to it before the partition gets assigned
        TopicPartition existingPartition = new TopicPartition(topic, 0);
        Set<String> processedBeforeIncrease = ConcurrentHashMap.newKeySet();
        try (ProcessorSubscription subscription = subscription(offsetReset, recording(processedBeforeIncrease))) {
            long endOffset = produceMarkersUntilProcessed(existingPartition, processedBeforeIncrease);
            awaitCommitted(existingPartition, endOffset);
        }

        rule.admin().increasePartitions(topic, 2);
        TopicPartition addedPartition = new TopicPartition(topic, 1);
        produce(addedPartition, "produced-to-added-partition");

        Set<String> processed = ConcurrentHashMap.newKeySet();
        try (ProcessorSubscription subscription = subscription(offsetReset, recording(processed))) {
            produceMarkersUntilProcessed(addedPartition, processed);
            // If the existing partition were consumed from the beginning, its old tasks would be processed
            // before this marker
            produceMarkersUntilProcessed(existingPartition, processed);
        }

        assertProcessedOnlyIfReadsFromBeginning(offsetReset, processed,
                                                Collections.singleton("produced-to-added-partition"));
        assertTrue(Collections.disjoint(processedBeforeIncrease, processed),
                   "partition with committed offset should be consumed from the committed offset");
    }

    @ParameterizedTest
    @EnumSource(OffsetReset.class)
    @Timeout(60)
    public void testRetryTaskInFlightOnRebalance(OffsetReset offsetReset) throws Exception {
        // scenario:
        //   * a task is retried, and a rebalance happens before the retried task completes
        //     (e.g. awaiting backoff during shutdown)
        //   * the retry topic partition has no committed offset since no retried task has completed on it
        String retryTopic = createTopic(topic + RetryConfig.DEFAULT_RETRY_TOPIC_SUFFIX);
        TopicPartition originPartition = new TopicPartition(topic, 0);
        TopicPartition retryPartition = new TopicPartition(retryTopic, 0);
        RetryConfig retryConfig = RetryConfig.withBackoff(Duration.ofMillis(10));

        Set<String> processedByFirst = ConcurrentHashMap.newKeySet();
        Set<String> inFlightOnFirst = ConcurrentHashMap.newKeySet();
        // The first subscription always uses the default so that it surely fetches the retried task
        // regardless of when its position is determined. Only the second one uses the offsetReset under test.
        try (ProcessorSubscription first = subscription(
                OffsetReset.DEFAULT,
                builder -> builder.enableRetry(retryConfig).processorsBuilder(
                        ProcessorsBuilder.consuming(topic, new StringDeserializer())
                                         .thenProcess((ctx, task) -> {
                                             if (!"retried-task".equals(task)) {
                                                 processedByFirst.add(task);
                                             } else if (ctx.metadata().retryCount() == 0) {
                                                 ctx.retry();
                                             } else {
                                                 // Never complete to keep the retried task in-flight
                                                 ctx.deferCompletion();
                                                 inFlightOnFirst.add(task);
                                             }
                                         })))) {
            produceMarkersUntilProcessed(originPartition, processedByFirst);
            long offset = produce(originPartition, "retried-task");
            TestUtils.awaitCondition("retried task should be in-flight",
                                     () -> inFlightOnFirst.contains("retried-task"));
            awaitCommitted(originPartition, offset + 1);
        }
        assertFalse(committedOffsets().containsKey(retryPartition),
                    "retry topic partition should not have committed offset");

        Set<String> processedBySecond = ConcurrentHashMap.newKeySet();
        try (ProcessorSubscription second = subscription(
                offsetReset,
                recording(processedBySecond).andThen(builder -> builder.enableRetry(retryConfig)))) {
            produceMarkersUntilProcessed(retryPartition, processedBySecond);
        }

        assertProcessedOnlyIfReadsFromBeginning(offsetReset, processedBySecond,
                                                Collections.singleton("retried-task"));
    }

    @ParameterizedTest
    @EnumSource(OffsetReset.class)
    @Timeout(60)
    public void testShapedTaskInFlightOnRebalance(OffsetReset offsetReset) throws Exception {
        // scenario:
        //   * tasks of a bursting key are shaped, and a rebalance happens before the shaped tasks complete
        //   * the shaping topic partition has no committed offset since no shaped task has completed on it
        String shapingTopic = createTopic(topic + PerKeyQuotaConfig.DEFAULT_SHAPING_TOPIC_SUFFIX);
        TopicPartition originPartition = new TopicPartition(topic, 0);
        TopicPartition shapingPartition = new TopicPartition(shapingTopic, 0);
        Set<String> shapedTasks = ConcurrentHashMap.newKeySet();
        Consumer<SubscriptionBuilder> enablePerKeyQuota = builder -> builder
                .addProperties(StaticPropertySupplier.of(
                        Property.ofStatic(ProcessorProperties.CONFIG_PER_KEY_QUOTA_PROCESSING_RATE, 1L)))
                .enablePerKeyQuota(PerKeyQuotaConfig.shape()
                                                    .toBuilder()
                                                    .window(Duration.ofMillis(50L))
                                                    .callback((record, metrics) -> {
                                                        shapedTasks.add(new String(record.value(), UTF_8));
                                                        return shapingTopic;
                                                    })
                                                    .build())
                // The shaping topic is processed at the per-key quota rate (1 task/sec) unless overridden,
                // which can't catch up with produceMarkersUntilProcessed producing a marker every second
                .overrideShapingRate(shapingTopic, StaticPropertySupplier.of(
                        Property.ofStatic(PerKeyQuotaConfig.shapingRateProperty(shapingTopic), RateLimiter.UNLIMITED))
                );

        Set<String> inFlightOnFirst = ConcurrentHashMap.newKeySet();
        // The first subscription always uses the default so that it surely fetches the shaped tasks
        // regardless of when its position is determined. Only the second one uses the offsetReset under test.
        try (ProcessorSubscription first = subscription(
                OffsetReset.DEFAULT,
                builder -> enablePerKeyQuota.accept(builder.processorsBuilder(
                        ProcessorsBuilder.consuming(topic, new StringDeserializer())
                                         .thenProcess((ctx, task) -> {
                                             if (shapedTasks.contains(task)) {
                                                 // Never complete to keep the shaped task in-flight
                                                 ctx.deferCompletion();
                                                 inFlightOnFirst.add(task);
                                             }
                                         }))))) {
            long endOffset = 0;
            for (int i = 0; shapedTasks.isEmpty(); i++) {
                endOffset = produce(originPartition, "task-" + i) + 1;
            }
            awaitCommitted(originPartition, endOffset);
            TestUtils.awaitCondition("shaped tasks should be in-flight",
                                     () -> inFlightOnFirst.containsAll(shapedTasks));
        }
        Set<String> shapedByFirst = new HashSet<>(shapedTasks);
        assertFalse(committedOffsets().containsKey(shapingPartition),
                    "shaping topic partition should not have committed offset");

        Set<String> processedBySecond = ConcurrentHashMap.newKeySet();
        try (ProcessorSubscription second = subscription(
                offsetReset,
                recording(processedBySecond).andThen(enablePerKeyQuota))) {
            produceMarkersUntilProcessed(shapingPartition, processedBySecond);
        }

        assertProcessedOnlyIfReadsFromBeginning(offsetReset, processedBySecond, shapedByFirst);
    }

    private static void assertProcessedOnlyIfReadsFromBeginning(OffsetReset offsetReset,
                                                                Set<String> processed,
                                                                Collection<String> tasks) {
        if (offsetReset.readsFromBeginning()) {
            assertTrue(processed.containsAll(tasks), "tasks should be processed: " + tasks);
        } else {
            assertTrue(Collections.disjoint(processed, tasks), "tasks should be skipped: " + tasks);
        }
    }

    private String createTopic(String name) {
        rule.admin().createTopic(name, 1, 3);
        topics.add(name);
        return name;
    }

    private ProcessorSubscription subscription(OffsetReset offsetReset,
                                               Consumer<SubscriptionBuilder> builderConfigurer)
            throws Exception {
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // Refresh metadata frequently so that newly created topics and added partitions get assigned soon
        // even if the subscription sees stale metadata right after they are created
        consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        if (offsetReset.value != null) {
            consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset.value);
        }
        return TestUtils.subscription("subscription-" + UUID.randomUUID(),
                                      rule.bootstrapServers(),
                                      builderConfigurer,
                                      consumerConfig);
    }

    private Consumer<SubscriptionBuilder> recording(Set<String> processed) {
        return builder -> builder.processorsBuilder(
                ProcessorsBuilder.consuming(topic, new StringDeserializer())
                                 .thenProcess((ctx, task) -> processed.add(task)));
    }

    private long produce(TopicPartition partition, String task) throws Exception {
        return producer.send(new ProducerRecord<>(partition.topic(), partition.partition(), KEY, task))
                       .get()
                       .offset();
    }

    /**
     * Produce markers until one of them is processed.
     * The position of a partition without committed offset is determined asynchronously after the assignment,
     * so a marker produced before that could be skipped with latest.
     * Since tasks of the same key are processed in order, all tasks before the processed marker
     * have been processed (or skipped) when this method returns.
     * @return the end offset of the partition
     */
    private long produceMarkersUntilProcessed(TopicPartition partition, Set<String> processed) throws Exception {
        while (true) {
            String marker = "marker-" + UUID.randomUUID();
            long offset = produce(partition, marker);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (System.nanoTime() < deadline) {
                if (processed.contains(marker)) {
                    return offset + 1;
                }
                Thread.sleep(50L);
            }
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> committedOffsets() {
        return rule.admin().consumerGroupOffsets(groupId);
    }

    private void awaitCommitted(TopicPartition partition, long offset) throws InterruptedException {
        TestUtils.awaitCondition("offset " + offset + " should be committed for " + partition, () -> {
            OffsetAndMetadata committed = committedOffsets().get(partition);
            return committed != null && committed.offset() == offset;
        });
    }
}
