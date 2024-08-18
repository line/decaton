/*
 * Copyright 2024 LY Corporation
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

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.client.kafka.PrintableAsciiStringSerializer;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.DynamicProperty;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;
import com.linecorp.decaton.protocol.Decaton.TaskMetadataProto;
import com.linecorp.decaton.protocol.Sample.HelloTask;
import com.linecorp.decaton.protocol.internal.DecatonInternal.DecatonTaskRequest;
import com.linecorp.decaton.testing.KafkaClusterExtension;
import com.linecorp.decaton.testing.TestUtils;

/*
 * Integration tests to check Decaton 9.0.0 migration from older version,
 * to check that protocol migration works correctly.
 */
public class ProtocolMigrationTest {
    @RegisterExtension
    public static KafkaClusterExtension rule = new KafkaClusterExtension();

    private String topic;
    private String retryTopic;

    @BeforeEach
    public void setUp() {
        topic = rule.admin().createRandomTopic(3, 3);
        retryTopic = topic + "-retry";
        rule.admin().createTopic(retryTopic, 3, 3);
    }

    @AfterEach
    public void tearDown() {
        rule.admin().deleteTopics(true, topic, retryTopic);
    }

    /*
     * Check if the migration works when DecatonClient is used.
     */
    @Test
    @Timeout(30)
    public void testMigration_deserializer() throws Exception {
        DynamicProperty<Boolean> retryTaskInLegacyFormat =
                new DynamicProperty<>(ProcessorProperties.CONFIG_RETRY_TASK_IN_LEGACY_FORMAT);
        retryTaskInLegacyFormat.set(true);
        DynamicProperty<Boolean> legacyParseFallbackEnabled =
                new DynamicProperty<>(ProcessorProperties.CONFIG_LEGACY_PARSE_FALLBACK_ENABLED);
        legacyParseFallbackEnabled.set(true);

        Set<HelloTask> produced = ConcurrentHashMap.newKeySet();
        Set<HelloTask> processed = ConcurrentHashMap.newKeySet();
        try (ProcessorSubscription ignored = TestUtils.subscription(
                rule.bootstrapServers(),
                builder -> {
                    builder.processorsBuilder(
                                   ProcessorsBuilder.consuming(topic, new ProtocolBuffersDeserializer<>(HelloTask.parser()))
                                                    .thenProcess((context, task) -> {
                                                        // always retry once, to check that retry-feature works at every step
                                                        if (context.metadata().retryCount() == 0) {
                                                            context.retry();
                                                        } else {
                                                            processed.add(task);
                                                        }
                                                    }))
                           .addProperties(StaticPropertySupplier.of(retryTaskInLegacyFormat, legacyParseFallbackEnabled))
                           .enableRetry(RetryConfig.builder().backoff(Duration.ZERO).build());
                });
             Producer<String, HelloTask> legacyClient = TestUtils.producer(
                     rule.bootstrapServers(),
                     new PrintableAsciiStringSerializer(),
                     (topic, task) -> DecatonTaskRequest
                             .newBuilder()
                             .setMetadata(TaskMetadataProto.newBuilder()
                                                           .setTimestampMillis(System.currentTimeMillis())
                                                           .setSourceApplicationId("test-application")
                                                           .setSourceInstanceId("test-instance")
                                                           .build())
                             .setSerializedTask(ByteString.copyFrom(task.toByteArray()))
                             .build()
                             .toByteArray());
             DecatonClient<HelloTask> client = TestUtils.client(topic, rule.bootstrapServers())) {

            // step1: initial
            for (int i = 0; i < 10; i++) {
                HelloTask task = HelloTask.newBuilder().setName("hello1-" + i).build();
                legacyClient.send(new ProducerRecord<>(topic, task));
                produced.add(task);
            }
            // step2: migrate retry tasks to new format
            retryTaskInLegacyFormat.set(false);
            for (int i = 0; i < 10; i++) {
                HelloTask task = HelloTask.newBuilder().setName("hello2-" + i).build();
                legacyClient.send(new ProducerRecord<>(topic, task));
                produced.add(task);
            }
            // step3: migrate decaton client to new format
            for (int i = 0; i < 10; i++) {
                HelloTask task = HelloTask.newBuilder().setName("hello3-" + i).build();
                client.put(null, task);
                produced.add(task);
            }

            // Before go to last step, we have to ensure that there are no legacy format task at all.
            TestUtils.awaitCondition(
                    "all produced tasks should be processed",
                    () -> produced.equals(processed));

            // step4: disable legacy parse fallback
            legacyParseFallbackEnabled.set(false);
            for (int i = 0; i < 10; i++) {
                HelloTask task = HelloTask.newBuilder().setName("hello4-" + i).build();
                client.put(null, task);
                produced.add(task);
            }

            // Check if it works after legacy parse fallback is disabled
            TestUtils.awaitCondition(
                    "all produced tasks should be processed",
                    () -> produced.equals(processed));
        }
    }

    /*
     * Check if the migration works when pure producer is used.
     */
    @Test
    @Timeout(30)
    public void testMigration_taskExtractor() throws Exception {
        DynamicProperty<Boolean> retryTaskInLegacyFormat =
                new DynamicProperty<>(ProcessorProperties.CONFIG_RETRY_TASK_IN_LEGACY_FORMAT);
        retryTaskInLegacyFormat.set(true);
        DynamicProperty<Boolean> legacyParseFallbackEnabled =
                new DynamicProperty<>(ProcessorProperties.CONFIG_LEGACY_PARSE_FALLBACK_ENABLED);
        legacyParseFallbackEnabled.set(true);

        Set<HelloTask> produced = ConcurrentHashMap.newKeySet();
        Set<HelloTask> processed = ConcurrentHashMap.newKeySet();
        try (ProcessorSubscription ignored = TestUtils.subscription(
                rule.bootstrapServers(),
                builder -> {
                    builder.processorsBuilder(
                                   ProcessorsBuilder.consuming(topic, (TaskExtractor<HelloTask>) record -> {
                                                        try {
                                                            return new DecatonTask<>(
                                                                    TaskMetadata.builder().build(),
                                                                    HelloTask.parser().parseFrom(record.value()),
                                                                    record.value());
                                                        } catch (InvalidProtocolBufferException e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                    })
                                                    .thenProcess((context, task) -> {
                                                        // always retry once, to check that retry-feature works at every step
                                                        if (context.metadata().retryCount() == 0) {
                                                            context.retry();
                                                        } else {
                                                            processed.add(task);
                                                        }
                                                    }))
                           .addProperties(StaticPropertySupplier.of(retryTaskInLegacyFormat, legacyParseFallbackEnabled))
                           .enableRetry(RetryConfig.builder().backoff(Duration.ZERO).build());
                });
             Producer<String, HelloTask> producer = TestUtils.producer(
                     rule.bootstrapServers(),
                     new PrintableAsciiStringSerializer(),
                     (topic, task) -> task.toByteArray())) {

            // step1: initial
            for (int i = 0; i < 10; i++) {
                HelloTask task = HelloTask.newBuilder().setName("hello1-" + i).build();
                producer.send(new ProducerRecord<>(topic, task));
                produced.add(task);
            }
            // step2: migrate retry tasks to new format
            retryTaskInLegacyFormat.set(false);
            for (int i = 0; i < 10; i++) {
                HelloTask task = HelloTask.newBuilder().setName("hello2-" + i).build();
                producer.send(new ProducerRecord<>(topic, task));
                produced.add(task);
            }

            // Before go to last step, we have to ensure that there are no legacy format task at all.
            TestUtils.awaitCondition(
                    "all produced tasks should be processed",
                    () -> produced.equals(processed));

            // step3: disable legacy parse fallback
            legacyParseFallbackEnabled.set(false);
            for (int i = 0; i < 10; i++) {
                HelloTask task = HelloTask.newBuilder().setName("hello3-" + i).build();
                producer.send(new ProducerRecord<>(topic, task));
                produced.add(task);
            }

            // Check if it works after legacy parse fallback is disabled
            TestUtils.awaitCondition(
                    "all produced tasks should be processed",
                    () -> produced.equals(processed));
        }
    }
}
