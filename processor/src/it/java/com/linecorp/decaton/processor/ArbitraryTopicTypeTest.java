/*
 * Copyright 2022 LINE Corporation
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

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.client.kafka.PrintableAsciiStringSerializer;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.TestUtils;

/**
 * This test class verifies that {@link ProcessorSubscription} is capable of subscribing topics with
 *   - any key types other than String (our default)
 *   - any record types other than {@link DecatonTask}
 */
public class ArbitraryTopicTypeTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    private String topic;
    private String retryTopic;

    @Before
    public void setUp() {
        topic = rule.admin().createRandomTopic(3, 3);
        retryTopic = rule.admin().createRandomTopic(3, 3);
    }

    @After
    public void tearDown() {
        rule.admin().deleteTopics(true, topic, retryTopic);
    }

    private static final class TestProducer<K, V> implements AutoCloseable {
        private final KafkaProducer<K, V> producer;
        private final String topic;

        private TestProducer(String bootstrapServers, String topic, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            final Properties properties = TestUtils.defaultProducerProps(bootstrapServers);

            this.topic = topic;
            producer = new KafkaProducer<>(properties, keySerializer, valueSerializer);
        }

        public CompletableFuture<RecordMetadata> put(K key, V value) {
            final ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);

            final CompletableFuture<RecordMetadata> result = new CompletableFuture<>();
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    result.complete(metadata);
                } else {
                    result.completeExceptionally(exception);
                }
            });

            return result;
        }

        @Override
        public void close() throws Exception {
            producer.close();
        }
    }

    private static final class TestTaskExtractor<T> implements TaskExtractor<T> {
        private final String topic;
        private final Deserializer<T> deserializer;

        private TestTaskExtractor(String topic, Deserializer<T> deserializer) {
            this.topic = topic;
            this.deserializer = deserializer;
        }

        @Override
        public DecatonTask<T> extract(byte[] bytes) {
            final T value = deserializer.deserialize(topic, bytes);
            final TaskMetadata metadata = TaskMetadata.builder().build();
            return new DecatonTask<>(metadata, value, bytes);
        }
    }

    private <K, V> void testRetryWithKeyValue(
            Serializer<K> keySerializer,
            K key,
            Serializer<V> valueSerializer,
            Deserializer<V> valueDeserializer,
            V value
    ) throws Exception {
        final CountDownLatch processLatch = new CountDownLatch(1);
        final RetryConfig retryConfig = RetryConfig.builder().retryTopic(retryTopic).backoff(Duration.ofMillis(10)).build();
        final Consumer<SubscriptionBuilder> builderConfigurer = builder -> builder.processorsBuilder(
                ProcessorsBuilder.consuming(topic, new TestTaskExtractor<>(topic, valueDeserializer)).thenProcess((context, task) -> {
                    if (context.metadata().retryCount() == 0) {
                        context.retry();
                    } else {
                        processLatch.countDown();
                    }
                })).enableRetry(retryConfig);

        try (ProcessorSubscription subscription = TestUtils.subscription(rule.bootstrapServers(), builderConfigurer);
             TestProducer<K, V> producer = new TestProducer<>(rule.bootstrapServers(), topic, keySerializer, valueSerializer)) {
            producer.put(key, value);
            processLatch.await();
        }
    }

    @Test(timeout = 30000)
    public void testPrintableAsciiStringKeyValue() throws Exception {
        testRetryWithKeyValue(
                new PrintableAsciiStringSerializer(),
                "abc",
                new PrintableAsciiStringSerializer(),
                new StringDeserializer(),
                "value"
        );
    }

    @Test(timeout = 30000)
    public void testLongKeyValue() throws Exception {
        testRetryWithKeyValue(
                new LongSerializer(),
                123L,
                new LongSerializer(),
                new LongDeserializer(),
                100L
        );
    }
}
