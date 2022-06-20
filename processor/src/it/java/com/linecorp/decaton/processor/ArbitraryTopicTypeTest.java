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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

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
    public static final KafkaClusterRule rule = new KafkaClusterRule();

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
             Producer<K, V> producer = TestUtils.producer(rule.bootstrapServers(), keySerializer, valueSerializer)) {
            producer.send(new ProducerRecord<>(topic, key, value));
            processLatch.await();
        }
    }

    @Test(timeout = 30000)
    public void testBytesKeyValue() throws Exception {
        testRetryWithKeyValue(
                new ByteArraySerializer(),
                "key".getBytes(StandardCharsets.UTF_8),
                new ByteArraySerializer(),
                new ByteArrayDeserializer(),
                "value".getBytes(StandardCharsets.UTF_8)
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
