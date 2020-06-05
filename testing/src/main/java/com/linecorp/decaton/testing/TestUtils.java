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

package com.linecorp.decaton.testing;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.google.protobuf.MessageLite;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.client.kafka.PrintableAsciiStringSerializer;
import com.linecorp.decaton.client.kafka.ProtocolBuffersKafkaSerializer;
import com.linecorp.decaton.common.Serializer;
import com.linecorp.decaton.processor.ProcessorsBuilder;
import com.linecorp.decaton.processor.PropertySupplier;
import com.linecorp.decaton.processor.SubscriptionStateListener.State;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;
import com.linecorp.decaton.protobuf.ProtocolBuffersSerializer;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;

public class TestUtils {
    private static final AtomicInteger sequence = new AtomicInteger(0);

    // generate a monotonic value to be added as part of client.id to ensure unique
    private static int sequence() {
        return sequence.getAndIncrement();
    }

    private static Properties defaultProducerProps(String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "test-client-" + sequence());
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");
        return props;
    }

    public static final String DEFAULT_GROUP_ID = "test-group";

    public static <T extends MessageLite> DecatonClient<T> client(String topic,
                                                                  String bootstrapServers) {
        return client(topic, bootstrapServers, new ProtocolBuffersSerializer<>());
    }

    public static <T> DecatonClient<T> client(String topic,
                                              String bootstrapServers,
                                              Serializer<T> serializer) {
        return DecatonClient.producing(topic, serializer)
                            .applicationId("test-application")
                            .instanceId("test-instance")
                            .producerConfig(defaultProducerProps(bootstrapServers))
                            .build();
    }

    public static Producer<String, DecatonTaskRequest> producer(String bootstrapServers) {
        return new KafkaProducer<>(defaultProducerProps(bootstrapServers),
                                   new PrintableAsciiStringSerializer(),
                                   new ProtocolBuffersKafkaSerializer<>());
    }

    public static <T> ProcessorSubscription subscription(String bootstrapServers,
                                                         ProcessorsBuilder<T> processorsBuilder,
                                                         RetryConfig retryConfig,
                                                         PropertySupplier propertySupplier) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "test-processor" + sequence());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP_ID);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        CountDownLatch initializationLatch = new CountDownLatch(1);
        SubscriptionBuilder builder = SubscriptionBuilder.newBuilder("test-subscription")
                                                         .consumerConfig(props)
                                                         .processorsBuilder(processorsBuilder)
                                                         .stateListener(state -> {
                                                             if (state == State.RUNNING) {
                                                                 initializationLatch.countDown();
                                                             }
                                                         });
        if (retryConfig != null) {
            builder.enableRetry(retryConfig);
        }
        if (propertySupplier != null) {
            builder.properties(propertySupplier);
        }
        ProcessorSubscription subscription = builder.buildAndStart();

        try {
            initializationLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return subscription;
    }

    public static void awaitCondition(String message,
                                      Supplier<Boolean> condition) {
        awaitCondition(message, condition, Long.MAX_VALUE);
    }

    public static void awaitCondition(String message,
                                      Supplier<Boolean> condition,
                                      long timeoutMillis) {
        long t = System.currentTimeMillis();
        while (!condition.get()) {
            long now = System.currentTimeMillis();
            timeoutMillis -= now - t;
            t = now;
            if (timeoutMillis <= 0) {
                throw new AssertionError(message);
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
