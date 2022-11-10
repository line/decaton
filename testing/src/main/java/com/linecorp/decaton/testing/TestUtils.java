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

import static com.linecorp.decaton.processor.runtime.ProcessorProperties.CONFIG_BIND_CLIENT_METRICS;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.google.protobuf.MessageLite;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.client.kafka.ProtocolBuffersKafkaSerializer;
import com.linecorp.decaton.common.Serializer;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;
import com.linecorp.decaton.processor.runtime.SubscriptionStateListener;
import com.linecorp.decaton.processor.runtime.SubscriptionStateListener.State;
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
    public static final Duration DEFINITELY_TOO_SLOW = Duration.ofSeconds(20);

    /**
     * A helper to instantiate {@link DecatonClient} for producing protobuf tasks with preset configurations
     *
     * @param topic destination topic
     * @param bootstrapServers bootstrap servers to connect
     * @param <T> type of tasks
     * @return {@link DecatonClient} instance
     */
    public static <T extends MessageLite> DecatonClient<T> client(String topic,
                                                                  String bootstrapServers) {
        return client(topic, bootstrapServers, new ProtocolBuffersSerializer<>());
    }

    /**
     * A helper to instantiate {@link DecatonClient} for arbitrary task type with preset configurations
     *
     * @param topic destination topic
     * @param bootstrapServers bootstrap servers to connect
     * @param serializer {@link Serializer} for the task
     * @param <T> type of tasks
     * @return {@link DecatonClient} instance
     */
    public static <T> DecatonClient<T> client(String topic,
                                              String bootstrapServers,
                                              Serializer<T> serializer) {
        return DecatonClient.producing(topic, serializer)
                            .applicationId("test-application")
                            .instanceId("test-instance")
                            .producerConfig(defaultProducerProps(bootstrapServers))
                            .build();
    }

    /**
     * A helper to instantiate {@link Producer} with preset configurations
     * and default serializers
     *
     * @param bootstrapServers bootstrap servers to connect
     * @return {@link Producer} instance with preset configurations
     */
    public static Producer<byte[], DecatonTaskRequest> producer(String bootstrapServers) {
        return producer(bootstrapServers,
                        new ByteArraySerializer(),
                        new ProtocolBuffersKafkaSerializer<>());
    }

    /**
     * A helper to instantiate {@link Producer} with preset configurations
     * and custom key/value serializers
     *
     * @param bootstrapServers bootstrap servers to connect
     * @param keySerializer serializer to serialize key into bytes
     * @param valueSerializer serializer to serialize value into bytes
     * @return {@link Producer} instance with preset configurations
     */
    public static <K, V> Producer<K, V> producer(String bootstrapServers,
                                                 org.apache.kafka.common.serialization.Serializer<K> keySerializer,
                                                 org.apache.kafka.common.serialization.Serializer<V> valueSerializer) {
        return new KafkaProducer<>(defaultProducerProps(bootstrapServers),
                                   keySerializer,
                                   valueSerializer);
    }

    /**
     * A helper to instantiate {@link ProcessorSubscription} with preset configurations
     * and unique subscription id assigned
     *
     * @param bootstrapServers bootstrap servers to connect
     * @param builderConfigurer configure subscription builder to fit test requirements
     * @return {@link ProcessorSubscription} instance which is already running with unique subscription id assigned
     */
    public static ProcessorSubscription subscription(String bootstrapServers,
                                                     Consumer<SubscriptionBuilder> builderConfigurer)
            throws InterruptedException, TimeoutException {
        return subscription("subscription-" + sequence(),
                            bootstrapServers,
                            builderConfigurer,
                            null);
    }

    /**
     * A helper to instantiate {@link ProcessorSubscription} with preset configurations.
     * This method returns after a subscription has been transitioned to {@link State#RUNNING} state.
     *
     * @param subscriptionId subscription id of the instance
     * @param bootstrapServers bootstrap servers to connect
     * @param builderConfigurer configure subscription builder to fit test requirements
     * @param additionalConsumerConfig additional configs to be passed when instantiating consumer
     * @return {@link ProcessorSubscription} instance which is already running
     */
    public static ProcessorSubscription subscription(String subscriptionId,
                                                     String bootstrapServers,
                                                     Consumer<SubscriptionBuilder> builderConfigurer,
                                                     Properties additionalConsumerConfig)
            throws InterruptedException, TimeoutException {
        AtomicReference<SubscriptionStateListener> stateListenerRef = new AtomicReference<>();
        CountDownLatch initializationLatch = new CountDownLatch(1);
        SubscriptionStateListener outerStateListener = state -> {
            if (state == State.RUNNING) {
                initializationLatch.countDown();
            }
            Optional.ofNullable(stateListenerRef.get()).ifPresent(s -> s.onChange(state));
        };

        SubscriptionBuilder builder = new SubscriptionBuilder(subscriptionId) {
            @Override
            public SubscriptionBuilder stateListener(SubscriptionStateListener stateListener) {
                if (stateListener != outerStateListener) {
                    stateListenerRef.set(stateListener);
                }
                return super.stateListener(stateListener);
            }
        };
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "test-" + subscriptionId);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP_ID);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        if (additionalConsumerConfig != null) {
            props.putAll(additionalConsumerConfig);
        }

        builderConfigurer.accept(builder);
        builder.consumerConfig(props)
               .addProperties(StaticPropertySupplier.of(Property.ofStatic(CONFIG_BIND_CLIENT_METRICS, true)))
               .stateListener(outerStateListener);
        ProcessorSubscription subscription = builder.buildAndStart();

        if (!initializationLatch.await(DEFINITELY_TOO_SLOW.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Initialization did not complete within " + DEFINITELY_TOO_SLOW);
        }
        return subscription;
    }

    /**
     * Wait indefinitely for a condition to be met
     * @param message assertion message
     * @param condition expected condition to be met
     */
    public static void awaitCondition(String message,
                                      BooleanSupplier condition) {
        awaitCondition(message, condition, Long.MAX_VALUE);
    }

    /**
     * Wait for a condition to be met up to specified timeout
     * @param message assertion message
     * @param condition expected condition to be met
     * @param timeoutMillis max duration to wait
     */
    public static void awaitCondition(String message,
                                      BooleanSupplier condition,
                                      long timeoutMillis) {
        long start = System.nanoTime();
        while (!condition.getAsBoolean()) {
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            if (elapsedMillis >= timeoutMillis) {
                throw new AssertionError(message);
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }
    }
}
