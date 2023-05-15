/*
 * Copyright 2023 LINE Corporation
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

package com.linecorp.decaton.processor.runtime;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.linecorp.decaton.processor.runtime.internal.RateLimiter;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder(toBuilder = true)
@Accessors(fluent = true)
public class PerKeyQuotaConfig {
    private static Map<String, PropertyDefinition<Long>> shapingRateDefinitions = new ConcurrentHashMap<>();

    private static final Duration DEFAULT_WINDOW = Duration.ofSeconds(10);
    public static final String DEFAULT_SHAPING_TOPIC_SUFFIX = "-shaping";

    /**
     * Optionally supplied the duration till detect bursting keys.
     * Shorter window enables quick detection at the risk of frequent bursting key isolation that may break per-key serial processing.
     * On the other hand, longer window enables stable detection at the risk of processors getting dominated by bursting keys for a while.
     */
    @NonNull
    @Builder.Default
    Duration window = DEFAULT_WINDOW;
    /**
     * Configure the shaping topics to tell the subscription to subscribe these topics.
     */
    @NonNull
    Function<String, Set<String>> shapingTopicsSupplier;
    /**
     * Configure the callback to determine the action when a key is detected as burst.
     */
    @NonNull
    Function<String, QuotaCallback> callbackSupplier;
    /**
     * Optionally supplied custom configuration for the {@link Producer} used to produce shaped tasks into shaping
     * topic. See {@link ProducerConfig}.
     */
    Properties producerConfig;
    /**
     * A factory function that can be used to inject arbitrary implementation of Kafka
     * {@link Producer} rather than the default {@link KafkaProducer}.
     * The supplied producer is closed along with {@link ProcessorSubscription}'s shutdown.
     */
    Function<Properties, Producer<byte[], byte[]>> producerSupplier;

    public static class PerKeyQuotaConfigBuilder {
        public PerKeyQuotaConfigBuilder shapingTopics(String... topics) {
            shapingTopicsSupplier = ignore -> new HashSet<>(Arrays.asList(topics));
            return this;
        }

        public PerKeyQuotaConfigBuilder callback(QuotaCallback callback) {
            callbackSupplier = topic -> callback;
            return this;
        }
    }

    @FunctionalInterface
    public interface QuotaCallback extends AutoCloseable {
        @Value
        @Builder
        @Accessors(fluent = true)
        class Metrics {
            /**
             * Observed processing rate for the key
             */
            double rate;
        }

        /**
         * Decide the destination topic for shaping against the key that is detected as bursting.
         * Note that whenever this callback throws, the task will be marked as completed immediately (i.e. will be committed)
         * without sending it to the shaping topic nor queueing to the processor.
         * @param record record which have bursted key
         * @param metrics observed metric for the key
         * @return destination topic for shaping
         */
        String apply(ConsumerRecord<byte[], byte[]> record, Metrics metrics);

        @Override
        default void close() {
            // do nothing
        }
    }

    /**
     * Create {@link PerKeyQuotaConfig} to shape tasks for bursting keys with default shaping topic name.
     * @return {@link PerKeyQuotaConfig} instance for shaping
     */
    public static PerKeyQuotaConfig shape() {
        return builder()
                .shapingTopicsSupplier(topic -> Collections.singleton(defaultShapingTopic(topic)))
                .callbackSupplier(topic -> {
                    String shapingTopic = defaultShapingTopic(topic);
                    return (key, metrics) -> shapingTopic;
                })
                .build();
    }

    private static String defaultShapingTopic(String originalTopic) {
        return originalTopic + DEFAULT_SHAPING_TOPIC_SUFFIX;
    }

    /**
     * Returns {@link PropertyDefinition} to optionally configure the processing rate per second for the shaping topic.
     * @param topic shaping topic name
     * @return definition to configure the property
     */
    public static PropertyDefinition<Long> shapingRateProperty(String topic) {
        String propertyName = "decaton.shaping.topic.processing.rate.per.partition." + topic;
        return shapingRateDefinitions.computeIfAbsent(propertyName, key ->
                PropertyDefinition.define(propertyName, Long.class,
                                          RateLimiter.UNLIMITED,
                                          v -> v instanceof Long
                                               && RateLimiter.UNLIMITED <= (long) v
                                               && (long) v <= RateLimiter.MAX_RATE));
    }
}
