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

package com.linecorp.decaton.processor.runtime;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.linecorp.decaton.client.DecatonClientBuilder.DefaultKafkaProducerSupplier;
import com.linecorp.decaton.client.KafkaProducerSupplier;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback.Action;
import com.linecorp.decaton.processor.runtime.internal.RateLimiter;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder(toBuilder = true)
@Accessors(fluent = true)
public class PerKeyQuotaConfig {
    private static final Duration DEFAULT_WINDOW = Duration.ofSeconds(10);
    public static final String DEFAULT_SHAPING_TOPIC_SUFFIX = "-shaping";

    /**
     * Optionally supplied the duration till detect bursting keys that have excessive processing rate
     * than {@link ProcessorProperties#CONFIG_PER_KEY_QUOTA_RATE}.
     * Shorter window enables quick detection at the risk of frequent bursting key isolation (that may break per-key serial processing).
     */
    Duration window;
    /**
     * Configure the shaping topics to tell the subscription to subscribe these topics.
     */
    Function<String, Set<String>> shapingTopicsSupplier;
    /**
     * Configure the callback to determine the action when a key is detected as burst.
     */
    Function<String, QuotaCallback<?>> callbackSupplier;
    /**
     * Optionally supplied custom configuration for the {@link Producer} used to produce shaped tasks into shaping
     * topic. See {@link ProducerConfig}.
     */
    Properties producerConfig;
    /**
     * A {@link KafkaProducerSupplier} that can be used to inject arbitrary implementation of Kafka
     * {@link Producer} rather than the default {@link KafkaProducer}.
     * The supplied producer is closed along with {@link ProcessorSubscription}'s shutdown.
     * If omitted, {@link DefaultKafkaProducerSupplier} is used.
     */
    KafkaProducerSupplier producerSupplier;

    public static class PerKeyQuotaConfigBuilder {
        public PerKeyQuotaConfigBuilder shapingTopics(String... topics) {
            shapingTopicsSupplier = topic -> new HashSet<>(Arrays.asList(topics));
            return this;
        }

        @SuppressWarnings("unchecked")
        public <T> PerKeyQuotaConfigBuilder callback(QuotaCallback<T> callback) {
            callbackSupplier = topic -> (QuotaCallback<Object>) callback;
            return this;
        }
    }

    @FunctionalInterface
    public interface QuotaCallback<T> {
        enum ActionType {
            /**
             * Drop the task
             */
            Drop,
            /**
             * Queue the task to the shaping topic without passing it to the processors
             */
            Shape,
        }

        @Value
        @Accessors(fluent = true)
        class Action {
            ActionType type;
            String topic;

            public static Action shape(String topic) {
                return new Action(ActionType.Shape, topic);
            }

            public static Action drop() {
                return new Action(ActionType.Drop, null);
            }
        }

        @Value
        @Accessors(fluent = true)
        class Observation {
            /**
             * Observed processing rate for the key
             */
            double rate;
        }

        Action apply(byte[] key, TaskMetadata metadata, T task, Observation observation);
    }

    public static PerKeyQuotaConfig shape() {
        return builder()
                .shapingTopicsSupplier(topic -> Collections.singleton(defaultShapingTopic(topic)))
                .callbackSupplier(topic -> {
                    String shapingTopic = defaultShapingTopic(topic);
                    return (key, metadata, task, observation) -> Action.shape(shapingTopic);
                })
                .build();
    }

    public static PerKeyQuotaConfig drop() {
        return builder()
                .callbackSupplier(topic -> (key, metadata, task, observation) -> Action.drop())
                .build();
    }

    private static String defaultShapingTopic(String originalTopic) {
        return originalTopic + DEFAULT_SHAPING_TOPIC_SUFFIX;
    }

    /**
     * Returns {@link PropertyDefinition} to optionally configure the processing rate for the shaping topic.
     * Unless specified, the value of {@link ProcessorProperties#CONFIG_PER_KEY_QUOTA_RATE} is used to limit the
     * processing rate of shaping topics.
     */
    public static PropertyDefinition<Long> topicProcessingRateProperty(String topic) {
        String propertyName = "decaton.shaping.topic.processing.rate.per.partition." + topic;
        return PropertyDefinition.define(propertyName, Long.class,
                                         RateLimiter.UNLIMITED,
                                         v -> v instanceof Long
                                              && RateLimiter.UNLIMITED <= (long) v
                                              && (long) v <= RateLimiter.MAX_RATE);
    }
}
