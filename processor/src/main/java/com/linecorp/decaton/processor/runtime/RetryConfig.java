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
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.linecorp.decaton.client.DecatonClientBuilder.DefaultKafkaProducerSupplier;
import com.linecorp.decaton.client.KafkaProducerSupplier;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder(toBuilder = true)
@Accessors(fluent = true)
public class RetryConfig {
    public static final String DEFAULT_RETRY_TOPIC_SUFFIX = "-retry";

    /**
     * Time to backoff before retry processing of queued tasks.
     */
    @NonNull
    Duration backoff;
    /**
     * Optionally supplied custom retry topic name. Unless specified, the name adding "-retry" suffix to the
     * original topic name is used as the default.
     */
    String retryTopic;
    /**
     * Optionally supplied custom configuration for the {@link Producer} used to produce retry tasks into retry
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

    /**
     * Create an instance of {@link RetryConfig} only with {@link #backoff} set.
     * This is a sugar for {@code RetryConfig.builder().backoff(...).build()} since in the most cases only the
     * backoff parameter is set.
     *
     * @param backoff see {@link #backoff}.
     * @return an instance of {@link RetryConfig}.
     */
    public static RetryConfig withBackoff(Duration backoff) {
        return builder().backoff(backoff).build();
    }

    public String retryTopicOrDefault(String originalTopic) {
        if (retryTopic == null) {
            return originalTopic + DEFAULT_RETRY_TOPIC_SUFFIX;
        } else {
            return retryTopic;
        }
    }
}
