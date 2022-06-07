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

package com.linecorp.decaton.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.linecorp.decaton.client.internal.DecatonClientImpl;
import com.linecorp.decaton.client.kafka.ProtocolBuffersKafkaSerializer;
import com.linecorp.decaton.common.Serializer;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;

import lombok.AccessLevel;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * A builder interface to instantiate {@link DecatonClient}.
 * @param <T> type of task to be submitted by resulting client
 */
@Setter
@Accessors(fluent = true)
public class DecatonClientBuilder<T> {
    @Setter(AccessLevel.NONE)
    private final String topic;
    @Setter(AccessLevel.NONE)
    private final Serializer<T> serializer;

    private Properties producerConfig;
    private String applicationId;
    private String instanceId;
    private KafkaProducerSupplier producerSupplier;

    public static class DefaultKafkaProducerSupplier implements KafkaProducerSupplier {
        @Override
        public Producer<byte[], DecatonTaskRequest> getProducer(Properties config) {
            return new KafkaProducer<>(config,
                                       new ByteArraySerializer(),
                                       new ProtocolBuffersKafkaSerializer<>());
        }
    }

    DecatonClientBuilder(String topic, Serializer<T> serializer) {
        this.topic = topic;
        this.serializer = serializer;
    }

    private static String defaultInstanceId() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new IllegalStateException(
                    "instanceId was not given so tried to obtain localhost address but failed", e);
        }
    }

    /**
     * Instantiates {@link DecatonClient} based on configured parameters.
     * This method may throw any {@link RuntimeException} to indicate erroneous configuration.
     * @return an instance of {@link DecatonClientImpl}
     */
    public DecatonClient<T> build() {
        String instanceId = Optional.ofNullable(this.instanceId)
                                    .orElseGet(DecatonClientBuilder::defaultInstanceId);
        KafkaProducerSupplier producerSupplier = Optional.ofNullable(this.producerSupplier)
                                                         .orElseGet(DefaultKafkaProducerSupplier::new);

        return new DecatonClientImpl<>(
                Objects.requireNonNull(topic, "topic"),
                Objects.requireNonNull(serializer, "serializer"),
                Objects.requireNonNull(applicationId, "applicationId"),
                instanceId,
                Objects.requireNonNull(producerConfig, "producerConfig"),
                producerSupplier);
    }
}
