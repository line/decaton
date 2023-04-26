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

package com.linecorp.decaton.client.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.client.KafkaProducerSupplier;
import com.linecorp.decaton.client.PutTaskResult;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;

/**
 * A raw interface to put a built {@link DecatonTaskRequest} directly.
 * This interface isn't expected to be used by applications unless it's really necessary.
 * Use {@link DecatonClient} to put task into a Decaton topic instead.
 */
public class DecatonTaskProducer implements AutoCloseable {
    private static final Map<String, String> presetProducerConfig;

    static {
        presetProducerConfig = new HashMap<>();
        presetProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        presetProducerConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    }

    private final Producer<byte[], DecatonTaskRequest> producer;
    private final String topic;

    private static Properties completeProducerConfig(Properties producerConfig) {
        final Properties result = new Properties();
        result.putAll(presetProducerConfig);
        result.putAll(producerConfig); // intentional overwrite
        return result;
    }

    public DecatonTaskProducer(String topic, Properties producerConfig,
                               KafkaProducerSupplier producerSupplier) {
        Properties completeProducerConfig = completeProducerConfig(producerConfig);
        producer = producerSupplier.getProducer(completeProducerConfig);
        this.topic = topic;
    }

    public CompletableFuture<PutTaskResult> sendRequest(byte[] key, DecatonTaskRequest request,
                                                        Integer partition) {
        ProducerRecord<byte[], DecatonTaskRequest> record = new ProducerRecord<>(topic, partition, key, request);
        return sendRequest(record);
    }

    private CompletableFuture<PutTaskResult> sendRequest(ProducerRecord<byte[], DecatonTaskRequest> record) {
        CompletableFuture<PutTaskResult> result = new CompletableFuture<>();
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                PutTaskResult putResult = new PutTaskResult(metadata);
                result.complete(putResult);
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
