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

package com.linecorp.decaton.processor;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.ProducerConfig;

import com.google.protobuf.MessageLite;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.processor.SubscriptionStateListener.State;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;
import com.linecorp.decaton.protobuf.ProtocolBuffersSerializer;

public class TestUtils {

    public static <T extends MessageLite> DecatonClient<T> client(String topic,
                                                                  String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "test-client");
        return DecatonClient.producing(topic, new ProtocolBuffersSerializer<T>())
                            .applicationId("test-application")
                            .instanceId("test-instance")
                            .producerConfig(props)
                            .build();
    }

    public static <T> ProcessorSubscription subscription(String bootstrapServers,
                                                         ProcessorsBuilder<T> processorsBuilder,
                                                         RetryConfig retryConfig) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("client.id", "test-processor");
        props.setProperty("group.id", "test-group");
        CountDownLatch initializationLatch = new CountDownLatch(1);
        ProcessorSubscription subscription = SubscriptionBuilder.newBuilder("test-subscription")
                                                                .consumerConfig(props)
                                                                .processorsBuilder(processorsBuilder)
                                                                .stateListener(state -> {
                                                                    if (state == State.RUNNING) {
                                                                        initializationLatch.countDown();
                                                                    }
                                                                })
                                                                .enableRetry(retryConfig)
                                                                .buildAndStart();
        try {
            initializationLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return subscription;
    }
}
