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

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;

import com.linecorp.decaton.client.internal.DecatonClientImpl;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;

/**
 * An interface to specify a custom instantiation function for {@link Producer}.
 * This interface can be implemented to inject custom Kafka {@link Producer} implementation if necessary.
 */
@FunctionalInterface
public interface KafkaProducerSupplier {
    /**
     * Instantiate Kafka {@link Producer} which is to be used to produce messages for request task to Decaton
     * processors.
     *
     * @param config an {@link Properties} which holds necessary information for Kafka producing. Some values
     * maybe overwritten by {@link DecatonClientImpl}
     *
     * @return an Kafka producer instance which implements {@link Producer}. The returned instance will be
     * closed along with {@link DecatonClient#close} being called.
     */
    Producer<byte[], DecatonTaskRequest> getProducer(Properties config);
}
