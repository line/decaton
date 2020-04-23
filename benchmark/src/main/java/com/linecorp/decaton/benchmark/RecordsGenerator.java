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

package com.linecorp.decaton.benchmark;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RecordsGenerator {
    private static void consumeResults(Deque<Future<RecordMetadata>> results, boolean await)
            throws InterruptedException {
        Future<RecordMetadata> future;
        while ((future = results.peekFirst()) != null) {
            if (!future.isDone() && !await) {
                break;
            }
            results.pollFirst();
            try {
                future.get();
            } catch (ExecutionException e) {
                throw new RuntimeException("failed producing record", e);
            }
        }
    }

    private static Future<RecordMetadata> produce(
            Producer<byte[], Task> producer, String topic, int latencyMs) {
        Task task = new Task(System.currentTimeMillis(), latencyMs);
        ProducerRecord<byte[], Task> record = new ProducerRecord<>(topic, null, task);
        return producer.send(record);
    }

    public void generate(String bootstrapServers, String topic, int numTasks, int simulateLatencyMs)
            throws InterruptedException {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "decaton-benchmark");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

        log.info("Start generating {} tasks", numTasks);
        Deque<Future<RecordMetadata>> results = new ArrayDeque<>();
        try (Producer<byte[], Task> producer =
                     new KafkaProducer<>(props, new ByteArraySerializer(), new Task.KafkaSerializer())) {
            for (int i = 0; i < numTasks; i++) {
                results.addLast(produce(producer, topic, simulateLatencyMs));
                consumeResults(results, false);
            }
        }
        consumeResults(results, true);
        log.info("Completed generating {} tasks", numTasks);
    }
}
