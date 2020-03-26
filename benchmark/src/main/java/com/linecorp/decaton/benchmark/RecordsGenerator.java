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
import java.util.Random;
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
    public static final int LATENCY_CYCLE = 1000;

    private final int[] latencies;

    public RecordsGenerator(int maxProcessLatencyMs) {
        latencies = new int[LATENCY_CYCLE];
        // Use maxProcessingLatencyMs as the seed so that we always get consistent
        // set of latencies.
        Random random = new Random(maxProcessLatencyMs);
        for (int i = 0; i < latencies.length; i++) {
            latencies[i] = random.nextInt(maxProcessLatencyMs);
        }
    }

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

    public void generate(String bootstrapServers, String topic, int numTasks) throws InterruptedException {
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
                int latencyMs = latencies[i % latencies.length];
                Task task = new Task(System.currentTimeMillis(), latencyMs);
                ProducerRecord<byte[], Task> record = new ProducerRecord<>(topic, null, task);
                Future<RecordMetadata> result = producer.send(record);
                results.addLast(result);

                consumeResults(results, false);
            }
        }
        consumeResults(results, true);
        log.info("Completed generating {} tasks", numTasks);
    }
}
