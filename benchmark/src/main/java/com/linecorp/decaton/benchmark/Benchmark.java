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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.linecorp.decaton.benchmark.Runner.Config;
import com.linecorp.decaton.benchmark.Task.KafkaDeserializer;
import com.linecorp.decaton.testing.EmbeddedKafkaCluster;
import com.linecorp.decaton.testing.EmbeddedZooKeeper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Benchmark {
    private final BenchmarkConfig config;

    public Benchmark(BenchmarkConfig config) {
        this.config = config;
    }

    private static TemporaryTopic createTempTopic(String bootstrapServers) {
        String topic = "decatonbench-" + UUID.randomUUID();
        log.info("Creating temporary topic {} on {}", topic, bootstrapServers);
        return TemporaryTopic.create(bootstrapServers, topic);
    }

    void generateWorkload(String bootstrapServers, String topic) throws InterruptedException {
        RecordsGenerator generator = new RecordsGenerator(config.maxLatencyMs());
        int totalTasks = config.tasks() + config.warmupTasks();
        log.info("Generate {} tasks in total wheres {} are for warmup", totalTasks, config.warmupTasks());
        generator.generate(bootstrapServers, topic, totalTasks);
    }

    Recording runRecording(String bootstrapServers, String topic) throws InterruptedException {
        log.info("Loading runner {}", config.runner());
        Runner runner = Runner.fromClassName(config.runner());
        Config config = new Config(bootstrapServers,
                                   topic,
                                   new KafkaDeserializer(),
                                   this.config.configs());

        Recording recording = new Recording(this.config.tasks(), this.config.warmupTasks());
        log.info("Initializing runner {}", this.config.runner());
        runner.init(config, recording);
        try {
            generateWorkload(bootstrapServers, topic);
            recording.await(3, TimeUnit.MINUTES);
        } finally {
            try {
                runner.close();
            } catch (Exception e) {
                log.warn("Failed to close runner - {}", runner.getClass(), e);
            }
        }

        return recording;
    }

    public BenchmarkResult run() throws InterruptedException {
        final Recording recording;

        EmbeddedZooKeeper zooKeeper = null;
        EmbeddedKafkaCluster kafkaCluster = null;
        String bootstrapServers = config.bootstrapServers();
        if (bootstrapServers == null) {
            zooKeeper = new EmbeddedZooKeeper();
            kafkaCluster = new EmbeddedKafkaCluster(3, zooKeeper.zkConnectAsString());
            bootstrapServers = kafkaCluster.bootstrapServers();
        }
        log.info("Using kafka clusters: {}", bootstrapServers);
        try {
            TemporaryTopic topic = createTempTopic(bootstrapServers);
            try {
                recording = runRecording(bootstrapServers, topic.topic());
            } finally {
                try {
                    topic.close();
                } catch (Exception e) {
                    log.warn("Failed to cleanup temporary topic {}", topic.topic(), e);
                }
            }

            Thread.sleep(1000);
        } finally {
            if (kafkaCluster != null) {
                kafkaCluster.close();
                zooKeeper.close();
            }
        }

        return recording.computeResult();
    }
}
