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

import com.linecorp.decaton.benchmark.Execution.Stage;
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

    private static void generateWorkload(String bootstrapServers, String topic, int numTasks, int latencyMs) {
        RecordsGenerator generator = new RecordsGenerator();
        log.info("Generate {} tasks with latency {} ms", numTasks, latencyMs);
        try {
            generator.generate(bootstrapServers, topic, numTasks, latencyMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private BenchmarkResult runRecording(String bootstrapServers, String topic) throws InterruptedException {
        Execution.Config config = new Execution.Config(bootstrapServers, topic, this.config);
        Execution execution = new ForkingExecution();
        return execution.execute(config, stage -> {
            if (stage == Stage.READY_WARMUP) {
                log.info("Start warmup with {} tasks", this.config.warmupTasks());
                generateWorkload(bootstrapServers, topic, this.config.warmupTasks(), 0);
            } else if (stage == Stage.READY) {
                log.info("Start real run with {} tasks", this.config.tasks());
                generateWorkload(bootstrapServers, topic, this.config.tasks(), this.config.simulateLatencyMs());
            }
        });
    }

    public BenchmarkResult run() throws InterruptedException {
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
                return runRecording(bootstrapServers, topic.topic());
            } finally {
                try {
                    topic.close();
                    // Need to wait a while until all replicas of the deleted topic to disappear from brokers.
                    Thread.sleep(1000);
                } catch (Exception e) {
                    log.warn("Failed to cleanup temporary topic {}", topic.topic(), e);
                }
            }
        } finally {
            if (kafkaCluster != null) {
                kafkaCluster.close();
                zooKeeper.close();
            }
        }
    }
}
