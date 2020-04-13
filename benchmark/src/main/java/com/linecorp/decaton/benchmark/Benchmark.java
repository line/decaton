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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.linecorp.decaton.benchmark.BenchmarkResult.Performance;
import com.linecorp.decaton.benchmark.BenchmarkResult.ResourceUsage;
import com.linecorp.decaton.benchmark.ResourceTracker.TrackingValues;
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

    private void generateWorkload(String bootstrapServers, String topic) throws InterruptedException {
        RecordsGenerator generator = new RecordsGenerator(config.simulateLatencyMs());
        int totalTasks = config.tasks() + config.warmupTasks();
        log.info("Generate {} tasks in total wheres {} are for warmup", totalTasks, config.warmupTasks());
        generator.generate(bootstrapServers, topic, totalTasks);
    }

    private BenchmarkResult runRecording(String bootstrapServers, String topic) throws InterruptedException {
        log.info("Loading runner {}", config.runner());
        Runner runner = Runner.fromClassName(config.runner());
        Config config = new Config(bootstrapServers,
                                   topic,
                                   new KafkaDeserializer(),
                                   this.config.params());

        Recording recording = new Recording(this.config.tasks(), this.config.warmupTasks());
        ResourceTracker resourceTracker = new ResourceTracker();
        log.info("Initializing runner {}", this.config.runner());
        runner.init(config, recording, resourceTracker);
        final Map<Long, TrackingValues> resourceUsageReport;
        try {
            generateWorkload(bootstrapServers, topic);
            if (!recording.await(3, TimeUnit.MINUTES)) {
                throw new RuntimeException("timeout on awaiting benchmark to complete");
            }
            resourceUsageReport = resourceTracker.report();
        } finally {
            try {
                runner.close();
            } catch (Exception e) {
                log.warn("Failed to close runner - {}", runner.getClass(), e);
            }
        }

        return assembleResult(recording, resourceUsageReport);
    }

    private static BenchmarkResult assembleResult(Recording recording,
                                                  Map<Long, TrackingValues> resourceUsageReport) {
        Performance performance = recording.computeResult();
        int threads = resourceUsageReport.size();
        TrackingValues resourceValues =
                resourceUsageReport.values().stream()
                                   .reduce(new TrackingValues(0, 0),
                                           (a, b) -> new TrackingValues(
                                                   a.cpuTime() + b.cpuTime(),
                                                   a.allocatedBytes() + b.allocatedBytes()));
        ResourceUsage resource = new ResourceUsage(threads,
                                                   resourceValues.cpuTime(),
                                                   resourceValues.allocatedBytes());
        return new BenchmarkResult(performance, resource);
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
