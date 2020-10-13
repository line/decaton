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

package com.linecorp.decaton.processor.metrics;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.client.kafka.PrintableAsciiStringSerializer;
import com.linecorp.decaton.client.kafka.ProtocolBuffersKafkaSerializer;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessorProperties;
import com.linecorp.decaton.processor.ProcessorsBuilder;
import com.linecorp.decaton.processor.Property;
import com.linecorp.decaton.processor.StaticPropertySupplier;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;
import com.linecorp.decaton.protocol.Sample.HelloTask;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.TestUtils;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

public class MetricsTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    private static final int PARTITIONS = 3;

    private String topicName;

    @Before
    public void setUp() {
        topicName = rule.admin().createRandomTopic(PARTITIONS, 3);
    }

    @After
    public void tearDown() {
        rule.admin().deleteTopics(true, topicName);
    }

    @Test(timeout = 30000)
    public void testMetricsCleanup() throws Exception {
        // Any neighbor integration tests that ran in the same JVM could leak subscription unclosed
        // (e.g, test timeout) and that causes this test to fail unless we clear the registry here.
        Metrics.registry().clear();
        Metrics.AbstractMetrics.meterRefCounts.clear();

        CountDownLatch processLatch = new CountDownLatch(1);
        try (ProcessorSubscription subscription = TestUtils.subscription(
                rule.bootstrapServers(),
                builder -> builder.processorsBuilder(
                        ProcessorsBuilder.consuming(topicName, new ProtocolBuffersDeserializer<>(HelloTask.parser()))
                                         .thenProcess((context, task) -> processLatch.countDown())));
             DecatonClient<HelloTask> client = TestUtils.client(topicName, rule.bootstrapServers())) {
            client.put(null, HelloTask.getDefaultInstance());
            processLatch.await();
        }

        List<Meter> meters = Metrics.registry().getMeters();
        assertEquals(emptyList(), meters);
    }

    @Test(timeout = 30000)
    public void testDetectStuckPartitions() throws Exception {
        // Micrometer doesn't track values without at least one register implementation added
        Metrics.register(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));

        CountDownLatch processLatch = new CountDownLatch(PARTITIONS);
        DecatonProcessor<HelloTask> processor = (context, task) -> {
            context.deferCompletion(); // Leak defer completion
            processLatch.countDown();
        };

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, rule.bootstrapServers());
        try (ProcessorSubscription subscription = TestUtils.subscription(
                rule.bootstrapServers(),
                builder -> builder.processorsBuilder(ProcessorsBuilder
                                                             .consuming(topicName,
                                                                        new ProtocolBuffersDeserializer<>(HelloTask.parser()))
                                                             .thenProcess(processor))
                                  .properties(StaticPropertySupplier.of(
                                          Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 1),
                                          Property.ofStatic(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS, 1))));
             Producer<String, DecatonTaskRequest> producer =
                     new KafkaProducer<>(props,
                                         new PrintableAsciiStringSerializer(),
                                         new ProtocolBuffersKafkaSerializer<>())) {

            DecatonTaskRequest req = DecatonTaskRequest.newBuilder()
                                                       .setSerializedTask(
                                                               HelloTask.getDefaultInstance().toByteString())
                                                       .build();
            for (int i = 0; i < PARTITIONS; i++) {
                // What we need here is just simple round-robin but it's not possible with null keys anymore
                // since https://cwiki.apache.org/confluence/display/KAFKA/KIP-480%3A+Sticky+Partitioner
                producer.send(new ProducerRecord<>(topicName, i, null, req));
            }
            processLatch.await();

            // Wait in loop until the total pause count becomes the number of partitions.
            // Fail by test timeout if it doesn't.
            TestUtils.awaitCondition("total pause count should becomes " + PARTITIONS,
                                     () -> (long) Metrics.registry()
                                                         .find("decaton.partition.paused")
                                                         .tags("topic", topicName)
                                                         .gauges().stream()
                                                         .mapToDouble(Gauge::value).sum() == PARTITIONS);
        }
    }
}
