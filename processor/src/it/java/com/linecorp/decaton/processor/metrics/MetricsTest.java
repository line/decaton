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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.client.kafka.PrintableAsciiStringSerializer;
import com.linecorp.decaton.client.internal.TaskMetadataUtil;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;
import com.linecorp.decaton.protocol.Decaton.TaskMetadataProto;
import com.linecorp.decaton.protocol.Sample.HelloTask;
import com.linecorp.decaton.testing.KafkaClusterExtension;
import com.linecorp.decaton.testing.TestUtils;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

public class MetricsTest {
    @RegisterExtension
    public static KafkaClusterExtension rule = new KafkaClusterExtension();

    private static final int PARTITIONS = 3;

    private String topicName;

    @BeforeEach
    public void setUp() {
        topicName = rule.admin().createRandomTopic(PARTITIONS, 3);
    }

    @AfterEach
    public void tearDown() {
        rule.admin().deleteTopics(true, topicName);
    }

    @Test
    @Timeout(30)
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

    @Test
    @Timeout(30)
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
                                  .addProperties(StaticPropertySupplier.of(
                                          Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 1),
                                          Property.ofStatic(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS, 1))));
             Producer<String, byte[]> producer =
                     new KafkaProducer<>(props,
                                         new PrintableAsciiStringSerializer(),
                                         new ByteArraySerializer())) {

            for (int i = 0; i < PARTITIONS; i++) {
                // What we need here is just simple round-robin but it's not possible with null keys anymore
                // since https://cwiki.apache.org/confluence/display/KAFKA/KIP-480%3A+Sticky+Partitioner
                producer.send(new ProducerRecord<>(topicName, i, null, HelloTask.getDefaultInstance().toByteArray()));
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

    @Test
    @Timeout(30)
    public void testTasksMetrics() throws Exception {
        Metrics.register(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
        CountDownLatch processLatch = new CountDownLatch(4);
        try (ProcessorSubscription subscription = TestUtils.subscription(
                rule.bootstrapServers(),
                builder -> builder.processorsBuilder(
                        ProcessorsBuilder.consuming(topicName, new ProtocolBuffersDeserializer<>(HelloTask.parser()))
                                .thenProcess((context, task) -> {
                                    processLatch.countDown();
                                    switch (new String(context.key())) {
                                        case "sync-success":
                                            break;
                                        case "sync-fail":
                                            throw new RuntimeException("exception!");
                                        case "async-success":
                                            context.deferCompletion().complete();
                                            break;
                                        case "async-fail":
                                            CompletableFuture<Void> future = new CompletableFuture<>();
                                            future.completeExceptionally(new RuntimeException("fail"));
                                            context.deferCompletion().completeWith(future);
                                            break;
                                        default:
                                            fail("unexpected key");
                                    }
                                })));
             DecatonClient<HelloTask> client = TestUtils.client(topicName, rule.bootstrapServers())) {
            client.put("sync-success", HelloTask.getDefaultInstance());
            client.put("sync-fail", HelloTask.getDefaultInstance());
            client.put("async-success", HelloTask.getDefaultInstance());
            client.put("async-fail", HelloTask.getDefaultInstance());
            processLatch.await();

            // count all tasks regardless of the result
            TestUtils.awaitCondition("total processed task count should becomes 4",
                    () -> Metrics.registry()
                                 .find("decaton.tasks.processed")
                                 .tags("topic", topicName)
                                 .counters()
                                 .stream()
                                 .mapToDouble(Counter::count)
                                 .sum() == 4.0,
                                     10000);
            // count synchronous failure only
            TestUtils.awaitCondition("total error task count should becomes 1",
                    () -> Metrics.registry()
                             .find("decaton.tasks.error")
                             .tags("topic", topicName)
                             .counters()
                             .stream()
                             .mapToDouble(Counter::count)
                             .sum() == 1.0,
                                     10000);
        }
    }

    @Test
    @Timeout(30)
    public void testDeferredCompletionLeak() throws Exception {
        Metrics.register(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));

        int count = 10;
        CountDownLatch processLatch = new CountDownLatch(count);
        DecatonProcessor<HelloTask> processor = (context, task) -> {
            processLatch.countDown();
            if (task.getAge() % 3 == 2) {
                context.deferCompletion();
            }
        };

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, rule.bootstrapServers());
        try (ProcessorSubscription subscription = TestUtils.subscription(
                rule.bootstrapServers(),
                builder -> builder.processorsBuilder(ProcessorsBuilder
                                                             .consuming(topicName,
                                                                        new ProtocolBuffersDeserializer<>(HelloTask.parser()))
                                                             .thenProcess(processor))
                                  .addProperties(StaticPropertySupplier.of(
                                          Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 2),
                                          Property.ofStatic(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS, 3))));
             Producer<String, byte[]> producer =
                     new KafkaProducer<>(props,
                                         new PrintableAsciiStringSerializer(),
                                         new ByteArraySerializer())) {
            for (int i = 0; i < count; i++) {
                // All requests will be sent on partition 0 for simplicity
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                        topicName, 0, null, HelloTask.newBuilder().setAge(i).build().toByteArray());
                // simulate produces from DecatonClient
                TaskMetadataUtil.writeAsHeader(TaskMetadataProto.getDefaultInstance(), record.headers());
                producer.send(record);
            }
            processLatch.await();

            // offset 2 will not be committed
            TestUtils.awaitCondition("last committed offset should becomes 1",
                                     () -> Metrics.registry()
                                                  .find("decaton.offset.last.committed")
                                                  .tags("topic", topicName, "partition", "0")
                                                  .gauge().value() == 1.0);
            TestUtils.awaitCondition("latest consumed offset should becomes 9",
                                     () -> Metrics.registry()
                                                  .find("decaton.offset.latest.consumed")
                                                  .tags("topic", topicName, "partition", "0")
                                                  .gauge().value() == 9.0);
        }}
}
