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

package com.linecorp.decaton.processor.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.linecorp.decaton.client.DecatonClientBuilder.DefaultKafkaProducerSupplier;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.testing.KafkaClusterExtension;
import com.linecorp.decaton.testing.TestUtils;
import com.linecorp.decaton.testing.processor.ProcessedRecord;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee.GuaranteeType;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;
import com.linecorp.decaton.testing.processor.ProducedRecord;

import brave.Tracing;
import brave.kafka.clients.KafkaTracing;

public class BraveTracingTest {
    private KafkaTracing kafkaTracing;
    private Tracing tracing;

    public static class BraveTracePropagation implements ProcessingGuarantee {
        private final Map<String, String> producedTraceIds = new ConcurrentHashMap<>();
        private final Map<String, String> consumedTraceIds = new ConcurrentHashMap<>();
        private final Tracing tracing;

        public BraveTracePropagation(Tracing tracing) {
            this.tracing = tracing;
        }

        @Override
        public void onProduce(ProducedRecord record) {
            producedTraceIds.put(record.task().getId(), tracing.currentTraceContext().get().traceIdString());
        }

        @Override
        public void onProcess(TaskMetadata metadata, ProcessedRecord record) {
            consumedTraceIds.put(record.task().getId(), tracing.currentTraceContext().get().traceIdString());
        }

        @Override
        public void doAssert() {
            assertEquals(producedTraceIds, consumedTraceIds);
        }
    }

    @RegisterExtension
    public static KafkaClusterExtension rule = new KafkaClusterExtension();

    private String retryTopic;

    @BeforeEach
    public void setUp() {
        retryTopic = rule.admin().createRandomTopic(3, 3);
        tracing = Tracing.newBuilder().build();
        kafkaTracing = KafkaTracing.create(tracing);
    }

    @AfterEach
    public void tearDown() {
        rule.admin().deleteTopics(true, retryTopic);
    }

    @Test
    @Timeout(30)
    public void testTracePropagation() throws Exception {
        // scenario:
        //   * half of arrived tasks are retried once
        //   * after retried (i.e. retryCount() > 0), no more retry
        final DefaultKafkaProducerSupplier producerSupplier = new DefaultKafkaProducerSupplier();
        ProcessorTestSuite
                .builder(rule)
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    if (ctx.metadata().retryCount() == 0 && ThreadLocalRandom.current().nextBoolean()) {
                        ctx.retry();
                    }
                }))
                .producerSupplier(
                        bootstrapServers -> kafkaTracing.producer(TestUtils.producer(bootstrapServers)))
                .retryConfig(RetryConfig.builder()
                                        .retryTopic(retryTopic)
                                        .backoff(Duration.ofMillis(10))
                                        .producerSupplier(config -> kafkaTracing.producer(
                                                producerSupplier.getProducer(config)))
                                        .build())
                .tracingProvider(new BraveTracingProvider(kafkaTracing))
                // If we retry tasks, there's no guarantee about ordering nor serial processing
                .excludeSemantics(
                        GuaranteeType.PROCESS_ORDERING,
                        GuaranteeType.SERIAL_PROCESSING)
                .customSemantics(new BraveTracePropagation(tracing))
                .build()
                .run();
    }
}
