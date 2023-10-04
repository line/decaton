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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
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
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.processor.tracing.TestTracingProvider;
import com.linecorp.decaton.testing.KafkaClusterExtension;
import com.linecorp.decaton.testing.TestUtils;
import com.linecorp.decaton.testing.processor.ProcessedRecord;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee.GuaranteeType;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;
import com.linecorp.decaton.testing.processor.ProducedRecord;
import com.linecorp.decaton.testing.processor.TestTracingProducer;

public class TracingTest {
    public static class TracePropagation implements ProcessingGuarantee {
        private final Map<String, String> producedTraceIds = new ConcurrentHashMap<>();
        private final Map<String, String> consumedTraceIds = new ConcurrentHashMap<>();

        @Override
        public void onProduce(ProducedRecord record) {
            producedTraceIds.put(record.task().getId(),
                                 new String(
                                         record.headers().lastHeader(TestTracingProvider.TRACE_HEADER).value(),
                                         StandardCharsets.UTF_8));
        }

        @Override
        public void onProcess(TaskMetadata metadata, ProcessedRecord record) {
            consumedTraceIds.put(record.task().getId(), TestTracingProvider.getCurrentTraceId());
        }

        @Override
        public void doAssert() {
            assertEquals(producedTraceIds.keySet(), consumedTraceIds.keySet());
            for (Map.Entry<String, String> e: producedTraceIds.entrySet()) {
                assertThat(consumedTraceIds.get(e.getKey()), startsWith(e.getValue()));
            }
            TestTracingProvider.assertAllTracesWereClosed();
        }
    }
    @RegisterExtension
    public static KafkaClusterExtension rule = new KafkaClusterExtension();

    private String retryTopic;

    @BeforeEach
    public void setUp() {
        retryTopic = rule.admin().createRandomTopic(3, 3);
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
                .producerSupplier(config -> new TestTracingProducer(TestUtils.producer(config)))
                .retryConfig(RetryConfig.builder()
                                        .retryTopic(retryTopic)
                                        .backoff(Duration.ofMillis(10))
                                        .producerSupplier(config -> new TestTracingProducer(
                                                producerSupplier.getProducer(config)))
                                        .build())
                .tracingProvider(new TestTracingProvider())
                // If we retry tasks, there's no guarantee about ordering nor serial processing
                .excludeSemantics(
                        GuaranteeType.PROCESS_ORDERING,
                        GuaranteeType.SERIAL_PROCESSING)
                .customSemantics(new TracePropagation())
                .build()
                .run();
    }
}
