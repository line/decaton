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

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.client.DecatonClientBuilder.DefaultKafkaProducerSupplier;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.TestUtils;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee.GuaranteeType;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;

import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.brave.bridge.BraveCurrentTraceContext;
import io.micrometer.tracing.brave.bridge.BravePropagator;
import io.micrometer.tracing.brave.bridge.BraveTracer;

public class MicrometerTracingBraveBridgeTest {

    private Tracing braveTracing;
    private KafkaTracing braveKafkaTracing;
    private Tracer tracer;
    private String retryTopic;

    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    @Before
    public void setUp() {
        braveTracing = Tracing.newBuilder().build();
        braveKafkaTracing = KafkaTracing.create(braveTracing);
        tracer = new BraveTracer(braveTracing.tracer(),
                                 new BraveCurrentTraceContext(braveTracing.currentTraceContext()));
        retryTopic = rule.admin().createRandomTopic(3, 3);
    }

    @After
    public void tearDown() {
        rule.admin().deleteTopics(true, retryTopic);
    }

    @Test(timeout = 30000)
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
                // micrometer-tracing does not yet have kafka producer support, so use brave directly
                .producerSupplier(
                        bootstrapServers -> braveKafkaTracing.producer(TestUtils.producer(bootstrapServers)))
                .retryConfig(RetryConfig.builder()
                                        .retryTopic(retryTopic)
                                        .backoff(Duration.ofMillis(10))
                                        .producerSupplier(config -> braveKafkaTracing.producer(
                                                producerSupplier.getProducer(config)))
                                        .build())
                .tracingProvider(new MicrometerTracingProvider(tracer, new BravePropagator(braveTracing)))
                // If we retry tasks, there's no guarantee about ordering nor serial processing
                .excludeSemantics(GuaranteeType.PROCESS_ORDERING, GuaranteeType.SERIAL_PROCESSING)
                .customSemantics(new TracePropagationGuarantee(tracer))
                .build()
                .run();
    }
}
