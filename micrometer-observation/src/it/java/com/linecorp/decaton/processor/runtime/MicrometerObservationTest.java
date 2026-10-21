/*
 * Copyright 2026 LY Corporation
 *
 * LY Corporation licenses this file to you under the Apache License,
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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.AfterEach;
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
import brave.propagation.B3SingleFormat;
import brave.propagation.TraceContextOrSamplingFlags;
import io.micrometer.observation.tck.ObservationContextAssert;
import io.micrometer.observation.tck.TestObservationRegistry;

public class MicrometerObservationTest {
    private final TestObservationRegistry registry = TestObservationRegistry.create();
    private final String retryTopic = rule.admin().createRandomTopic(3, 3);

    /**
     * As an example, run tests using the Brave implementation.
     * From Decaton’s perspective, as long as Micrometer Observation is used correctly,
     * it’s OK—so writing tests with either Brave or OpenTelemetry is sufficient.
     */
    private final Tracing tracing = Tracing.newBuilder().build();
    private final KafkaTracing kafkaTracing = KafkaTracing.create(tracing);

    @RegisterExtension
    public static KafkaClusterExtension rule = new KafkaClusterExtension();

    @AfterEach
    public void tearDown() {
        tracing.close();
        rule.admin().deleteTopics(true, retryTopic);
    }

    @Test
    @Timeout(30)
    public void test() throws Exception {
        // scenario:
        //   * half of arrived tasks are retried once
        //   * after retried (i.e. retryCount() > 0), no more retry
        final DefaultKafkaProducerSupplier producerSupplier = new DefaultKafkaProducerSupplier();
        ProcessorTestSuite
                .builder(rule)
                .configureProcessorsBuilder(
                        builder -> builder.thenProcess((ctx, task) -> {
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
                .tracingProvider(new MicrometerObservationProvider(registry))
                // If we retry tasks, there's no guarantee about ordering nor serial processing
                .excludeSemantics(GuaranteeType.PROCESS_ORDERING, GuaranteeType.SERIAL_PROCESSING)
                .customSemantics(new MyPropagationGuarantee(registry, tracing))
                .build()
                .run();
    }

    private static class MyPropagationGuarantee implements ProcessingGuarantee {
        private final TestObservationRegistry registry;
        private final Tracing tracing;
        private final Map<String, String> producedTraceIds = new ConcurrentHashMap<>();
        private final Map<String, String> consumedTraceIds = new ConcurrentHashMap<>();
        private final ConcurrentLinkedDeque<DecatonProcessorReceiverContext> consumedContexts =
                new ConcurrentLinkedDeque<>();

        MyPropagationGuarantee(TestObservationRegistry registry, Tracing tracing) {
            this.registry = registry;
            this.tracing = tracing;
        }

        @Override
        public void onProduce(ProducedRecord record) {
            producedTraceIds.put(record.task().getId(), tracing.currentTraceContext().get().traceIdString());
        }

        @Override
        public void onProcess(TaskMetadata metadata, ProcessedRecord record) {
            // It’s sufficient to verify that tracing headers (B3 headers in the case of Brave) can be accessed
            // via ReceiverContext’s Propagator.Getter.
            // The actual tracing implementation is handled by Micrometer Observation.
            final DecatonProcessorReceiverContext context =
                    (DecatonProcessorReceiverContext) registry.getCurrentObservation().getContext();
            final String b3 = context.getGetter().get(context.getCarrier(), "b3");
            final TraceContextOrSamplingFlags traceContext = B3SingleFormat.parseB3SingleFormat(b3);
            consumedTraceIds.put(record.task().getId(), traceContext.context().traceIdString());

            consumedContexts.add(context);
        }

        @Override
        public void doAssert() {
            assertEquals(producedTraceIds, consumedTraceIds);

            // Verify that the ObservationContext is injected into the processor thread as intended.
            consumedContexts.stream().forEach(context -> {
                ObservationContextAssert
                        .assertThat(context)
                        .hasNameEqualTo("decaton.processor")
                        .hasContextualNameEqualTo(
                                String.format("decaton processor (%s/%s)", context.getSubscriptionId(),
                                              context.getProcessorName()))
                        .hasLowCardinalityKeyValue("subscriptionId", context.getSubscriptionId())
                        .hasLowCardinalityKeyValue("processor", context.getProcessorName())
                        .hasLowCardinalityKeyValue("topic", context.getCarrier().topic())
                        .hasLowCardinalityKeyValue("partition",
                                                   String.valueOf(context.getCarrier().partition()))
                        .hasHighCardinalityKeyValue("offset",
                                                    String.valueOf(context.getCarrier().offset()))
                ;
            });
        }
    }
}
