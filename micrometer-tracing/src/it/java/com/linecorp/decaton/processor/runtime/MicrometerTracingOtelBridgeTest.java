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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.client.DecatonClientBuilder.DefaultKafkaProducerSupplier;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.TestUtils;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee.GuaranteeType;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.KafkaTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;

public class MicrometerTracingOtelBridgeTest {

    private OpenTelemetry openTelemetry;
    private io.opentelemetry.api.trace.Tracer otelTracer;
    private KafkaTelemetry otelKafkaTelemetry;
    private Tracer tracer;
    private String retryTopic;

    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    @Before
    public void setUp() {
        openTelemetry = OpenTelemetrySdk.builder()
                                        .setPropagators(ContextPropagators.create(
                                                W3CTraceContextPropagator.getInstance()))
                                        .buildAndRegisterGlobal();
        otelTracer = openTelemetry.getTracerProvider().get("io.micrometer.micrometer-tracing");
        otelKafkaTelemetry = KafkaTelemetry.create(openTelemetry);
        tracer = new OtelTracer(otelTracer, new OtelCurrentTraceContext(), event -> {
        });
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
                // micrometer-tracing does not yet have kafka producer support, so use openTelemetry directly
                .producerSupplier(bootstrapServers -> new WithParentSpanProducer<>(
                        otelKafkaTelemetry.wrap(TestUtils.producer(bootstrapServers)), otelTracer))
                .retryConfig(RetryConfig.builder()
                                        .retryTopic(retryTopic)
                                        .backoff(Duration.ofMillis(10))
                                        .producerSupplier(config -> new WithParentSpanProducer<>(
                                                otelKafkaTelemetry.wrap(producerSupplier.getProducer(config)),
                                                otelTracer))
                                        .build())
                .tracingProvider(new MicrometerTracingProvider(
                        tracer, new OtelPropagator(openTelemetry.getPropagators(), otelTracer)))
                // If we retry tasks, there's no guarantee about ordering nor serial processing
                .excludeSemantics(GuaranteeType.PROCESS_ORDERING, GuaranteeType.SERIAL_PROCESSING)
                .customSemantics(new TracePropagationGuarantee(tracer))
                .build()
                .run();
    }

    private static class WithParentSpanProducer<K, V> implements Producer<K, V> {
        private final Producer<K, V> delegate;
        private final io.opentelemetry.api.trace.Tracer otelTracer;

        WithParentSpanProducer(Producer<K, V> delegate, io.opentelemetry.api.trace.Tracer otelTracer) {
            this.delegate = delegate;
            this.otelTracer = otelTracer;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
            return this.send(record, null);
        }

        // Since the context of parent is injected in Callback of `producer.send`, create the span manually.
        // ref: https://github.com/open-telemetry/opentelemetry-java-instrumentation/blob/8a15975dcacda48375cae62e98fe7551fb192d1f/instrumentation/kafka/kafka-clients/kafka-clients-2.6/library/src/main/java/io/opentelemetry/instrumentation/kafkaclients/v2_6/KafkaTelemetry.java#L262-L264
        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
            final Span span = otelTracer.spanBuilder("test span").startSpan();
            try (final Scope scope = span.makeCurrent()) {
                return delegate.send(record, callback);
            } finally {
                span.end();
            }
        }

        @Override
        public void initTransactions() {
            delegate.initTransactions();
        }

        @Override
        public void beginTransaction() throws ProducerFencedException {
            delegate.beginTransaction();
        }

        @Deprecated
        @Override
        public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                             String consumerGroupId) throws ProducerFencedException {
            delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
        }

        @Override
        public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                             ConsumerGroupMetadata groupMetadata)
                throws ProducerFencedException {
            delegate.sendOffsetsToTransaction(offsets, groupMetadata);
        }

        @Override
        public void commitTransaction() throws ProducerFencedException {
            delegate.commitTransaction();
        }

        @Override
        public void abortTransaction() throws ProducerFencedException {
            delegate.abortTransaction();
        }

        @Override
        public void flush() {
            delegate.flush();
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return delegate.partitionsFor(topic);
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return delegate.metrics();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public void close(Duration timeout) {
            delegate.close();
        }
    }
}
