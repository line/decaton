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


import org.apache.kafka.clients.consumer.ConsumerRecord;

import brave.Span;
import brave.kafka.clients.KafkaTracing;
import brave.propagation.CurrentTraceContext.Scope;

public class BraveTracingProvider implements TracingProvider {
    private final KafkaTracing kafkaTracing;

    public BraveTracingProvider(KafkaTracing kafkaTracing) {
        this.kafkaTracing = kafkaTracing;
    }

    @Override
    public TraceHandle traceFor(ConsumerRecord<?, ?> record, String subscriptionId) {
        final Span span = kafkaTracing.nextSpan(record).name("decaton").tag("subscriptionId", subscriptionId)
                                      .start();
        return new TraceHandle() {
            private Scope scope;

            @Override
            public void processingStart() {
                scope = kafkaTracing.messagingTracing().tracing().currentTraceContext().newScope(
                        span.context());
                span.annotate("decaton.pipeline.start");
            }

            @Override
            public void processingReturn() {
                span.annotate("decaton.pipeline.return");
                scope.close();
            }

            @Override
            public void processingCompletion() {
                span.finish();
            }
        };
    }
}
