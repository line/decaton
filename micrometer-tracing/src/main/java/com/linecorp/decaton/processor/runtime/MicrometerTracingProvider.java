/*
 * Copyright 2023 LINE Corporation
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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import com.linecorp.decaton.processor.tracing.TracingProvider;

import io.micrometer.common.lang.Nullable;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;

public class MicrometerTracingProvider implements TracingProvider {
    private final Tracer tracer;
    private final Propagator propagator;

    public MicrometerTracingProvider(Tracer tracer, Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    @Override
    public MicrometerRecordTraceHandle traceFor(ConsumerRecord<?, ?> record, String subscriptionId) {
        return new MicrometerRecordTraceHandle(
                tracer,
                propagator.extract(record.headers(), GETTER)
                          .name("decaton").tag("subscriptionId", subscriptionId).start()
        );
    }

    private static final Propagator.Getter<Headers> GETTER = new Propagator.Getter<Headers>() {
        @Override
        public String get(Headers carrier, String key) {
            return lastStringHeader(carrier, key);
        }

        @Nullable
        private String lastStringHeader(Headers headers, String key) {
            final Header header = headers.lastHeader(key);
            if (header == null || header.value() == null) {
                return null;
            }
            return new String(header.value(), UTF_8);
        }
    };
}
