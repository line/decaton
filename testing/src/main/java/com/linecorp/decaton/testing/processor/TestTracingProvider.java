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

package com.linecorp.decaton.testing.processor;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.runtime.NoopTracingProvider.NoopTrace;
import com.linecorp.decaton.processor.runtime.TracingProvider;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestTracingProvider implements TracingProvider {
    public static final String TRACE_HEADER = "test-trace-id";
    private static final ThreadLocal<String> traceId = new ThreadLocal<>();
    private static final Set<String> openTraces = ConcurrentHashMap.newKeySet();

    public static String getCurrentTraceId() {
        return traceId.get();
    }

    public static void assertAllTracesWereClosed() {
        if (!openTraces.isEmpty()) {
            throw new AssertionError("Unclosed traces: " + openTraces);
        }
    }

    @Override
    public TraceHandle traceFor(ConsumerRecord<?, ?> record, String subscriptionId) {
        final Header header = record.headers().lastHeader(TRACE_HEADER);
        if (null != header) {
            final String recordTraceId = new String(header.value(), StandardCharsets.UTF_8);
            traceId.set(recordTraceId);
            openTraces.add(recordTraceId);
            return new TraceHandle() {
                @Override
                public void processingStart() {
                    traceId.set(recordTraceId);
                }

                @Override
                public void processingReturn() {
                    assertEquals(recordTraceId, traceId.get());
                    traceId.remove();
                }

                @Override
                public void processingCompletion() {
                    openTraces.remove(recordTraceId);
                }

                @Override
                public TraceHandle childFor(DecatonProcessor<?> processor) {
                    return NoopTrace.INSTANCE;
                }
            };
        } else {
            return NoopTrace.INSTANCE;
        }
    }
}
