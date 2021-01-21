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

package com.linecorp.decaton.processor.tracing;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider.NoopTrace;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestTracingProvider implements TracingProvider {
    public static final String TRACE_HEADER = "test-trace-id";
    static final ThreadLocal<String> traceId = new ThreadLocal<>();
    static final Set<String> openTraces = ConcurrentHashMap.newKeySet();

    public static String getCurrentTraceId() {
        return traceId.get();
    }

    public static Set<String> getOpenTraces() {
        return Collections.unmodifiableSet(openTraces);
    }

    public static void assertAllTracesWereClosed() {
        if (!openTraces.isEmpty()) {
            throw new AssertionError("Unclosed traces: " + openTraces);
        }
    }

    @Override
    public RecordTraceHandle traceFor(ConsumerRecord<?, ?> record, String subscriptionId) {
        final Header header = record.headers().lastHeader(TRACE_HEADER);
        if (null != header) {
            final String recordTraceId = new String(header.value(), StandardCharsets.UTF_8);
            traceId.set(recordTraceId);
            return new TestTraceHandle(recordTraceId);
        } else {
            return NoopTrace.INSTANCE;
        }
    }

}
