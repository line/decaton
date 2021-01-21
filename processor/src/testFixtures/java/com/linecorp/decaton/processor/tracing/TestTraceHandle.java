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

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.tracing.TracingProvider.ProcessorTraceHandle;
import com.linecorp.decaton.processor.tracing.TracingProvider.RecordTraceHandle;

public class TestTraceHandle implements RecordTraceHandle, ProcessorTraceHandle {
    private final String traceId;
    private String previousTraceId;

    public TestTraceHandle(String traceId) {
        this.traceId = traceId;
        TestTracingProvider.openTraces.add(traceId);
    }

    @Override
    public void processingStart() {
        previousTraceId = TestTracingProvider.traceId.get();
        TestTracingProvider.traceId.set(traceId);
    }

    @Override
    public void processingReturn() {
        TestTracingProvider.traceId.set(previousTraceId);
    }

    @Override
    public void processingCompletion() {
        TestTracingProvider.openTraces.remove(traceId);
    }

    @Override
    public ProcessorTraceHandle childFor(DecatonProcessor<?> processor) {
        final String childTraceId = traceId + "-" + processor.name();
        return new TestTraceHandle(childTraceId);
    }
}
