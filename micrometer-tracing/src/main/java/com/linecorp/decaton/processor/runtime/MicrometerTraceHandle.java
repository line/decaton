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

import com.linecorp.decaton.processor.tracing.TracingProvider.TraceHandle;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;

class MicrometerTraceHandle implements TraceHandle {
    protected final Tracer tracer;
    protected final Span span;

    MicrometerTraceHandle(Tracer tracer, Span span) {
        this.tracer = tracer;
        this.span = span;
    }

    @Override
    public void processingCompletion() {
        span.end();
    }
}
