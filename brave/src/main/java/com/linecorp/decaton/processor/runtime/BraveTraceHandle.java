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

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.runtime.TracingProvider.TraceHandle;

import brave.Span;
import brave.messaging.MessagingTracing;
import brave.propagation.CurrentTraceContext.Scope;

class BraveTraceHandle implements TraceHandle {
    private final MessagingTracing messagingTracing;
    private final Span span;
    private Scope scope;

    public BraveTraceHandle(MessagingTracing messagingTracing, Span span) {
        this.messagingTracing = messagingTracing;
        this.span = span;
    }

    @Override
    public void processingStart() {
        scope = messagingTracing.tracing().currentTraceContext().newScope(span.context());
        span.annotate("decaton.start");
    }

    @Override
    public void processingReturn() {
        span.annotate("decaton.return");
        scope.close();
    }

    @Override
    public void processingCompletion() {
        span.finish();
    }

    @Override
    public TraceHandle childFor(DecatonProcessor<?> processor) {
        final Span childSpan = messagingTracing.tracing().tracer().newChild(span.context())
                                               .name(processor.getClass().getSimpleName())
                                               .start();
        return new BraveTraceHandle(messagingTracing, childSpan);
    }
}
