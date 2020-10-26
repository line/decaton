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

import com.linecorp.decaton.processor.TracingProvider.ProcessorTraceHandle;

import brave.Span;
import brave.messaging.MessagingTracing;
import brave.propagation.CurrentTraceContext.Scope;

final class BraveProcessorTraceHandle extends BraveTraceHandle implements ProcessorTraceHandle {
    private Scope scope;

    BraveProcessorTraceHandle(MessagingTracing messagingTracing, Span span) {
        super(messagingTracing, span);
    }

    @Override
    public void processingStart() {
        scope = messagingTracing.tracing().currentTraceContext().newScope(span.context());
    }

    @Override
    public void processingReturn() {
        span.annotate("return");
        scope.close();
    }
}

