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

package com.linecorp.decaton.processor.tracing.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.tracing.TracingProvider;

public enum NoopTracingProvider implements TracingProvider {
    INSTANCE;
    public enum NoopTrace implements RecordTraceHandle, ProcessorTraceHandle {
        INSTANCE;

        @Override
        public void processingStart() {}

        @Override
        public void processingReturn() {}

        @Override
        public void processingCompletion() {}

        @Override
        public ProcessorTraceHandle childFor(DecatonProcessor<?> processor) {
            return this;
        }
    }

    @Override
    public RecordTraceHandle traceFor(ConsumerRecord<?, ?> record, String subscriptionId) {
        return NoopTrace.INSTANCE;
    }
}
