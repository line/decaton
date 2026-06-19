/*
 * Copyright 2026 LY Corporation
 *
 * LY Corporation licenses this file to you under the Apache License,
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
import com.linecorp.decaton.processor.tracing.TracingProvider;
import com.linecorp.decaton.processor.tracing.TracingProvider.ProcessorTraceHandle;

import io.micrometer.observation.Observation;

final class MicrometerObservationRecordTraceHandle implements TracingProvider.RecordTraceHandle {
    private final Observation observation;

    MicrometerObservationRecordTraceHandle(Observation observation) {
        this.observation = observation;
        this.observation.start();
    }

    @Override
    public ProcessorTraceHandle childFor(DecatonProcessor<?> processor) {
        final DecatonProcessorReceiverContext context =
                (DecatonProcessorReceiverContext) observation.getContext();
        context.setProcessorName(processor.name());
        return new MicrometerObservationProcessorTraceHandle(observation);
    }

    @Override
    public void processingCompletion() {
        observation.stop();
    }
}
