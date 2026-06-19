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

import com.linecorp.decaton.processor.tracing.TracingProvider;

import io.micrometer.observation.Observation;
import io.micrometer.observation.Observation.Event;

final class MicrometerObservationProcessorTraceHandle implements TracingProvider.ProcessorTraceHandle {
    private final Observation observation;
    private Observation.Scope scope;

    MicrometerObservationProcessorTraceHandle(Observation observation) {
        this.observation = observation;
    }

    @Override
    public void processingStart() {
        observation.event(Event.of("processingStart"));
        scope = observation.openScope();
    }

    @Override
    public void processingReturn() {
        observation.event(Event.of("processingReturn"));
        if (scope != null) {
            scope.close();
        }
    }

    @Override
    public void processingCompletion() {
        observation.event(Event.of("processingCompletion"));
    }
}
