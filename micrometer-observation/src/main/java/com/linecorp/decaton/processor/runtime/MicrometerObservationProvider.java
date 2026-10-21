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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.linecorp.decaton.processor.tracing.TracingProvider;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

public class MicrometerObservationProvider implements TracingProvider {
    private final ObservationRegistry registry;
    private final DecatonProcessorObservationConvention customConvention;
    private final DecatonProcessorObservationConvention defaultConvention =
            new DefaultDecatonProcessorObservationConvention();

    public MicrometerObservationProvider(ObservationRegistry registry) {
        this(registry, null);
    }

    public MicrometerObservationProvider(ObservationRegistry registry,
                                         DecatonProcessorObservationConvention customConvention) {
        this.registry = registry;
        this.customConvention = customConvention;
    }

    @Override
    public RecordTraceHandle traceFor(ConsumerRecord<?, ?> record, String subscriptionId) {
        final Observation observation = DecatonProcessorObservationDocumentation.INSTANCE.observation(
                customConvention,
                defaultConvention,
                () -> new DecatonProcessorReceiverContext(record, subscriptionId),
                registry
        );
        return new MicrometerObservationRecordTraceHandle(observation);
    }
}
