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

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.testing.processor.ProcessedRecord;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee;
import com.linecorp.decaton.testing.processor.ProducedRecord;

import io.micrometer.tracing.Tracer;

public class TracePropagationGuarantee implements ProcessingGuarantee {
    private final Map<String, String> producedTraceIds = new ConcurrentHashMap<>();
    private final Map<String, String> consumedTraceIds = new ConcurrentHashMap<>();
    private final Tracer tracer;

    public TracePropagationGuarantee(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public void onProduce(ProducedRecord record) {
        producedTraceIds.put(record.task().getId(), tracer.currentTraceContext().context().traceId());
    }

    @Override
    public void onProcess(TaskMetadata metadata, ProcessedRecord record) {
        consumedTraceIds.put(record.task().getId(), tracer.currentTraceContext().context().traceId());
    }

    @Override
    public void doAssert() {
        assertEquals(producedTraceIds, consumedTraceIds);
    }
}
