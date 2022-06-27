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

package com.linecorp.decaton.testing.processor;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;

import com.linecorp.decaton.processor.tracing.TestTracingProvider;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;

public class TestTracingProducer extends ProducerAdaptor<byte[], DecatonTaskRequest> {
    public TestTracingProducer(Producer<byte[], DecatonTaskRequest> delegate) {
        super(delegate);
    }

    private static void propagateCurrentTrace(ProducerRecord<byte[], DecatonTaskRequest> record) {
        String traceId = TestTracingProvider.getCurrentTraceId();
        if (null == traceId) {
            traceId = "trace-" + UUID.randomUUID();
        }
        final RecordHeader tracingHeader = new RecordHeader(TestTracingProvider.TRACE_HEADER,
                                                            traceId.getBytes(StandardCharsets.UTF_8));
        record.headers().add(tracingHeader);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<byte[], DecatonTaskRequest> record) {
        propagateCurrentTrace(record);
        return super.send(record);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<byte[], DecatonTaskRequest> record, Callback callback) {
        propagateCurrentTrace(record);
        return super.send(record, callback);
    }
}
