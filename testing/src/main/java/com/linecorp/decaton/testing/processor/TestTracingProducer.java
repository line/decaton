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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.header.internals.RecordHeader;

import com.linecorp.decaton.processor.TestTracingProvider;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;

public class TestTracingProducer implements Producer<String, DecatonTaskRequest> {
    private final Producer<String, DecatonTaskRequest> inner;

    public TestTracingProducer(Producer<String, DecatonTaskRequest> inner) {this.inner = inner;}

    @Override
    public void initTransactions() {
        inner.initTransactions();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        inner.beginTransaction();
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
            throws ProducerFencedException {
        inner.sendOffsetsToTransaction(offsets, consumerGroupId);
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        inner.commitTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        inner.abortTransaction();
    }

    private static void propagateCurrentTrace(ProducerRecord<String, DecatonTaskRequest> record) {
        final String traceId = TestTracingProvider.getCurrentTraceId();
        if (null != traceId) {
            final RecordHeader tracingHeader = new RecordHeader(TestTracingProvider.TRACE_HEADER,
                                                                traceId.getBytes(StandardCharsets.UTF_8));
            record.headers().add(tracingHeader);
        }
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String, DecatonTaskRequest> record) {
        propagateCurrentTrace(record);
        return inner.send(record);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String, DecatonTaskRequest> record, Callback callback) {
        propagateCurrentTrace(record);
        return inner.send(record, callback);
    }

    @Override
    public void flush() {
        inner.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return inner.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return inner.metrics();
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public void close(Duration timeout) {
        inner.close(timeout);
    }
}
