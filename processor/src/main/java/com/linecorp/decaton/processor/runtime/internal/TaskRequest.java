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

package com.linecorp.decaton.processor.runtime.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;

import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.tracing.TracingProvider.RecordTraceHandle;

import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public class TaskRequest {
    private final ConsumerRecord<String, byte[]> record;
    private final DeferredCompletion completion;
    private final String id;
    private final RecordTraceHandle trace;

    public TaskRequest(ConsumerRecord<String, byte[]> record,
                       DeferredCompletion completion,
                       RecordTraceHandle trace) {
        this.record = record;
        this.completion = completion;
        this.trace = trace;

        StringBuilder idBuilder = new StringBuilder();
        idBuilder.append("topic=").append(record.topic());
        idBuilder.append(" partition=").append(record.partition());
        idBuilder.append(" offset=").append(record.offset());
        id = idBuilder.toString();
    }

    public TopicPartition topicPartition() {
        return new TopicPartition(record.topic(), record.partition());
    }

    public String key() {
        return record.key();
    }

    public long recordOffset() {
        return record.offset();
    }

    public Headers headers() {
        return record.headers();
    }

    @Override
    public String toString() {
        return "TaskRequest(" +
               topicPartition() + ", " +
               recordOffset() + ", " +
               key() + ", " +
               id();
    }
}
