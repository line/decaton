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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;

import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.QuotaUsage;
import com.linecorp.decaton.processor.tracing.TracingProvider.RecordTraceHandle;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;

@ToString
@Getter
@Accessors(fluent = true)
@AllArgsConstructor
public class TaskRequest {
    private final TopicPartition topicPartition;
    private final long recordOffset;
    private final OffsetState offsetState;
    @ToString.Exclude
    private final byte[] key;
    @ToString.Exclude
    private final Headers headers;
    @ToString.Exclude
    private final RecordTraceHandle trace;
    @ToString.Exclude
    private byte[] rawRequestBytes;
    @ToString.Exclude
    private final QuotaUsage quotaUsage;

    public String id() {
        // TaskRequest object is held alive through associated ProcessingContext's lifetime, hence holding
        // any value as its field makes memory occupation worse. Since this ID field is rarely used (typically
        // when trace level logging is enabled), it is better to take short lived object allocation and cpu cost
        // rather than building it once and cache as an object field.
        return "topic=" + topicPartition.topic() + " partition=" + topicPartition.partition() +
               " offset=" + recordOffset;
    }

    /**
     * This class will live until the task process has been completed.
     * To lessen heap pressure, rawRequestBytes should be purged by calling this once the task is extracted.
     */
    public void purgeRawRequestBytes() {
        rawRequestBytes = null;
    }
}
