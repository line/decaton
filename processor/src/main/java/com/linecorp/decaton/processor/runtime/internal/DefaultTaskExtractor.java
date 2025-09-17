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

import org.apache.kafka.common.serialization.Deserializer;

import com.google.protobuf.InvalidProtocolBufferException;

import com.linecorp.decaton.client.internal.TaskMetadataUtil;
import com.linecorp.decaton.processor.runtime.ConsumedRecord;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.protocol.internal.DecatonInternal.DecatonTaskRequest;
import com.linecorp.decaton.protocol.Decaton.TaskMetadataProto;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DefaultTaskExtractor<T> implements TaskExtractor<T> {
    private final Deserializer<T> taskDeserializer;
    private final Property<Boolean> legacyFallbackEnabledProperty;

    @Override
    public DecatonTask<T> extract(ConsumedRecord record) {
        TaskMetadataProto headerMeta = TaskMetadataUtil.readFromHeader(record.headers());
        if (headerMeta != null) {
            byte[] taskDataBytes = record.value();
            return new DecatonTask<>(
                    TaskMetadata.fromProto(headerMeta),
                    taskDeserializer.deserialize(record.topic(), record.headers(), taskDataBytes),
                    taskDataBytes);
        } else {
            // There are two cases where task metadata header is missing:
            // 1. The task is produced by an old producer which wraps tasks in DecatonTaskRequest proto.
            // 2. The task is produced by non-DecatonClient producer.
            //
            // From Decaton perspective, there is no way to distinguish between these two cases,
            // so we need to rely on a configuration to determine how to deserialize the task.
            if (legacyFallbackEnabledProperty.value()) {
                try {
                    DecatonTaskRequest taskRequest = DecatonTaskRequest.parseFrom(record.value());
                    TaskMetadata metadata = TaskMetadata.fromProto(taskRequest.getMetadata());
                    byte[] taskDataBytes = taskRequest.getSerializedTask().toByteArray();

                    return new DecatonTask<>(
                            metadata,
                            taskDeserializer.deserialize(record.topic(), record.headers(), taskDataBytes),
                            taskDataBytes);
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalArgumentException(e);
                }
            } else {
                T task = taskDeserializer.deserialize(record.topic(), record.headers(), record.value());
                return new DecatonTask<>(
                        TaskMetadata.builder()
                                    .timestampMillis(record.recordTimestampMillis())
                                    .build(),
                        task,
                        record.value());
            }
        }
    }

    @Override
    public void close() {
        taskDeserializer.close();
    }
}
