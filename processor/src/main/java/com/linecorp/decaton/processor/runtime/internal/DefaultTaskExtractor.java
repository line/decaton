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

import com.google.protobuf.InvalidProtocolBufferException;

import com.linecorp.decaton.common.Deserializer;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;
import com.linecorp.decaton.protocol.Decaton.TaskMetadataProto;
import com.linecorp.decaton.protocol.TaskMetadataUtil;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DefaultTaskExtractor<T> implements TaskExtractor<T> {
    private final Deserializer<T> taskDeserializer;

    @Override
    public DecatonTask<T> extract(ConsumerRecord<String, byte[]> record) {
        try {
            final byte[] taskBytes;
            final TaskMetadataProto metadataProto;

            TaskMetadataProto headerMetadata = TaskMetadataUtil.readFromHeader(record.headers());
            if (headerMetadata != null) {
                // metadata is written in headers. i.e. the task is produced by decaton client >= 2.x
                metadataProto = headerMetadata;
                taskBytes = record.value();
            } else {
                // metadata is absent in headers. i.e. the task is produced by decaton client < 2.x
                DecatonTaskRequest taskRequest = DecatonTaskRequest.parseFrom(record.value());
                metadataProto = taskRequest.getMetadata();
                taskBytes = taskRequest.getSerializedTask().toByteArray();
            }

            return new DecatonTask<>(
                    TaskMetadata.fromProto(metadataProto),
                    taskDeserializer.deserialize(taskBytes),
                    taskBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
