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

import com.google.protobuf.InvalidProtocolBufferException;

import com.linecorp.decaton.common.Deserializer;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DefaultTaskExtractor<T> implements TaskExtractor<T> {
    private final Deserializer<T> taskDeserializer;

    @Override
    public DecatonTask<T> extract(byte[] bytes) {
        try {
            DecatonTaskRequest taskRequest = DecatonTaskRequest.parseFrom(bytes);
            TaskMetadata metadata = TaskMetadata.fromProto(taskRequest.getMetadata());
            byte[] taskDataBytes = taskRequest.getSerializedTask().toByteArray();

            return new DecatonTask<>(
                    metadata,
                    taskDeserializer.deserialize(taskDataBytes),
                    taskDataBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
