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

package com.linecorp.decaton.processor;

import com.linecorp.decaton.protocol.Decaton.TaskMetadataProto;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

/**
 * The metadata of a task, which should contain all information which is not representing task data
 * but can be necessary on processor
 */
@Value
@Builder
@Accessors(fluent = true)
public class TaskMetadata {
    /**
     * See {@link TaskMetadataProto#getTimestampMillis()}
     */
    long timestampMillis;

    /**
     * See {@link TaskMetadataProto#getSourceApplicationId()}
     */
    @NonNull
    @Builder.Default
    String sourceApplicationId = "";

    /**
     * See {@link TaskMetadataProto#getSourceInstanceId()}
     */
    @NonNull
    @Builder.Default
    String sourceInstanceId = "";

    /**
     * See {@link TaskMetadataProto#getRetryCount()}
     */
    long retryCount;

    /**
     * See {@link TaskMetadataProto#getScheduledTimeMillis()}
     */
    long scheduledTimeMillis;

    /**
     * Convert the metadata to raw protobuf format
     * @return protobuf representation of the metadata
     */
    public TaskMetadataProto toProto() {
        return TaskMetadataProto.newBuilder()
                                .setTimestampMillis(timestampMillis)
                                .setSourceApplicationId(sourceApplicationId)
                                .setSourceInstanceId(sourceInstanceId)
                                .setRetryCount(retryCount)
                                .setScheduledTimeMillis(scheduledTimeMillis)
                                .build();
    }

    /**
     * Instantiate a task metadata from raw protobuf format
     * @param taskMetadataProto raw task metadata
     * @return Plain java representation of the metadata
     */
    public static TaskMetadata fromProto(TaskMetadataProto taskMetadataProto) {
        return new TaskMetadata(taskMetadataProto.getTimestampMillis(),
                                taskMetadataProto.getSourceApplicationId(),
                                taskMetadataProto.getSourceInstanceId(),
                                taskMetadataProto.getRetryCount(),
                                taskMetadataProto.getScheduledTimeMillis());
    }
}
