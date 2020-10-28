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

package com.linecorp.decaton.client;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.protobuf.ByteString;

import com.linecorp.decaton.common.Serializer;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;
import com.linecorp.decaton.protocol.Decaton.TaskMetadataProto;

public class DecatonClientImpl<T> implements DecatonClient<T> {
    private final Serializer<T> serializer;
    private final DecatonTaskProducer producer;
    private final String applicationId;
    private final String instanceId;
    private final Supplier<Long> timestampSupplier;

    DecatonClientImpl(String topic,
                      Serializer<T> serializer,
                      String applicationId,
                      String instanceId,
                      Properties producerConfig,
                      KafkaProducerSupplier producerSupplier,
                      Supplier<Long> timestampSupplier) {
        this.serializer = serializer;
        this.applicationId = applicationId;
        this.instanceId = instanceId;
        producer = new DecatonTaskProducer(topic, producerConfig, producerSupplier);
        this.timestampSupplier = timestampSupplier;
    }

    public DecatonClientImpl(String topic,
                             Serializer<T> serializer,
                             String applicationId,
                             String instanceId,
                             Properties producerConfig,
                             KafkaProducerSupplier producerSupplier) {
        this(topic, serializer, applicationId, instanceId, producerConfig, producerSupplier,
             System::currentTimeMillis);
    }

    @Override
    public CompletableFuture<PutTaskResult> put(String key, T task, long timestamp) {
        byte[] serializedTask = serializer.serialize(task);

        TaskMetadataProto taskMetadata =
                TaskMetadataProto.newBuilder()
                                 .setTimestampMillis(timestamp)
                                 .setSourceApplicationId(applicationId)
                                 .setSourceInstanceId(instanceId)
                                 .build();
        DecatonTaskRequest request =
                DecatonTaskRequest.newBuilder()
                                  .setMetadata(taskMetadata)
                                  .setSerializedTask(ByteString.copyFrom(serializedTask))
                                  .build();

        return producer.sendRequest(key, request);
    }

    @Override
    public CompletableFuture<PutTaskResult> put(String key, T task, TaskMetaData overrideTaskMetaData) {
        byte[] serializedTask = serializer.serialize(task);

        DecatonTaskRequest request =
                DecatonTaskRequest.newBuilder()
                                  .setMetadata(convertToTaskMetadataProto(overrideTaskMetaData))
                                  .setSerializedTask(ByteString.copyFrom(serializedTask))
                                  .build();

        return producer.sendRequest(key, request);
    }

    @Override
    public CompletableFuture<PutTaskResult> put(String key, T task) {
        return put(key, task, timestampSupplier.get());
    }

    @Override
    public CompletableFuture<PutTaskResult> put(String key, T task, Consumer<Throwable> errorCallback) {
        return put(key, task, timestampSupplier.get(), errorCallback);
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }

    private TaskMetadataProto convertToTaskMetadataProto(TaskMetaData overrideTaskMetadata) {
        final TaskMetadataProto.Builder
                taskMetadataProtoBuilder = TaskMetadataProto.newBuilder()
                                                            .setSourceApplicationId(applicationId)
                                                            .setSourceInstanceId(instanceId);
        if (timestampSupplier != null) {
            taskMetadataProtoBuilder.setTimestampMillis(timestampSupplier.get());
        } else {
            taskMetadataProtoBuilder.setTimestampMillis(System.currentTimeMillis());
        }
        if (overrideTaskMetadata == null) {
            return taskMetadataProtoBuilder.build();
        }
        final Long timestamp = overrideTaskMetadata.getTimestamp();
        final Long scheduledTime = overrideTaskMetadata.getScheduledTime();
        if (timestamp != null && timestamp > 0) {
            taskMetadataProtoBuilder.setTimestampMillis(timestamp);
        }
        if (scheduledTime != null && scheduledTime > 0) {
            taskMetadataProtoBuilder.setScheduledTimeMillis(scheduledTime);
        }
        return taskMetadataProtoBuilder.build();
    }

}
