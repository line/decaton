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

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.linecorp.decaton.common.Serializer;

import lombok.Builder;
import lombok.Value;

/**
 * Decaton client interface to use for putting tasks on decaton queue.
 *
 * @param <T> a type of task which is defined as Protocol Buffers message
 */
public interface DecatonClient<T> extends AutoCloseable {
    /**
     * Put a task onto associated decaton queue.
     *
     * @param key the criteria to shuffle and order tasks. null can be specified if it doesn't matters.
     * @param task an instance of task. Should never be null.
     *
     * @return a {@link CompletableFuture} which represents the result of task put.
     */
    CompletableFuture<PutTaskResult> put(String key, T task);

    /**
     * Put a task onto associated decaton queue with specifying arbitrary timestamp.
     * @param key the criteria to shuffle and order tasks. null can be specified if it doesn't matters.
     * @param task an instance of task. Should never be null.
     * @param timestamp milliseconds precision timestamp which is to be used to set timestamp of task metadata.
     * @return a {@link CompletableFuture} which represents the result of task put.
     */
    CompletableFuture<PutTaskResult> put(String key, T task, long timestamp);

    /**
     * Put a task onto associated decaton queue with specifying some fields of task metadata.
     * @param key the criteria to shuffle and order tasks. null can be specified if it doesn't matters.
     * @param task an instance of task. Should never be null.
     * @param overrideTaskMetadata taskMetaData which can be set by users and used for event publish.
     * @return a {@link CompletableFuture} which represents the result of task put.
     */
    CompletableFuture<PutTaskResult> put(String key, T task, TaskMetadata overrideTaskMetadata);

    /**
     * Put a task onto a specified kafka partition with specifying some fields of task metadata.
     * @param key the criteria to shuffle and order tasks. null can be specified if it doesn't matter.
     * @param task an instance of task. Should never be null.
     * @param overrideTaskMetadata taskMetaData which can be set by users and used for event publish.
     * null can be specified if it doesn't matter.
     * @param partition the id of the partition. null can be specified if it doesn't matter.
     *
     * @return a {@link CompletableFuture} which represents the result of task put.
     */
    CompletableFuture<PutTaskResult> put(String key, T task, TaskMetadata overrideTaskMetadata,
                                         Integer partition);

    /**
     * Put a task onto associated decaton queue.
     * This is just a helper method for typical use cases - put task asynchronously and observe result just for telling
     * whether the message production succeeded:
     * {@code
     * decaton.put(key, task)
     *        .exceptionally(e -> {
     *            logger.error("failed to put task", e);
     *            return null;
     *        });
     * }
     *
     * @param key the criteria to shuffle and order tasks. null can be specified if it doesn't matters.
     * @param task an instance of task. Should never be null.
     * @param errorCallback an implemented {@link Consumer} which is called with exception only if this client
     * failed to put task to the associated topic.
     *
     * @return a {@link CompletableFuture} which represents the result of task put.
     */
    CompletableFuture<PutTaskResult> put(String key, T task, Consumer<Throwable> errorCallback);

    /**
     * Put a task onto associated decaton queue with specifying arbitrary timestamp.
     * This is just a helper method for typical use cases - put task asynchronously and observe result just for telling
     * whether the message production succeeded:
     *
     * {@code
     * decaton.put(key, task)
     *        .exceptionally(e -> {
     *            logger.error("failed to put task", e);
     *            return null;
     *        });
     * }
     *
     * @param key the criteria to shuffle and order tasks. null can be specified if it doesn't matters.
     * @param task an instance of task. Should never be null.
     * @param timestamp milliseconds precision timestamp which is to be used to set timestamp of task metadata.
     * @param errorCallback an implemented {@link Consumer} which is called with exception only if this client
     * failed to put task to the associated topic.
     *
     * @return a {@link CompletableFuture} which represents the result of task put.
     */
    default CompletableFuture<PutTaskResult> put(String key, T task, long timestamp,
                                                 Consumer<Throwable> errorCallback) {
        CompletableFuture<PutTaskResult> result = put(key, task, timestamp);
        result.exceptionally(e -> {
            try {
                errorCallback.accept(e);
            } catch (RuntimeException ignored) {
                // noop
            }
            return null;
        });
        return result;
    }

    /**
     * Creates new {@link DecatonClientBuilder} for the task type given as an argument.
     * @param topic the name of destination topic.
     * @param serializer an instance of {@link Serializer} used to serialize task into bytes.
     * @param <T> the type of task to be submitted by resulting client.
     * @return an instance of {@link DecatonClientBuilder}.
     */
    static <T> DecatonClientBuilder<T> producing(String topic, Serializer<T> serializer) {
        return new DecatonClientBuilder<>(topic, serializer);
    }

    @Builder
    @Value
    class TaskMetadata {
        /**
         * timestamp of task metadata
         */
        Long timestamp;
        /**
         * scheduledTime for event processing, it should be (timestamp + delayDuration) in milliseconds
         */
        Long scheduledTime;
    }
}
