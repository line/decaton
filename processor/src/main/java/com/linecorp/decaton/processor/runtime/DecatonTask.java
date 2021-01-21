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

package com.linecorp.decaton.processor.runtime;

import com.linecorp.decaton.processor.TaskMetadata;

import lombok.Value;
import lombok.experimental.Accessors;

/**
 * Represents a task to be processed by Decaton
 * @param <T> type of task
 */
@Value
@Accessors(fluent = true)
public class DecatonTask<T> {
    /**
     * The metadata of a task, which should contain all information
     * which is not representing task data but can be necessary on processor side.
     *
     * <p>This field must be supplied regardless of using delayed processing feature or not.</p>
     * <p>For example, you can construct metadata as below:</p>
     * <pre>
     * {@code
     * TaskMetadata.builder()
     *             // These field just be used for tracing purpose. you can omit setting these fields if not necessary.
     *             .sourceApplicationId(applicationId)
     *             .sourceInstanceId(instanceId)
     *             .timestampMillis(eventTimestampMillis)
     *
     *             // If you set this field, the processor will wait until the scheduled time comes.
     *             // You can omit setting this field if you do not need delayed processing feature.
     *             .scheduledTimeMillis(eventTimestampMillis + 1000L)
     *
     *             .build();
     * }
     * </pre>
     */
    TaskMetadata metadata;

    /**
     * A task data to be processed.
     *
     * <p>This value will be passed to decaton processor.</p>
     */
    T taskData;

    /**
     * Holds serialized task bytes.
     * <p>
     * This field must be exactly same as the bytes passed to {@link TaskExtractor#extract}.
     * </p>
     */
    byte[] taskDataBytes;
}
