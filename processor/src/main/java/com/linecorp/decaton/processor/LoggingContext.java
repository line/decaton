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

import org.slf4j.MDC;

import com.linecorp.decaton.processor.internal.ByteArrays;
import com.linecorp.decaton.processor.runtime.internal.TaskRequest;

/**
 * The {@link LoggingContext} class allows users to write logs lines
 * without explicitly needing to access the details within a {@link ProcessingContext}
 *
 * How to use:
 *
 *   try (LoggingContext loggingContext = processingContext.loggingContext()) {
 *       // your implementation
 *   }
 */
public class LoggingContext implements AutoCloseable {
    public static final String OFFSET_KEY = "dt_offset";
    public static final String TOPIC_KEY = "dt_topic";
    public static final String PARTITION_KEY = "dt_partition";
    public static final String METADATA_KEY = "dt_metadata";
    public static final String SUBSCRIPTION_ID_KEY = "dt_subscription_id";
    public static final String TASK_KEY = "dt_task_key";

    private final boolean enabled;

    public LoggingContext(boolean enabled, String subscriptionId, TaskRequest request, TaskMetadata metadata) {
        this.enabled = enabled;
        if (enabled) {
            MDC.put(METADATA_KEY, metadata.toString());
            MDC.put(TASK_KEY, ByteArrays.toString(request.key()));
            MDC.put(SUBSCRIPTION_ID_KEY, subscriptionId);
            MDC.put(OFFSET_KEY, String.valueOf(request.recordOffset()));
            MDC.put(TOPIC_KEY, request.topicPartition().topic());
            MDC.put(PARTITION_KEY, String.valueOf(request.topicPartition().partition()));
        }
    }

    public LoggingContext(String subscriptionId, TaskRequest request, TaskMetadata metadata) {
        this(true, subscriptionId, request, metadata);
    }

    @Override
    public void close() {
        if (enabled) {
            MDC.remove(OFFSET_KEY);
            MDC.remove(TOPIC_KEY);
            MDC.remove(TASK_KEY);
            MDC.remove(PARTITION_KEY);
            MDC.remove(METADATA_KEY);
            MDC.remove(SUBSCRIPTION_ID_KEY);
        }
    }
}
