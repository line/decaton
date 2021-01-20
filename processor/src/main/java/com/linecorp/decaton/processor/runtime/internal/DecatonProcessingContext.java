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

import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;

import lombok.AllArgsConstructor;

public final class DecatonProcessingContext {
    @AllArgsConstructor
    private static class ProcessingTarget {
        private final String subscriptionId;
        private final TopicPartition topicPartition;
        private final Integer subPartitionId;
    }

    private static final ThreadLocal<ProcessingTarget> processingTarget = new ThreadLocal<>();

    private DecatonProcessingContext() {}

    private static ProcessingTarget getOrThrow() {
        ProcessingTarget target = processingTarget.get();
        if (target == null) {
            throw new IllegalStateException("this thread isn't configured to have Decaton processing target");
        }
        return target;
    }

    public static String processingSubscription() {
        return getOrThrow().subscriptionId;
    }

    public static TopicPartition processingTopicPartition() {
        return getOrThrow().topicPartition;
    }

    public static Integer processingThreadId() {
        return getOrThrow().subPartitionId;
    }

    public static <T> T withContext(String subscriptionId, TopicPartition tp, Integer subPartitionId,
                                    Supplier<T> action) {
        processingTarget.set(new ProcessingTarget(subscriptionId, tp, subPartitionId));
        try {
            return action.get();
        } finally {
            processingTarget.remove();
        }
    }
}
