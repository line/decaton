/*
 * Copyright 2023 LINE Corporation
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

import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback;
import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.QuotaUsage;

/**
 * An interface which is responsible for applying quota to a {@link TaskRequest}.
 * When per-key-quota feature is not enabled, you should use {@link NoopQuotaApplier}
 */
public interface QuotaApplier extends AutoCloseable {
    /**
     * Maybe applying a quota to the given {@link ConsumerRecord}.
     * If quota is applied, this method will handle the task accordingly based on
     * the {@link QuotaCallback} result and return true.
     * Otherwise, this method will return false.
     * If true is returned, the caller should not enqueue the task to the processor.
     * @param record the task record to apply quota.
     * @param offsetState the offset state of the task.
     * @param quotaUsage the observed quota usage of the key
     * @return true if quota is applied, false otherwise.
     */
    boolean apply(ConsumerRecord<byte[], byte[]> record,
                  OffsetState offsetState,
                  QuotaUsage quotaUsage);

    @Override
    default void close() {
        // do nothing
    }
}
