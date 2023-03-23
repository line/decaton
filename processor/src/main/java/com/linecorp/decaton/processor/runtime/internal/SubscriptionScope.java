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

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.SubPartitionerSupplier;
import com.linecorp.decaton.processor.tracing.TracingProvider;
import com.linecorp.decaton.processor.runtime.RetryConfig;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;

@AllArgsConstructor
@EqualsAndHashCode
@Accessors(fluent = true)
@Getter
public class SubscriptionScope {
    private final String subscriptionId;
    private final String topic;
    private final Optional<RetryConfig> retryConfig;
    private final Optional<PerKeyQuotaConfig> perKeyQuotaConfig;
    private final ProcessorProperties props;
    private final TracingProvider tracingProvider;
    private final int maxPollRecords;
    private final SubPartitionerSupplier subPartitionerSupplier;

    public Optional<String> retryTopic() {
        return retryConfig.map(conf -> conf.retryTopicOrDefault(topic));
    }

    public Set<String> shapingTopics() {
        return perKeyQuotaConfig.map(conf -> conf.shapingTopicsSupplier().apply(topic))
                                .orElse(Collections.emptySet());
    }

    @Override
    public String toString() {
        return subscriptionId;
    }
}
