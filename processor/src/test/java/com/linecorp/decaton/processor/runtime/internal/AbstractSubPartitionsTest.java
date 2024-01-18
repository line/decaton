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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import java.util.function.Consumer;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.SubPartitionRuntime;
import com.linecorp.decaton.processor.runtime.internal.AbstractDecatonProperties.Builder;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;

@ExtendWith(MockitoExtension.class)
public class AbstractSubPartitionsTest {
    private static PartitionScope scope(
            String topic,
            Optional<PerKeyQuotaConfig> perKeyQuotaConfig,
            Consumer<Builder<ProcessorProperties>> propConfigurer) {
        Builder<ProcessorProperties> builder = ProcessorProperties.builder();
        builder.set(Property.ofStatic(
                ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 4));
        propConfigurer.accept(builder);
        return new PartitionScope(
                new SubscriptionScope("subscription", "topic",
                                      SubPartitionRuntime.THREAD_POOL,
                                      Optional.empty(), perKeyQuotaConfig,
                                      builder.build(),
                                      NoopTracingProvider.INSTANCE,
                                      ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                                      DefaultSubPartitioner::new),
                new TopicPartition(topic, 0));
    }

    @Test
    public void testRateProperty() {
        assertEquals("decaton.processing.rate.per.partition",
                     AbstractSubPartitions.rateProperty(scope("topic", Optional.of(PerKeyQuotaConfig.shape()), prop -> {}))
                                          .definition().name());
    }

    @Test
    public void testRatePropertyForShapingTopic() {
        assertEquals("decaton.per.key.quota.processing.rate",
                     AbstractSubPartitions.rateProperty(scope("topic-shaping", Optional.of(PerKeyQuotaConfig.shape()), prop -> {}))
                                          .definition().name());
    }

    @Test
    public void testOverrideRatePropertyForShapingTopic() {
        assertEquals("decaton.shaping.topic.processing.rate.per.partition.topic-shaping",
                     AbstractSubPartitions.rateProperty(scope("topic-shaping", Optional.of(PerKeyQuotaConfig.shape()), prop -> {
                                           prop.set(Property.ofStatic(PerKeyQuotaConfig.shapingRateProperty("topic-shaping"),
                                                                      42L));
                                       }))
                                          .definition().name());
    }
}
