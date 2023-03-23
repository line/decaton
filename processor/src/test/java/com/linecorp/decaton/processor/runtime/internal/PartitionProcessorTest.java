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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.internal.AbstractDecatonProperties.Builder;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;

public class PartitionProcessorTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

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
                                      Optional.empty(), perKeyQuotaConfig,
                                      builder.build(),
                                      NoopTracingProvider.INSTANCE,
                                      ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                                      DefaultSubPartitioner::new),
                new TopicPartition(topic, 0));
    }

    @Mock
    private Processors<?> processors;

    @Test
    public void testCleanupPartiallyInitializedUnits() throws Exception {
        AtomicInteger count = new AtomicInteger();
        doAnswer(invocation -> {
            if (count.incrementAndGet() == 3) {
                throw new RuntimeException("exception");
            }
            return mock(ProcessPipeline.class);
        }).when(processors).newPipeline(any(), any(), any());

        List<ProcessorUnit> units = new ArrayList<>();
        try {
            new PartitionProcessor(scope("topic", Optional.empty(), prop -> {}), processors) {
                @Override
                ProcessorUnit createUnit(int threadId) {
                    ProcessorUnit unit = spy(super.createUnit(threadId));
                    units.add(unit);
                    return unit;
                }
            };
            fail("Successful call w/o exception");
        } catch (RuntimeException ignored) {
        }

        assertEquals(2, units.size());
        verify(units.get(0), times(1)).initiateShutdown();
        verify(units.get(1), times(1)).initiateShutdown();
        verify(units.get(0), times(1)).shutdownFuture();
        verify(units.get(1), times(1)).shutdownFuture();
    }

    @Test
    public void testRateProperty() {
        assertEquals("decaton.processing.rate.per.partition",
                     PartitionProcessor.rateProperty(scope("topic", Optional.of(PerKeyQuotaConfig.shape()), prop -> {}))
                                       .definition().name());
    }

    @Test
    public void testRatePropertyForShapingTopic() {
        assertEquals("decaton.per.key.quota.processing.rate",
                     PartitionProcessor.rateProperty(scope("topic-shaping", Optional.of(PerKeyQuotaConfig.shape()), prop -> {}))
                                       .definition().name());
    }

    @Test
    public void testOverrideRatePropertyForShapingTopic() {
        assertEquals("decaton.shaping.topic.processing.rate.per.partition.topic-shaping",
                     PartitionProcessor.rateProperty(scope("topic-shaping", Optional.of(PerKeyQuotaConfig.shape()), prop -> {
                                           prop.set(Property.ofStatic(PerKeyQuotaConfig.shapingRateProperty("topic-shaping"),
                                                                      42L));
                                       }))
                                       .definition().name());
    }
}
