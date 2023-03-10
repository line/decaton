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

import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;

public class PartitionProcessorTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    private static final PartitionScope scope = new PartitionScope(
            new SubscriptionScope("subscription", "topic",
                                  Optional.empty(),
                                  ProcessorProperties.builder().set(Property.ofStatic(
                                          ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 4)).build(),
                                  NoopTracingProvider.INSTANCE,
                                  ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                                  DefaultSubPartitioner::new),
            new TopicPartition("topic", 0));

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
            new PartitionProcessor(scope, processors) {
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
}
