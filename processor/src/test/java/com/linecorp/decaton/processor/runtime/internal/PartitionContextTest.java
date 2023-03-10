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
import static org.junit.Assert.assertFalse;

import java.util.Optional;
import java.util.OptionalLong;

import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;

public class PartitionContextTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    private final PartitionScope scope = new PartitionScope(
            new SubscriptionScope("subscription", "topic",
                                  Optional.empty(), ProcessorProperties.builder().build(),
                                  NoopTracingProvider.INSTANCE,
                                  ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                                  DefaultSubPartitioner::new),
            new TopicPartition("topic", 0));

    @Mock
    private Processors<?> processors;

    private static final int MAX_PENDING_RECORDS = 100;

    private PartitionContext context;

    @Before
    public void setUp() {
        context = new PartitionContext(scope, processors, MAX_PENDING_RECORDS);
    }

    @Test
    public void testOffsetWaitingCommit() {
        assertFalse(context.offsetWaitingCommit().isPresent());

        OffsetState state = context.registerOffset(100);
        assertFalse(context.offsetWaitingCommit().isPresent());

        state.completion().complete();
        context.updateHighWatermark();
        assertEquals(OptionalLong.of(100), context.offsetWaitingCommit());

        context.updateCommittedOffset(100);
        assertFalse(context.offsetWaitingCommit().isPresent());
    }
}
