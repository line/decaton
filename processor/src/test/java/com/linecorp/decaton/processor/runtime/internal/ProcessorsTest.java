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

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.DecatonProcessorSupplier;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;
import com.linecorp.decaton.protocol.Sample.HelloTask;

public class ProcessorsTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    private static final String SUBSC_ID = "subscription";

    private static final TopicPartition topicPartition = new TopicPartition("topic", 1);

    private static final ThreadScope scope = new ThreadScope(
            new PartitionScope(
                    new SubscriptionScope(SUBSC_ID, topicPartition.topic(),
                                          Optional.empty(), Optional.empty(), ProcessorProperties.builder().build(),
                                          NoopTracingProvider.INSTANCE,
                                          ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                                          DefaultSubPartitioner::new),
                    topicPartition),
            0);

    @Test
    public void testCleanupPartiallyInitializedProcessors() {
        @SuppressWarnings("unchecked")
        List<DecatonProcessorSupplier<HelloTask>> suppliers = Arrays.asList(
                mock(DecatonProcessorSupplier.class),
                mock(DecatonProcessorSupplier.class),
                mock(DecatonProcessorSupplier.class), // this fails
                mock(DecatonProcessorSupplier.class));

        Processors<HelloTask> processors = new Processors<>(
                suppliers, null,
                new DefaultTaskExtractor<>(bytes -> HelloTask.getDefaultInstance()),
                null);

        doThrow(new RuntimeException("exception")).when(suppliers.get(2)).getProcessor(any(), any(), anyInt());

        try {
            processors.newPipeline(scope, null, null);
            fail("Successful call w/o exception");
        } catch (RuntimeException ignored) {
        }

        verify(suppliers.get(0), times(1)).getProcessor(SUBSC_ID, topicPartition, 0);
        verify(suppliers.get(1), times(1)).getProcessor(SUBSC_ID, topicPartition, 0);
        verify(suppliers.get(2), times(1)).getProcessor(SUBSC_ID, topicPartition, 0);
        verify(suppliers.get(3), never()).getProcessor(SUBSC_ID, topicPartition, 0);

        verify(suppliers.get(0), times(1)).leaveThreadScope(SUBSC_ID, topicPartition, 0);
        verify(suppliers.get(1), times(1)).leaveThreadScope(SUBSC_ID, topicPartition, 0);
        verify(suppliers.get(2), times(1)).leaveThreadScope(SUBSC_ID, topicPartition, 0);
        verify(suppliers.get(3), times(1)).leaveThreadScope(SUBSC_ID, topicPartition, 0);
    }
}
