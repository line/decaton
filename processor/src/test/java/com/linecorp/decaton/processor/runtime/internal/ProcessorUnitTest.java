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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.SubPartitionRuntime;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;
import com.linecorp.decaton.protocol.Sample.HelloTask;

@ExtendWith(MockitoExtension.class)
public class ProcessorUnitTest {
    private static final TopicPartition topicPartition = new TopicPartition("topic", 1);

    @Mock
    private ProcessPipeline<?> pipeline;

    private TaskRequest taskRequest;

    private ProcessorUnit unit;

    @BeforeEach
    public void setUp() {
        ThreadScope scope = new ThreadScope(
                new PartitionScope(
                        new SubscriptionScope("subscription", "topic",
                                              SubPartitionRuntime.THREAD_POOL,
                                              Optional.empty(), Optional.empty(), ProcessorProperties.builder().build(),
                                              NoopTracingProvider.INSTANCE,
                                              ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                                              DefaultSubPartitioner::new),
                        new TopicPartition("topic", 0)),
                0);

        unit = spy(new ProcessorUnit(scope, pipeline, Executors.newSingleThreadExecutor()));

        taskRequest = new TaskRequest(1723687072569L, topicPartition, 1, new OffsetState(1234), null, null, null, HelloTask.getDefaultInstance().toByteArray(), null);
    }

    @Test
    @Timeout(1)
    public void testProcessNormally() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return CompletableFuture.completedFuture(null);
        }).when(pipeline).scheduleThenProcess(taskRequest);

        unit.putTask(taskRequest);
        latch.await();
        unit.close();

        verify(pipeline, times(1)).scheduleThenProcess(taskRequest);
    }

    @Test
    @Timeout(1)
    public void testProcess_PIPELINE_THREW() throws Exception {
        CountDownLatch processLatch = new CountDownLatch(2);
        doAnswer(invocation -> {
            processLatch.countDown();
            throw new RuntimeException("something happened");
        }).when(pipeline).scheduleThenProcess(any());

        unit.putTask(taskRequest);
        unit.putTask(taskRequest);
        processLatch.await();
        unit.close();

        // Even if the first process throw it should keep processing it
        verify(pipeline, times(2)).scheduleThenProcess(taskRequest);
    }
}
