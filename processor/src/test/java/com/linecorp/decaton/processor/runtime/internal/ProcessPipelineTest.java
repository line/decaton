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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.TaskMetrics;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.DynamicProperty;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.SubPartitionRuntime;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider.NoopTrace;
import com.linecorp.decaton.protocol.Sample.HelloTask;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ProcessPipelineTest {
    private static final HelloTask TASK = HelloTask.getDefaultInstance();

    private final DynamicProperty<Long> completionTimeoutMsProp =
            new DynamicProperty<>(ProcessorProperties.CONFIG_DEFERRED_COMPLETE_TIMEOUT_MS);

    private final ThreadScope scope = new ThreadScope(
            new PartitionScope(
                    new SubscriptionScope("subscription", "topic",
                                          SubPartitionRuntime.THREAD_POOL,
                                          Optional.empty(), Optional.empty(),
                                          ProcessorProperties.builder().set(completionTimeoutMsProp).build(),
                                          NoopTracingProvider.INSTANCE,
                                          ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                                          DefaultSubPartitioner::new),
                    new TopicPartition("topic", 0)),
            0);

    private static final TaskMetrics METRICS = Metrics.withTags("subscription", "subscriptionId",
                                                                "topic", "topic",
                                                                "partition", "1",
                                                                "subpartition", "0")
            .new TaskMetrics();

    private static TaskRequest taskRequest() {
        return new TaskRequest(
                new TopicPartition("topic", 1), 1, new OffsetState(1234), "TEST".getBytes(StandardCharsets.UTF_8), null, NoopTrace.INSTANCE, TASK.toByteArray(), null);
    }

    @Mock
    private TaskExtractor<HelloTask> extractorMock;

    @Mock
    private DecatonProcessor<HelloTask> processorMock;

    @Mock
    private ExecutionScheduler schedulerMock;

    private ProcessPipeline<HelloTask> pipeline;

    @Mock
    private Clock clock;

    @BeforeEach
    public void setUp() {
        completionTimeoutMsProp.set(100L);
        doReturn(10L).when(clock).millis();
        pipeline = spy(new ProcessPipeline<>(
                scope, Collections.singletonList(processorMock), null, extractorMock, schedulerMock, METRICS, clock));
    }

    @Test
    public void testScheduleThenProcess_SYNC_COMPLETE() throws InterruptedException {
        when(extractorMock.extract(any()))
                .thenReturn(new DecatonTask<>(TaskMetadata.builder().build(), TASK, TASK.toByteArray()));

        TaskRequest request = taskRequest();
        pipeline.scheduleThenProcess(request);
        verify(schedulerMock, times(1)).schedule(eq(TaskMetadata.builder().build()));
        verify(processorMock, times(1)).process(any(), eq(TASK));
        assertTrue(request.offsetState().completion().isComplete());
        assertEquals(completionTimeoutMsProp.value() + clock.millis(),
                     request.offsetState().timeoutAt());
    }

    @Test
    public void testScheduleThenProcess_ASYNC_COMPLETE() throws InterruptedException {
        when(extractorMock.extract(any()))
                .thenReturn(new DecatonTask<>(TaskMetadata.builder().build(), TASK, TASK.toByteArray()));
        CountDownLatch beforeComplete = new CountDownLatch(1);
        CountDownLatch afterComplete = new CountDownLatch(1);
        doAnswer(invocation -> {
            ProcessingContext<?> context = invocation.getArgument(0);
            DeferredCompletion completion = context.deferCompletion();
            new Thread(() -> {
                try {
                    beforeComplete.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                completion.complete();
                afterComplete.countDown();
            }).start();
            return null;
        }).when(processorMock).process(any(), any());

        TaskRequest request = taskRequest();
        pipeline.scheduleThenProcess(request);
        verify(schedulerMock, times(1)).schedule(eq(TaskMetadata.builder().build()));
        verify(processorMock, times(1)).process(any(), eq(TASK));

        // Should complete only after processor completes it
        assertFalse(request.offsetState().completion().isComplete());
        beforeComplete.countDown();
        afterComplete.await();
        assertTrue(request.offsetState().completion().isComplete());
        assertEquals(completionTimeoutMsProp.value() + clock.millis(),
                     request.offsetState().timeoutAt());
    }

    @Test
    public void testExtract_InvalidTask() throws InterruptedException {
        when(extractorMock.extract(any()))
                .thenReturn(new DecatonTask<>(null, TASK, TASK.toByteArray()));

        TaskRequest request = taskRequest();
        // Checking exception doesn't bubble up
        pipeline.scheduleThenProcess(request);
        verify(schedulerMock, never()).schedule(any());
        verify(processorMock, never()).process(any(), any());
        assertTrue(request.offsetState().completion().isComplete());
    }

    @Test
    public void testExtract_ThrowsNonRuntimeException() throws InterruptedException {
        // thenThrow cannot be used with checked exception
        when(extractorMock.extract(any())).thenAnswer(inv -> {
            throw new Exception();
        });

        TaskRequest request = taskRequest();
        // Checking exception doesn't bubble up
        pipeline.scheduleThenProcess(request);
        verify(schedulerMock, never()).schedule(any());
        verify(processorMock, never()).process(any(), any());
        assertTrue(request.offsetState().completion().isComplete());
    }

    @Test
    public void testScheduleThenProcess_ExtractThrows() throws InterruptedException {
        when(extractorMock.extract(any())).thenThrow(new RuntimeException());

        TaskRequest request = taskRequest();
        // Checking exception doesn't bubble up
        pipeline.scheduleThenProcess(request);
        verify(schedulerMock, never()).schedule(any());
        verify(processorMock, never()).process(any(), any());
        assertTrue(request.offsetState().completion().isComplete());
    }

    @Test
    public void testExtract_PurgeRawRequestBytes() {
        when(extractorMock.extract(any()))
                .thenReturn(new DecatonTask<>(TaskMetadata.builder().build(), TASK, TASK.toByteArray()));

        TaskRequest request = taskRequest();
        pipeline.extract(request);

        assertNull(request.rawRequestBytes());
    }

    @Test
    public void testScheduleThenProcess_SynchronousFailure() throws InterruptedException {
        DecatonTask<HelloTask> task = new DecatonTask<>(TaskMetadata.builder().build(), TASK, TASK.toByteArray());
        when(extractorMock.extract(any())).thenReturn(task);

        doThrow(new RuntimeException()).when(processorMock).process(any(), eq(TASK));

        TaskRequest request = taskRequest();
        // Checking exception doesn't bubble up
        pipeline.scheduleThenProcess(request);
        assertTrue(request.offsetState().completion().isComplete());
    }

    @Test
    @Timeout(5)
    public void testScheduleThenProcess_Terminate() throws InterruptedException {
        DecatonTask<HelloTask> task = new DecatonTask<>(TaskMetadata.builder().build(), TASK, TASK.toByteArray());
        when(extractorMock.extract(any())).thenReturn(task);

        CountDownLatch atSchedule = new CountDownLatch(1);
        CountDownLatch closeLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            atSchedule.countDown();
            closeLatch.await();
            return null;
        }).when(schedulerMock).schedule(any());

        TaskRequest request = taskRequest();
        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.execute(() -> {
            try {
                pipeline.scheduleThenProcess(request);
            } catch (InterruptedException e) {
                fail("Fail by exception: " + e);
            }
        });
        atSchedule.await();
        pipeline.close();
        closeLatch.countDown();

        executor.shutdown();
        // Checking it actually returns
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        verify(pipeline, never()).process(any(), any());
        assertFalse(request.offsetState().completion().isComplete());
    }
}
