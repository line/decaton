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

package com.linecorp.decaton.processor.runtime;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.DecatonTask;
import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.ProcessorProperties;
import com.linecorp.decaton.processor.TaskExtractor;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.runtime.NoopTracingProvider.NoopTrace;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;
import com.linecorp.decaton.protocol.Decaton.TaskMetadataProto;
import com.linecorp.decaton.protocol.Sample.HelloTask;

public class ProcessPipelineTest {
    private static final HelloTask TASK = HelloTask.getDefaultInstance();

    private static final DecatonTaskRequest REQUEST =
            DecatonTaskRequest.newBuilder()
                              .setMetadata(TaskMetadataProto.getDefaultInstance())
                              .setSerializedTask(TASK.toByteString())
                              .build();

    private static final ThreadScope scope = new ThreadScope(
            new PartitionScope(
                    new SubscriptionScope("subscription", "topic",
                                          Optional.empty(), ProcessorProperties.builder().build(),
                                          NoopTracingProvider.INSTANCE),
                    new TopicPartition("topic", 0)),
            0);

    private static final Metrics METRICS = Metrics.withTags("subscription", "subscriptionId",
                                                            "topic", "topic",
                                                            "partition", "1",
                                                            "subpartition", "0");

    private static TaskRequest taskRequest() {
        return new TaskRequest(
                new TopicPartition("topic", 1), 1, mock(DeferredCompletion.class), "TEST", null, NoopTrace.INSTANCE, REQUEST.toByteArray());
    }

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private TaskExtractor<HelloTask> extractorMock;

    @Mock
    private DecatonProcessor<HelloTask> processorMock;

    @Mock
    private ExecutionScheduler schedulerMock;

    private ProcessPipeline<HelloTask> pipeline;

    @Before
    public void setUp() {
        pipeline = spy(new ProcessPipeline<>(
                scope, Collections.singletonList(processorMock), null, extractorMock, schedulerMock, METRICS));
    }

    @Test
    public void testScheduleThenProcess_SYNC_COMPLETE() throws InterruptedException {
        when(extractorMock.extract(any()))
                .thenReturn(new DecatonTask<>(TaskMetadata.fromProto(REQUEST.getMetadata()), TASK, TASK.toByteArray()));

        TaskRequest request = taskRequest();
        pipeline.scheduleThenProcess(request);
        verify(schedulerMock, times(1)).schedule(eq(TaskMetadata.fromProto(REQUEST.getMetadata())));
        verify(processorMock, times(1)).process(any(), eq(TASK));
        verify(request.completion(), times(1)).complete();
    }

    @Test
    public void testScheduleThenProcess_ASYNC_COMPLETE() throws InterruptedException {
        when(extractorMock.extract(any()))
                .thenReturn(new DecatonTask<>(TaskMetadata.fromProto(REQUEST.getMetadata()), TASK, TASK.toByteArray()));
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
        verify(schedulerMock, times(1)).schedule(eq(TaskMetadata.fromProto(REQUEST.getMetadata())));
        verify(processorMock, times(1)).process(any(), eq(TASK));

        // Should complete only after processor completes it
        verify(request.completion(), never()).complete();
        beforeComplete.countDown();
        afterComplete.await();
        verify(request.completion(), times(1)).complete();
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
        verify(request.completion(), times(1)).complete();
    }

    @Test
    public void testScheduleThenProcess_ExtractThrows() throws InterruptedException {
        when(extractorMock.extract(any())).thenThrow(new RuntimeException());

        TaskRequest request = taskRequest();
        // Checking exception doesn't bubble up
        pipeline.scheduleThenProcess(request);
        verify(schedulerMock, never()).schedule(any());
        verify(processorMock, never()).process(any(), any());
        verify(request.completion(), times(1)).complete();
    }

    @Test
    public void testExtract_PurgeRawRequestBytes() {
        when(extractorMock.extract(any()))
                .thenReturn(new DecatonTask<>(TaskMetadata.fromProto(REQUEST.getMetadata()), TASK, TASK.toByteArray()));

        TaskRequest request = taskRequest();
        pipeline.extract(request);

        assertNull(request.rawRequestBytes());
    }

    @Test
    public void testScheduleThenProcess_SynchronousFailure() throws InterruptedException {
        DecatonTask<HelloTask> task = new DecatonTask<>(TaskMetadata.fromProto(REQUEST.getMetadata()), TASK, TASK.toByteArray());
        when(extractorMock.extract(any())).thenReturn(task);

        doThrow(new RuntimeException()).when(processorMock).process(any(), eq(TASK));

        TaskRequest request = taskRequest();
        // Checking exception doesn't bubble up
        pipeline.scheduleThenProcess(request);
        verify(request.completion(), times(1)).complete();
    }

    @Test(timeout = 5000)
    public void testScheduleThenProcess_Terminate() throws InterruptedException {
        DecatonTask<HelloTask> task = new DecatonTask<>(TaskMetadata.fromProto(REQUEST.getMetadata()), TASK, TASK.toByteArray());
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
        verify(request.completion(), never()).complete();
    }
}

