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

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.DecatonTask;
import com.linecorp.decaton.processor.ProcessorProperties;
import com.linecorp.decaton.processor.TaskExtractor;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.metrics.Metrics;
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
                                          Optional.empty(), ProcessorProperties.builder().build()),
                    new TopicPartition("topic", 0)),
            0);

    private static final Metrics METRICS = Metrics.withTags("subscription", "subscriptionId",
                                                            "topic", "topic",
                                                            "partition", "1",
                                                            "subpartition", "0");

    private static TaskRequest taskRequest() {
        return new TaskRequest(
                new TopicPartition("topic", 1), 1, null, "TEST", REQUEST.toByteArray());
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
        pipeline = new ProcessPipeline<>(
                scope, Collections.singletonList(processorMock), null, extractorMock, schedulerMock, METRICS);
    }

    @Test
    public void testScheduleThenProcess() throws InterruptedException {
        when(extractorMock.extract(any()))
                .thenReturn(new DecatonTask<>(TaskMetadata.fromProto(REQUEST.getMetadata()), TASK, TASK.toByteArray()));

        CompletableFuture<Void> future = pipeline.scheduleThenProcess(taskRequest());
        verify(schedulerMock, times(1)).schedule(eq(TaskMetadata.fromProto(REQUEST.getMetadata())));
        verify(processorMock, times(1)).process(any(), eq(TASK));

        assertTrue(future.isDone());
    }

    @Test(expected = RuntimeException.class)
    public void testExtract_InvalidTask() throws InterruptedException {
        when(extractorMock.extract(any()))
                .thenReturn(new DecatonTask<>(null, TASK, TASK.toByteArray()));

        pipeline.extract(taskRequest());
    }

    static class ExtractionException extends RuntimeException {
    }

    @Test(expected = ExtractionException.class)
    public void testExtract_ExtractorThrows() throws InterruptedException {
        when(extractorMock.extract(any()))
                .thenThrow(new ExtractionException());

        pipeline.extract(taskRequest());
    }

    @Test
    public void testExtract_PurgeRawRequestBytes() {
        when(extractorMock.extract(any()))
                .thenReturn(new DecatonTask<>(TaskMetadata.fromProto(REQUEST.getMetadata()), TASK, TASK.toByteArray()));

        TaskRequest requestSpy = spy(taskRequest());

        pipeline.extract(requestSpy);

        verify(requestSpy, times(1)).purgeRawRequestBytes();
    }

    static class ProcessException extends RuntimeException {
    }

    @Test(expected = ProcessException.class)
    public void testProcess_SynchronousFailure() throws InterruptedException {
        DecatonTask<HelloTask> task = new DecatonTask<>(TaskMetadata.fromProto(REQUEST.getMetadata()), TASK, TASK.toByteArray());

        when(extractorMock.extract(any()))
                .thenReturn(task);

        doThrow(new ProcessException()).when(processorMock).process(any(), eq(TASK));

        pipeline.process(taskRequest(), task);
    }
}
