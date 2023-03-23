/*
 * Copyright 2023 LINE Corporation
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.client.internal.DecatonTaskProducer;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback.Action;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback.Metric;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.QuotaUsage;
import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.UsageType;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;
import com.linecorp.decaton.protocol.Sample.HelloTask;

public class DecatonShapingProcessorTest {
    private final TaskMetadata metadata = TaskMetadata.builder()
                                                      .sourceApplicationId("application")
                                                      .sourceInstanceId("instance")
                                                      .timestampMillis(1678006758149L)
                                                      .build();
    private final HelloTask task = HelloTask.newBuilder()
                                            .setName("hello")
                                            .build();

    private final byte[] key = "key".getBytes(StandardCharsets.UTF_8);

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private ProcessingContext<QuotaAwareTask<HelloTask>> context;
    @Mock
    private DecatonTaskProducer producer;
    @Mock
    private QuotaCallback<HelloTask> quotaCallback;

    private DecatonShapingProcessor<HelloTask> processor;

    @Before
    public void setUp() {
        doReturn(Action.builder().topic("topic-shaping").build()).when(quotaCallback).apply(any(), any(), any(), any());
        processor = new DecatonShapingProcessor<>(
                new SubscriptionScope("subscription", "topic",
                                      Optional.empty(), Optional.empty(), ProcessorProperties.builder().build(),
                                      NoopTracingProvider.INSTANCE,
                                      ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                                      DefaultSubPartitioner::new),
                producer,
                quotaCallback);
    }

    @Test
    public void testViolateQuota() throws Exception {
        doReturn(metadata).when(context).metadata();
        doReturn(key).when(context).key();
        doReturn(CompletionImpl.completedCompletion()).when(context).deferCompletion();
        doReturn(CompletableFuture.completedFuture(null)).when(producer).sendRequest(
                any(), any(), any(), any());
        processor.process(context, new QuotaAwareTask<>(
                task, task.toByteArray(), new QuotaUsage(UsageType.Violate, Metric.builder().rate(42).build())));

        verify(producer, times(1))
                .sendRequest(eq("topic-shaping"),
                             eq(key),
                             eq(DecatonTaskRequest.newBuilder()
                                                  .setMetadata(metadata.toProto())
                                                  .setSerializedTask(task.toByteString())
                                                  .build()),
                             isNull());
        verify(context, never()).push(any());
    }

    @Test
    public void testComplyQuota() throws Exception {
        QuotaAwareTask<HelloTask> quotaAwareTask =
                new QuotaAwareTask<>(task, task.toByteArray(), QuotaUsage.COMPLY);
        doReturn(metadata).when(context).metadata();
        doReturn(key).when(context).key();
        doReturn(CompletionImpl.completedCompletion()).when(context).deferCompletion();
        doReturn(CompletionImpl.completedCompletion()).when(context).push(any());
        processor.process(context, quotaAwareTask);

        verify(producer, never()).sendRequest(
                any(), any(), any(), any());
        verify(context, times(1)).push(eq(quotaAwareTask));
    }
}
