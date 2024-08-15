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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.linecorp.decaton.client.internal.DecatonTaskProducer;
import com.linecorp.decaton.client.internal.TaskMetadataUtil;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.processor.runtime.SubPartitionRuntime;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;
import com.linecorp.decaton.protocol.Sample.HelloTask;
import com.linecorp.decaton.protocol.internal.DecatonInternal.DecatonTaskRequest;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DecatonTaskRetryQueueingProcessorTest {
    private static final Duration RETRY_BACKOFF = Duration.ofMillis(10);

    private static final SubscriptionScope scope = new SubscriptionScope(
            "subscription", "topic",
            SubPartitionRuntime.THREAD_POOL,
            Optional.of(RetryConfig.builder().backoff(RETRY_BACKOFF).build()), Optional.empty(),
            ProcessorProperties.builder().build(), NoopTracingProvider.INSTANCE,
            ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
            DefaultSubPartitioner::new);

    @Mock
    private ProcessingContext<byte[]> context;

    @Mock
    private DecatonTaskProducer producer;

    private DecatonTaskRetryQueueingProcessor processor;

    @BeforeEach
    public void setUp() {
        processor = new DecatonTaskRetryQueueingProcessor(scope, producer);
        doReturn(CompletableFuture.completedFuture(null)).when(producer).sendRequest(any());
        doReturn(new CompletionImpl()).when(context).deferCompletion();
        doReturn("key".getBytes(StandardCharsets.UTF_8)).when(context).key();
        doReturn(TaskMetadata.builder().build()).when(context).metadata();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRetryRequest() throws InterruptedException {
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        TaskMetadata meta =
                TaskMetadata.builder()
                            .sourceApplicationId("unit-test")
                            .sourceInstanceId("testing")
                            .timestampMillis(12345)
                            .retryCount(1)
                            .scheduledTimeMillis(67891)
                            .build();

        doReturn(key).when(context).key();
        doReturn(meta).when(context).metadata();

        HelloTask task = HelloTask.getDefaultInstance();
        long currentTime = System.currentTimeMillis();
        processor.process(context, task.toByteArray());

        ArgumentCaptor<ProducerRecord<byte[], byte[]>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer, times(1)).sendRequest(captor.capture());

        ProducerRecord<byte[], byte[]> record = captor.getValue();
        assertArrayEquals(task.toByteArray(), record.value());

        TaskMetadata gotMeta = TaskMetadata.fromProto(TaskMetadataUtil.readFromHeader(record.headers()));
        assertEquals(meta.sourceApplicationId(), gotMeta.sourceApplicationId());
        assertEquals(meta.sourceInstanceId(), gotMeta.sourceInstanceId());
        assertEquals(meta.timestampMillis(), gotMeta.timestampMillis());
        assertEquals(meta.retryCount() + 1, gotMeta.retryCount());
        assertTrue(gotMeta.scheduledTimeMillis() >= currentTime + RETRY_BACKOFF.toMillis());
    }

    @Test
    public void testDeferCompletion() throws InterruptedException {
        CompletableFuture<HelloTask> future = new CompletableFuture<>();
        CompletionImpl comp = new CompletionImpl();

        doReturn(comp).when(context).deferCompletion();
        doReturn(future).when(producer).sendRequest(any());

        processor.process(context, HelloTask.getDefaultInstance().toByteArray());

        verify(context, times(1)).deferCompletion();
        // Check if the returned completion is associated with the producer's send completion
        assertFalse(comp.isComplete());
        future.complete(null);
        assertTrue(comp.isComplete());
    }

    @Test
    public void testDeferCompletion_EXCEPTION() throws InterruptedException {
        doThrow(new KafkaException("kafka")).when(producer).sendRequest(any());

        try {
            processor.process(context, HelloTask.getDefaultInstance().toByteArray());
            fail("Exception must be thrown on the above statement");
        } catch (RuntimeException ignored) {
        }

        verify(context, never()).deferCompletion();
    }

    @Test
    public void testLegacyRetryTaskFormat() throws Exception {
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        TaskMetadata meta =
                TaskMetadata.builder()
                            .sourceApplicationId("unit-test")
                            .sourceInstanceId("testing")
                            .timestampMillis(12345)
                            .retryCount(1)
                            .scheduledTimeMillis(67891)
                            .build();
        doReturn(key).when(context).key();
        doReturn(meta).when(context).metadata();

        SubscriptionScope scope = new SubscriptionScope(
                "subscription", "topic",
                SubPartitionRuntime.THREAD_POOL,
                Optional.of(RetryConfig.builder().backoff(RETRY_BACKOFF).build()), Optional.empty(),
                ProcessorProperties.builder()
                                   .set(Property.ofStatic(ProcessorProperties.CONFIG_RETRY_TASK_IN_LEGACY_FORMAT,
                                                          true))
                                   .build(), NoopTracingProvider.INSTANCE,
                ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                DefaultSubPartitioner::new);

        DecatonTaskRetryQueueingProcessor processor = new DecatonTaskRetryQueueingProcessor(scope, producer);

        HelloTask task = HelloTask.getDefaultInstance();
        long currentTime = System.currentTimeMillis();
        processor.process(context, task.toByteArray());

        ArgumentCaptor<ProducerRecord<byte[], byte[]>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer, times(1)).sendRequest(captor.capture());

        ProducerRecord<byte[], byte[]> record = captor.getValue();
        DecatonTaskRequest taskRequest = DecatonTaskRequest.parseFrom(record.value());

        assertArrayEquals(task.toByteArray(), taskRequest.getSerializedTask().toByteArray());
        assertNull(TaskMetadataUtil.readFromHeader(record.headers()));

        assertEquals(meta.sourceApplicationId(), taskRequest.getMetadata().getSourceApplicationId());
        assertEquals(meta.sourceInstanceId(), taskRequest.getMetadata().getSourceInstanceId());
        assertEquals(meta.timestampMillis(), taskRequest.getMetadata().getTimestampMillis());
        assertEquals(meta.retryCount() + 1, taskRequest.getMetadata().getRetryCount());
        assertTrue(taskRequest.getMetadata().getScheduledTimeMillis() >= currentTime + RETRY_BACKOFF.toMillis());
    }
}
