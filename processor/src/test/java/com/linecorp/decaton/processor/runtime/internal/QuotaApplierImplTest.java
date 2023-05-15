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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback.Metrics;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.QuotaUsage;
import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.UsageType;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;
import com.linecorp.decaton.protocol.Sample.HelloTask;

public class QuotaApplierImplTest {
    private final TopicPartition tp = new TopicPartition("topic", 42);
    private final HelloTask task = HelloTask.newBuilder()
                                            .setName("hello")
                                            .build();
    private final byte[] key = "key".getBytes(StandardCharsets.UTF_8);

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private Producer<byte[], byte[]> producer;
    @Mock
    private QuotaCallback callback;

    private QuotaApplierImpl applier;

    @Before
    public void setUp() {
        applier = new QuotaApplierImpl(
                producer,
                callback,
                new SubscriptionScope("subscription",
                                      "topic",
                                      Optional.empty(),
                                      Optional.empty(),
                                      ProcessorProperties.builder().build(),
                                      NoopTracingProvider.INSTANCE,
                                      ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                                      DefaultSubPartitioner::new));
    }

    @After
    public void tearDown() {
        applier.close();
    }

    @Test(timeout = 10000L)
    public void testViolateQuota() throws Exception {
        CountDownLatch produceLatch = new CountDownLatch(1);
        Metrics metrics = Metrics.builder().rate(1000).build();
        OffsetState offsetState = new OffsetState(123);
        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(tp.topic(), tp.partition(), 1, key, task.toByteArray());
        doReturn("foo").when(callback).apply(eq(record), eq(metrics));
        doAnswer(inv -> {
            Callback cb = inv.getArgument(1);
            cb.onCompletion(new RecordMetadata(tp, 1, 2, 3, 4, 5), null);
            produceLatch.countDown();
            return null;
        }).when(producer).send(any(), any());
        assertTrue(applier.apply(record, offsetState, new QuotaUsage(UsageType.VIOLATE, metrics)));

        produceLatch.await();
        verify(callback, times(1)).apply(eq(record), eq(metrics));
        verify(producer, times(1)).send(any(), any());
        assertTrue(offsetState.completion().isComplete());

        applier.close();
        verify(callback, times(1)).close();
    }

    @Test
    public void testComplyQuota() throws Exception {
        OffsetState offsetState = new OffsetState(123);
        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(tp.topic(), tp.partition(), 1, key, task.toByteArray());
        assertFalse(applier.apply(record, offsetState, QuotaUsage.COMPLY));

        applier.shapingExecutor.shutdown();
        applier.shapingExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        verify(callback, never()).apply(any(), any());
        verify(producer, never()).send(any());
        assertFalse(offsetState.completion().isComplete());
    }

    @Test(timeout = 10000L)
    public void testIgnoreQueuedTasksAfterClose() throws Exception {
        CountDownLatch firstTaskLatch = new CountDownLatch(1);
        CountDownLatch producerCloseLatch = new CountDownLatch(1);

        OffsetState offsetState1 = new OffsetState(123);
        OffsetState offsetState2 = new OffsetState(124);

        ConsumerRecord<byte[], byte[]> record1 =
                new ConsumerRecord<>(tp.topic(), tp.partition(), 123, key, task.toByteArray());
        ConsumerRecord<byte[], byte[]> record2 =
                new ConsumerRecord<>(tp.topic(), tp.partition(), 124, key, task.toByteArray());

        Metrics metrics = Metrics.builder().rate(1000).build();

        doAnswer(inv -> {
            producerCloseLatch.countDown();
            return null;
        }).when(producer).close(eq(Duration.ZERO));

        doAnswer(inv -> {
            ConsumerRecord<byte[], byte[]> record = inv.getArgument(0);
            if (record.offset() == 123L) {
                firstTaskLatch.countDown();
                producerCloseLatch.await();
            }
            return "foo";
        }).when(callback).apply(any(), any());

        // queue two shaping tasks. first one will be blocked until we initiate close
        assertTrue(applier.apply(record1, offsetState1, new QuotaUsage(UsageType.VIOLATE, metrics)));
        assertTrue(applier.apply(record2, offsetState2, new QuotaUsage(UsageType.VIOLATE, metrics)));

        firstTaskLatch.await();

        applier.close();

        // check the callback is not invoked for 2nd task
        verify(callback, times(1)).apply(any(), any());
        verify(producer, never()).send(any());

        assertFalse(offsetState1.completion().isComplete());
        assertFalse(offsetState2.completion().isComplete());
    }
}
