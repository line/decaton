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

package com.linecorp.decaton.client.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Properties;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.client.DecatonClient.TaskMetadata;
import com.linecorp.decaton.protobuf.ProtocolBuffersSerializer;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;
import com.linecorp.decaton.protocol.Sample.HelloTask;

public class DecatonClientImplTest {
    private static final String TOPIC = "topic";
    private static final String APPLICATION_ID = "unittest";
    private static final String INSTANCE_ID = "instance";

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private Producer<byte[], DecatonTaskRequest> producer;

    @Mock
    private Supplier<Long> timestampSupplier;

    private DecatonClientImpl<HelloTask> client;

    @Captor
    private ArgumentCaptor<ProducerRecord<byte[], DecatonTaskRequest>> captor;

    @Before
    public void setUp() {
        client = new DecatonClientImpl<>(TOPIC, new ProtocolBuffersSerializer<>(),
                                         APPLICATION_ID, INSTANCE_ID, new Properties(),
                                         config -> producer, timestampSupplier);
    }

    @Test
    public void testTimestampFieldSetInternally() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance());

        verify(producer, times(1)).send(captor.capture(), any(Callback.class));
        ProducerRecord<byte[], DecatonTaskRequest> record = captor.getValue();
        assertNull(record.timestamp());
        assertEquals(1234, record.value().getMetadata().getTimestampMillis());
    }

    @Test
    public void testTimestampFieldSetInternallyWithCallback() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance(), ignored -> {});

        verify(producer, times(1)).send(captor.capture(), any(Callback.class));
        ProducerRecord<byte[], DecatonTaskRequest> record = captor.getValue();
        assertNull(record.timestamp());
        assertEquals(1234, record.value().getMetadata().getTimestampMillis());
    }

    @Test
    public void testTimestampFieldSetExternally() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance(), 5678L);

        verify(producer, times(1)).send(captor.capture(), any(Callback.class));
        ProducerRecord<byte[], DecatonTaskRequest> record = captor.getValue();
        assertNull(record.timestamp());
        assertEquals(5678L, record.value().getMetadata().getTimestampMillis());
    }

    @Test
    public void testTimestampFieldSetExternallyWithCallback() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance(), 5678, ignored -> {
        });

        verify(producer, times(1)).send(captor.capture(), any(Callback.class));
        ProducerRecord<byte[], DecatonTaskRequest> record = captor.getValue();
        assertNull(record.timestamp());
        assertEquals(5678, record.value().getMetadata().getTimestampMillis());
    }

    @Test
    public void testTaskMetaDataSetExternally() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance(), TaskMetadata.builder()
                                                                      .timestamp(5678L)
                                                                      .scheduledTime(6912L)
                                                                      .build());

        verifyAndAssertTaskMetadata(5678L, 6912L);
    }

    @Test
    public void testWithScheduledTimeSetExternally() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance(), TaskMetadata.builder()
                                                                      .scheduledTime(181234L)
                                                                      .build());

        verifyAndAssertTaskMetadata(1234L, 181234L);
    }

    @Test
    public void testWithEmptyTaskMetaDataSetExternally() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance(), TaskMetadata.builder().build());

        verify(producer, times(1)).send(captor.capture(), any(Callback.class));
        ProducerRecord<byte[], DecatonTaskRequest> record = captor.getValue();
        assertTrue(record.value().getMetadata().getTimestampMillis() > 0);
        assertNotNull(record.value().getMetadata().getSourceApplicationId());
        assertNotNull(record.value().getMetadata().getSourceInstanceId());
    }

    @Test
    public void testSpecifyingPartition() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance(),
                   TaskMetadata.builder()
                               .timestamp(5678L)
                               .scheduledTime(6912L)
                               .build(), 4);

        verify(producer, times(1)).send(captor.capture(), any(Callback.class));
        ProducerRecord<byte[], DecatonTaskRequest> record = captor.getValue();
        assertNotNull(record.partition());
        assertEquals(4, record.partition().intValue());
        assertNull(record.timestamp());
        assertEquals(5678L, record.value().getMetadata().getTimestampMillis());
        assertEquals(6912L, record.value().getMetadata().getScheduledTimeMillis());
    }

    @Test
    public void testSpecifyingPartitionWithoutMetadata() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance(), null, 4);

        verify(producer, times(1)).send(captor.capture(), any(Callback.class));
        ProducerRecord<byte[], DecatonTaskRequest> record = captor.getValue();
        assertNotNull(record.partition());
        assertEquals(4, record.partition().intValue());
    }

    private void verifyAndAssertTaskMetadata(long timestamp, long scheduledTime) {
        verify(producer, times(1)).send(captor.capture(), any(Callback.class));
        ProducerRecord<byte[], DecatonTaskRequest> record = captor.getValue();
        assertNull(record.timestamp());
        assertEquals(timestamp, record.value().getMetadata().getTimestampMillis());
        assertEquals(scheduledTime, record.value().getMetadata().getScheduledTimeMillis());
        assertNotNull(record.value().getMetadata().getSourceApplicationId());
        assertNotNull(record.value().getMetadata().getSourceInstanceId());
    }
}
