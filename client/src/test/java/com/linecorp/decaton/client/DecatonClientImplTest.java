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

package com.linecorp.decaton.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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

import com.linecorp.decaton.client.DecatonClient.TaskMetaData;
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
    private Producer<String, DecatonTaskRequest> producer;

    @Mock
    private Supplier<Long> timestampSupplier;

    private DecatonClientImpl<HelloTask> client;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, DecatonTaskRequest>> captor;

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
        ProducerRecord<String, DecatonTaskRequest> record = captor.getValue();
        assertEquals(1234, record.timestamp().longValue());
        assertEquals(1234, record.value().getMetadata().getTimestampMillis());
    }

    @Test
    public void testTimestampFieldSetInternallyWithCallback() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance(), ignored -> {});

        verify(producer, times(1)).send(captor.capture(), any(Callback.class));
        ProducerRecord<String, DecatonTaskRequest> record = captor.getValue();
        assertEquals(1234, record.timestamp().longValue());
        assertEquals(1234, record.value().getMetadata().getTimestampMillis());
    }

    @Test
    public void testTimestampFieldSetExternally() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance(), 5678);

        verify(producer, times(1)).send(captor.capture(), any(Callback.class));
        ProducerRecord<String, DecatonTaskRequest> record = captor.getValue();
        assertEquals(5678, record.timestamp().longValue());
        assertEquals(5678, record.value().getMetadata().getTimestampMillis());
    }

    @Test
    public void testTimestampFieldSetExternallyWithCallback() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance(), 5678, ignored -> {});

        verify(producer, times(1)).send(captor.capture(), any(Callback.class));
        ProducerRecord<String, DecatonTaskRequest> record = captor.getValue();
        assertEquals(5678, record.timestamp().longValue());
        assertEquals(5678, record.value().getMetadata().getTimestampMillis());
    }

    @Test
    public void testTaskMetaDataSetExternally() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance(), TaskMetaData.builder()
                                                                      .timestamp(5678L)
                                                                      .scheduledTime(6912L)
                                                                      .build());

        verify(producer, times(1)).send(captor.capture(), any(Callback.class));
        ProducerRecord<String, DecatonTaskRequest> record = captor.getValue();
        assertEquals(5678, record.timestamp().longValue());
        assertEquals(5678, record.value().getMetadata().getTimestampMillis());
        assertEquals(6912, record.value().getMetadata().getScheduledTimeMillis());
    }

    @Test
    public void testWithScheduledTimeSetExternally() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance(), TaskMetaData.builder()
                                                                      .scheduledTime(181234L)
                                                                      .build());

        verify(producer, times(1)).send(captor.capture(), any(Callback.class));
        ProducerRecord<String, DecatonTaskRequest> record = captor.getValue();
        assertEquals(1234, record.timestamp().longValue());
        assertEquals(1234, record.value().getMetadata().getTimestampMillis());
        assertEquals(181234, record.value().getMetadata().getScheduledTimeMillis());
    }

    @Test
    public void testWithEmptyTaskMetaDataSetExternally() {
        doReturn(1234L).when(timestampSupplier).get();

        client.put("key", HelloTask.getDefaultInstance(), TaskMetaData.builder().build());

        verify(producer, times(1)).send(captor.capture(), any(Callback.class));
        ProducerRecord<String, DecatonTaskRequest> record = captor.getValue();
        assertTrue(record.value().getMetadata().getTimestampMillis() > 0);
        assertNotNull(record.value().getMetadata().getSourceApplicationId());
        assertNotNull(record.value().getMetadata().getSourceInstanceId());
    }
}
