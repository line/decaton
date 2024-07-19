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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linecorp.decaton.client.internal.TaskMetadataUtil;
import com.linecorp.decaton.protobuf.ProtocolBuffersSerializer;
import com.linecorp.decaton.protocol.Decaton.TaskMetadataProto;
import com.linecorp.decaton.protocol.Sample.HelloTask;

@ExtendWith(MockitoExtension.class)
public class DecatonClientBuilderTest {
    @Mock
    private Producer<byte[], byte[]> producer;

    @Captor
    private ArgumentCaptor<ProducerRecord<byte[], byte[]>> recordCaptor;

    private ProducerRecord<byte[], byte[]> doProduce(DecatonClient<HelloTask> dclient) {
        dclient.put(null, HelloTask.getDefaultInstance());
        verify(producer, times(1)).send(recordCaptor.capture(), any(Callback.class));
        return recordCaptor.getValue();
    }

    @Test
    public void testBuild() {
        String applicationId = "decaton-unit-test";
        String topic = "decaton-topic";
        String instanceId = "localhost";

        DecatonClient<HelloTask> dclient =
                DecatonClient.producing(topic, new ProtocolBuffersSerializer<HelloTask>())
                             .applicationId(applicationId)
                             .instanceId(instanceId)
                             .producerConfig(new Properties())
                             .producerSupplier(config -> producer)
                             .build();

        ProducerRecord<byte[], byte[]> record = doProduce(dclient);
        assertEquals(topic, record.topic());

        TaskMetadataProto metadata = TaskMetadataUtil.readFromHeader(record.headers());
        assertEquals(applicationId, metadata.getSourceApplicationId());
        assertEquals(instanceId, metadata.getSourceInstanceId());
    }
}
