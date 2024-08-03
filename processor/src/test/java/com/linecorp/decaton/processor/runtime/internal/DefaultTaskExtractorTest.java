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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import com.linecorp.decaton.processor.runtime.ConsumedRecord;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;
import com.linecorp.decaton.protocol.internal.DecatonInternal.DecatonTaskRequest;
import com.linecorp.decaton.protocol.Decaton.TaskMetadataProto;
import com.linecorp.decaton.protocol.Sample.HelloTask;

public class DefaultTaskExtractorTest {
    private static final HelloTask TASK = HelloTask.getDefaultInstance();

    private static final DecatonTaskRequest LEGACY_REQUEST =
            DecatonTaskRequest.newBuilder()
                              .setMetadata(TaskMetadataProto.newBuilder().setTimestampMillis(1561709151628L).build())
                              .setSerializedTask(TASK.toByteString())
                              .build();
    @Test
    public void testExtract() {
        DefaultTaskExtractor<HelloTask> extractor = new DefaultTaskExtractor<>(
                new ProtocolBuffersDeserializer<>(HelloTask.parser()));

        ConsumedRecord record = ConsumedRecord
                .builder()
                .headers(new RecordHeaders())
                .value(LEGACY_REQUEST.toByteArray())
                .build();

        DecatonTask<HelloTask> extracted = extractor.extract(record);

        assertEquals(LEGACY_REQUEST.getMetadata(), extracted.metadata().toProto());
        assertEquals(TASK, extracted.taskData());

        assertArrayEquals(TASK.toByteArray(), extracted.taskDataBytes());
    }
}
