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

package com.linecorp.decaton.benchmark;

import java.io.IOException;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Task {
    public static class KafkaSerializer implements Serializer<Task> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public byte[] serialize(String topic, Task data) {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("serialization error", e);
            }
        }
    }

    public static class KafkaDeserializer implements Deserializer<Task> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public Task deserialize(String topic, byte[] data) {
            try {
                return mapper.readValue(data, Task.class);
            } catch (IOException e) {
                throw new RuntimeException("deserialization error", e);
            }
        }
    }

    /**
     * Timestamp of when this task was produced.
     */
    private long producedTime;
    /**
     * Simulated processing latency for this task.
     */
    private int processLatency;
}
