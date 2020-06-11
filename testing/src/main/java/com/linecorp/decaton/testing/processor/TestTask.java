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

package com.linecorp.decaton.testing.processor;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.linecorp.decaton.common.Deserializer;
import com.linecorp.decaton.common.Serializer;

import lombok.Value;

/**
 * A test task for {@link ProcessorTestSuite}.
 *
 * Intended to be assigned unique id for each tasks so that every task can be differentiated.
 */
@Value
public class TestTask {
    /**
     * Unique id of the task
     */
    String id;

    private static final ObjectMapper mapper = new ObjectMapper();

    public static class TestTaskSerializer implements Serializer<TestTask> {
        @Override
        public byte[] serialize(TestTask data) {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static class TestTaskDeserializer implements Deserializer<TestTask> {
        @Override
        public TestTask deserialize(byte[] bytes) {
            try {
                return mapper.readValue(bytes, TestTask.class);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
