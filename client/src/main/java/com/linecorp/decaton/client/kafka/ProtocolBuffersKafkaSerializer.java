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

package com.linecorp.decaton.client.kafka;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.google.protobuf.MessageLite;

public class ProtocolBuffersKafkaSerializer<T extends MessageLite> implements Serializer<T> {
    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
        // noop
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        return data.toByteArray();
    }

    @Override
    public void close() {
        // noop
    }
}
