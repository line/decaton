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

package com.linecorp.decaton.protobuf;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import com.linecorp.decaton.common.Deserializer;
import com.linecorp.decaton.common.Serializer;

/**
 * Implementation of {@link Serializer} which can be used to serialize object of Protocol Buffers message.
 * @param <T> type of protocol buffers message.
 */
public class ProtocolBuffersDeserializer<T extends MessageLite> implements Deserializer<T> {
    private final Parser<T> parser;

    /**
     * Creates new {@link ProtocolBuffersDeserializer} for message type {@link T}.
     * @param parser a {@link Parser} used to instantiate object from bytes. You can pass parser obtained from
     * concrete message type: {@code YourMessageClass.parser()}.
     */
    public ProtocolBuffersDeserializer(Parser<T> parser) {
        this.parser = parser;
    }

    @Override
    public T deserialize(byte[] bytes) {
        try {
            return parser.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
