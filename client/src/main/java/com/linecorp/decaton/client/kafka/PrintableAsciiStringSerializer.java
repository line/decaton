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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class PrintableAsciiStringSerializer implements Serializer<String> {
    private static final byte[] EMPTY_BYTES = new byte[0];
    public static final byte MIN_VALUE = 32;
    public static final byte MAX_VALUE = 126;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // noop
    }

    @Override
    public byte[] serialize(String topic, String data) {
        if (data == null) {
            return null;
        }

        if (data.isEmpty()) {
            return EMPTY_BYTES;
        }

        int length = data.length();
        byte[] bytes = new byte[length];

        for (int i = 0; i < length; i++) {
            char c = data.charAt(i);
            if (c < MIN_VALUE || c > MAX_VALUE) {
                throw new SerializationException("illegal character: " + c);
            }
            bytes[i] = (byte) c;
        }

        return bytes;
    }

    @Override
    public void close() {
        // noop
    }
}
