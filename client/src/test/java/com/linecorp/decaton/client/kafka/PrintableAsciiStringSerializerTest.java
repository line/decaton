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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

public class PrintableAsciiStringSerializerTest {
    private final PrintableAsciiStringSerializer serializer = new PrintableAsciiStringSerializer();

    @Test
    public void testSerializeAsciiString() {
        String text = "abcdef";
        assertArrayEquals(text.getBytes(UTF_8), serializer.serialize(null, text));
    }

    @Test
    public void testSerializeMultiByteString() {
        String text = "abcdえf";
        assertThrows(SerializationException.class, () -> serializer.serialize(null, text));
    }

    @Test
    public void testSerializeStringContainingNotPrintableChar() {
        String text = "abc" + (char) -1;
        assertThrows(SerializationException.class, () -> serializer.serialize(null, text));
    }
}
