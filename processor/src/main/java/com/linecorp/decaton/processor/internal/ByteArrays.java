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

package com.linecorp.decaton.processor.internal;

import java.nio.charset.StandardCharsets;

/**
 * A collection of utility methods for {@code byte[]}, which aim to be used internally.
 */
public final class ByteArrays {
    public static String toString(byte[] array) {
        if (array == null) {
            return "null";
        }
        // Quote here to differentiate null vs array of {'n', 'u', 'l', 'l'}
        return '"' + new String(array, StandardCharsets.UTF_8) + '"';
    }

    private ByteArrays() {}
}
