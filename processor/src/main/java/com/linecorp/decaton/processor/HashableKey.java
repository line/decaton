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

package com.linecorp.decaton.processor;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * This is a wrapper for record keys in {@code byte[]} to support {@link #equals(Object)} and {@link #hashCode()},
 * so that they can be stored as a key or item of HashMap or HashSet.
 *
 * It also supports caching the computed hash code like {@link String}.
 */
public class HashableKey {
    private final byte[] key;
    private int hash;

    public HashableKey(byte[] key) {
        this.key = key;
    }

    public byte[] getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}

        final HashableKey that = (HashableKey) o;

        return Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        if (hash == 0 && key.length > 0) {
            hash = Arrays.hashCode(key);
        }
        return hash;
    }

    @Override
    public String toString() {
        return "HashableKey{key=" + new String(key, StandardCharsets.UTF_8) + '}';
    }
}
