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

import java.util.Arrays;

/**
 * This is a wrapper for {@code byte[]} to support {@link #equals(Object)} and {@link #hashCode()},
 * so that they can be stored as a key or item of HashMap or HashSet.
 *
 * It also supports caching the computed hash code like {@link String}.
 */
public final class HashableByteArray {
    private final byte[] array;
    private final int hash;

    public HashableByteArray(byte[] array) {
        this.array = array;
        hash = Arrays.hashCode(array);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}

        final HashableByteArray that = (HashableByteArray) o;

        return Arrays.equals(array, that.array);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "HashableKey{key=" + ByteArrays.toString(array) + '}';
    }
}
