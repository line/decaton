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

package com.linecorp.decaton.common;

/**
 * An interface for classes serializing given instance of type {@link T} into bytes.
 * @param <T> type of instantiated data.
 */
public interface Serializer<T> {
    /**
     * Serialize given data into bytes.
     * @param data data to be serialized.
     * @return serialized bytes.
     */
    byte[] serialize(T data);
}
