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
 * An interface for classes deserializing given bytes into object of type {@link T}.
 * @param <T> type of instantiated data.
 */
public interface Deserializer<T> {
    /**
     * Serialize given bytes into object of type {@link T}.
     * @param bytes data to be deserialized.
     * @return object of type {@link T}.
     */
    T deserialize(byte[] bytes);
}
