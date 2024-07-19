/*
 * Copyright 2024 LY Corporation
 *
 * LY Corporation licenses this file to you under the Apache License,
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

package com.linecorp.decaton.processor.runtime;

import org.apache.kafka.common.header.Headers;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

/**
 * Represents a single record consumed by Decaton and to be extracted as task
 */
@Value
@Builder
@Accessors(fluent = true)
public class ConsumedRecord {
    /**
     * Headers of the record
     */
    Headers headers;

    /**
     * Key of the record
     */
    byte[] key;

    /**
     * Value of the record
     */
    byte[] value;
}
