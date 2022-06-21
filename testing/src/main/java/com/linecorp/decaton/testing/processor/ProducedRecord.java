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

package com.linecorp.decaton.testing.processor;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;

import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;

/**
 * A class which holds produced task along with produce metadata
 */
@Value
@Accessors(fluent = true)
public class ProducedRecord {
    /**
     * Key of the task
     */
    @ToString.Exclude
    byte[] key;
    /**
     * Topic partition the record was sent to
     */
    TopicPartition topicPartition;
    /**
     * Offset in the partition
     */
    long offset;
    /**
     * Produced task
     */
    TestTask task;
    /**
     * Headers that were set on the produced task
     */
    Headers headers;
}
