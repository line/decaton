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

package com.linecorp.decaton.client;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * A representation of successfully finished task put for Decaton.
 */
public class PutTaskResult {
    private final String id;

    public PutTaskResult(String id) {
        this.id = id;
    }

    public PutTaskResult(RecordMetadata metadata) {
        this(metadataToTaskId(metadata));
    }

    private static String metadataToTaskId(RecordMetadata metadata) {
        return metadata.topic() + '-' + metadata.partition() + '-' + metadata.offset();
    }

    /**
     * An unique task id which is assigned for this task.
     * @return an unique id of put task
     */
    public String id() {
        return id;
    }
}
