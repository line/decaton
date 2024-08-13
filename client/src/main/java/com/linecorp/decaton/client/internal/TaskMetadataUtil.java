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

package com.linecorp.decaton.client.internal;

import java.io.UncheckedIOException;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import com.google.protobuf.InvalidProtocolBufferException;

import com.linecorp.decaton.protocol.Decaton.TaskMetadataProto;

public class TaskMetadataUtil {
    private static final String METADATA_HEADER_KEY = "dt_meta";

    /**
     * Write metadata to {@link Headers}
     * @param metadata task metadata to be written
     * @param headers record header to write to
     */
    public static void writeAsHeader(TaskMetadataProto metadata, Headers headers) {
        headers.remove(METADATA_HEADER_KEY);
        headers.add(METADATA_HEADER_KEY, metadata.toByteArray());
    }

    /**
     * Read metadata from given {@link Headers}
     * @param headers record header to read from
     * @return parsed {@link TaskMetadataProto} or null if header is absent
     * @throws IllegalStateException if metadata bytes is invalid
     */
    public static TaskMetadataProto readFromHeader(Headers headers) {
        Header header = headers.lastHeader(METADATA_HEADER_KEY);
        if (header == null) {
            return null;
        }
        try {
            return TaskMetadataProto.parseFrom(header.value());
        } catch (InvalidProtocolBufferException e) {
            throw new UncheckedIOException(e);
        }
    }
}
