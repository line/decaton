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

package com.linecorp.decaton.processor.runtime.internal;

import static com.linecorp.decaton.processor.runtime.ProcessorProperties.CONFIG_IGNORE_KEYS;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.runtime.ProcessorProperties;

public class BlacklistedKeysFilter {
    private static final Logger logger = LoggerFactory.getLogger(BlacklistedKeysFilter.class);

    private volatile Set<String> ignoreKeys;

    public BlacklistedKeysFilter(ProcessorProperties props) {
        props.get(CONFIG_IGNORE_KEYS)
             .listen((oldValue, newValue) -> ignoreKeys = new HashSet<>(newValue));
    }

    public boolean shouldTake(ConsumerRecord<byte[], byte[]> record) {
        final byte[] key = record.key();
        if (key == null) {
            return true;
        }

        final String stringKey = new String(key, StandardCharsets.UTF_8);

        // Preceding isEmpty() check is for reducing tiny overhead applied for each contains() by calling
        // Object#hashCode. Since ignoreKeys should be empty for most cases..
        if (!ignoreKeys.isEmpty() && ignoreKeys.contains(stringKey)) {
            logger.debug("Ignore task which has key configured to ignore: {}", stringKey);
            return false;
        }

        return true;
    }
}
