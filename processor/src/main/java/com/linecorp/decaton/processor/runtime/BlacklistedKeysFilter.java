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

package com.linecorp.decaton.processor.runtime;

import static com.linecorp.decaton.processor.ProcessorProperties.CONFIG_IGNORE_KEYS;

import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.ProcessorProperties;

class BlacklistedKeysFilter {
    private static final Logger logger = LoggerFactory.getLogger(BlacklistedKeysFilter.class);

    private volatile Set<String> ignoreKeys;

    BlacklistedKeysFilter(ProcessorProperties props) {
        props.get(CONFIG_IGNORE_KEYS)
             .listen((oldValue, newValue) -> ignoreKeys = new HashSet<>(newValue));
    }

    boolean shouldTake(ConsumerRecord<String, byte[]> record) {
        // Preceding isEmpty() check is for reducing tiny overhead applied for each contains() by calling
        // Object#hashCode. Since ignoreKeys should be empty for most cases..
        if (!ignoreKeys.isEmpty() && ignoreKeys.contains(record.key())) {
            logger.debug("Ignore task which has key configured to ignore: {}", record.key());
            return false;
        }

        return true;
    }
}
