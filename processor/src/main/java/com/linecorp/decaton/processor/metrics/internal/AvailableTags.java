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

package com.linecorp.decaton.processor.metrics.internal;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.linecorp.decaton.processor.metrics.Metrics;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

/**
 * Holds all available key values in the context at the instantiation of {@link Metrics}.
 */
public class AvailableTags {
    private final Map<String, String> tags;

    private AvailableTags(String[] keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("size must be even, it is a set of key=value pairs");
        }

        tags = new HashMap<>();
        for (int i = 0; i < keyValues.length - 1; i += 2) {
            tags.put(keyValues[i], keyValues[i + 1]);
        }
    }

    public static AvailableTags of(String... keyValues) {
        return new AvailableTags(keyValues);
    }

    public Tags subscriptionScope() {
        return assemble("subscription");
    }

    public Tags topicScope() {
        return assemble("subscription", "topic");
    }

    public Tags partitionScope() {
        return assemble("subscription", "topic", "partition");
    }

    public Tags subpartitionScope() {
        return assemble("subscription", "topic", "partition", "subpartition");
    }

    private Tags assemble(String... keys) {
        return Tags.of(
                Arrays.stream(keys)
                      .map(this::toTag)
                      .collect(Collectors.toSet()));
    }

    private Tag toTag(String key) {
        String value = tags.get(key);
        if (value == null) {
            throw new IllegalArgumentException("tag " + key + " does not exists in available tags");
        }

        return Tag.of(key, value);
    }
}
