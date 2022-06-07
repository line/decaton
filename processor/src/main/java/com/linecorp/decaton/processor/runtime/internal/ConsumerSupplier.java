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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class ConsumerSupplier implements Supplier<Consumer<byte[], byte[]>> {
    public static final int DEFAULT_MAX_POLL_RECORDS = 100;

    private static final Map<String, String> configOverwrites = new HashMap<String, String>() {{
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }};

    private final Properties config;

    public ConsumerSupplier(Properties config) {
        this.config = config;
    }

    @Override
    public Consumer<byte[], byte[]> get() {
        return new KafkaConsumer<>(mergedProps(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    private Properties mergedProps() {
        Properties props = new Properties();
        for (String key : config.stringPropertyNames()) {
            props.setProperty(key, config.getProperty(key));
        }
        for (Entry<String, String> entry : configOverwrites.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }

        // max.poll.records handling to avoid memory overused
        if (props.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG) == null) {
            props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                              String.valueOf(DEFAULT_MAX_POLL_RECORDS));
        }

        return props;
    }
}
