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

package com.linecorp.decaton.benchmark;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import lombok.Value;
import lombok.experimental.Accessors;

public interface Runner extends AutoCloseable {
    @Value
    @Accessors(fluent = true)
    class Config {
        String bootstrapServers;
        String topic;
        Deserializer<Task> taskDeserializer;
        Map<String, String> parameters;
    }

    /**
     * Initialize consumer for start consuming the topic.
     * The underlying implementation must be configured to consume {@link Config#topic} from
     * {@link Config#bootstrapServers}, using {@link Config#taskDeserializer} to deserialize read record values
     * and pass it immediately to {@link Recording#process(Task)} which is given by an argument.
     *
     * @param config runner configurations.
     * @param recording recording for this benchmark.
     * @param resourceTracker resource tracking interface (use is optional).
     * @throws InterruptedException whenever appropriate.
     */
    void init(Config config, Recording recording, ResourceTracker resourceTracker) throws InterruptedException;

    @Override
    default void close() throws Exception {
        // noop by default
    }

    static Runner fromClassName(String className) {
        try {
            return Class.forName(className).asSubclass(Runner.class).newInstance();
        } catch (IllegalAccessException | ClassNotFoundException | InstantiationException e) {
            throw new RuntimeException("error loading runner class" + className, e);
        }
    }
}
