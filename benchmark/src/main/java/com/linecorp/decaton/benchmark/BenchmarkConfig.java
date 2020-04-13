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

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
public class BenchmarkConfig {
    /**
     * Title of this benchmark.
     */
    String title;
    /**
     * Fully-qualified runner class name that implements {@link Runner} interface.
     */
    String runner;
    /**
     * The number of tasks to apply for testing.
     */
    int tasks;
    /**
     * The number of tasks to apply before start measuring performance for warmup execution (considering JIT,
     * class loading...).
     */
    int warmupTasks;
    /**
     * Max latency to simulate as processing duration. Latencies between 0 to this value are randomly generated.
     */
    int simulateLatencyMs;
    /**
     * Optional bootstrap.servers to specify the cluster to use for testing. Otherwise local embedded cluster is
     * used.
     */
    String bootstrapServers;
    /**
     * Implementation specific key-value parameters that are supported by {@link Runner} implementation.
     */
    Map<String, String> params;
}
