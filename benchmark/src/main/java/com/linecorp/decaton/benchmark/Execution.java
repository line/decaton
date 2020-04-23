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

import java.util.function.Consumer;

import lombok.Value;
import lombok.experimental.Accessors;

/**
 * Execute benchmark given a configuration.
 */
public interface Execution {
    @Value
    @Accessors(fluent = true)
    class Config {
        String bootstrapServers;
        String topic;
        BenchmarkConfig benchmarkConfig;
    }

    /**
     * A list of commands used to communicate between this execution and the benchmark controller
     * to synchronize at some points where cooperative progress is required.
     */
    enum Stage {
        /**
         * The execution is ready to receive workload for warmup.
         */
        READY_WARMUP,
        /**
         * The execution has done dealing with warmup workloads and ready to receive real workload.
         */
        READY,
        /**
         * The execution has completed measuring real workload and now ready to return the result.
         */
        FINISH,
    }

    /**
     * Execute the benchmark.
     * The implementation should instantiate {@link Runner}, invoke {@code stageCallback} on every
     * points to indicate progress and aggregate the result as {@link BenchmarkResult}.
     *
     * @param config the benchmark configuration.
     * @param stageCallback the callback to tell progress.
     * @return the benchmark result.
     * @throws InterruptedException when interrupted.
     */
    BenchmarkResult execute(Config config, Consumer<Stage> stageCallback) throws InterruptedException;
}
