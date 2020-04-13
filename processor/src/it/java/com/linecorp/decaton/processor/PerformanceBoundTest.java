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

package com.linecorp.decaton.processor;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.linecorp.decaton.benchmark.Benchmark;
import com.linecorp.decaton.benchmark.BenchmarkConfig;
import com.linecorp.decaton.benchmark.BenchmarkResult;
import com.linecorp.decaton.benchmark.DecatonRunner;

public class PerformanceBoundTest {
    static final int ITERATIONS = 3;
    static final int NUM_TASKS = 10_000;
    static final int NUM_WARMUP_TASKS = 1000;
    static final int MAX_LATENCY_MS = 10;

    static final int THROUGHPUT_BOUND = 3000;

    @Test
    public void testPerformanceBound() throws InterruptedException {
        Map<String, String> params = new HashMap<>();
        params.put(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY.name(), "10");
        BenchmarkConfig config = new BenchmarkConfig(
                "PerformanceBound", DecatonRunner.class.getCanonicalName(),
                NUM_TASKS, NUM_WARMUP_TASKS, MAX_LATENCY_MS, null, params);
        Benchmark benchmark = new Benchmark(config);
        List<BenchmarkResult> results = new ArrayList<>(ITERATIONS);
        for (int i = 0; i < ITERATIONS; i++) {
            BenchmarkResult result = benchmark.run();
            results.add(result);
        }
        System.err.println("results = " + results);
        BenchmarkResult result = BenchmarkResult.aggregateAverage(results);

        assertThat((int) result.performance().throughput(), greaterThanOrEqualTo(THROUGHPUT_BOUND));
    }
}
