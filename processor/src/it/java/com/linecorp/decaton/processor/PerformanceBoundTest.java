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

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.linecorp.decaton.benchmark.Benchmark;
import com.linecorp.decaton.benchmark.BenchmarkConfig;
import com.linecorp.decaton.benchmark.BenchmarkResult;
import com.linecorp.decaton.benchmark.DecatonRunner;

public class PerformanceBoundTest {
    static final int ITERATIONS = 3;

    @Before
    public void setUp() {
        Assume.assumeTrue(System.getProperty("it.test-perf") != null);
    }

    @Test
    public void testPerformanceBoundBusy() throws InterruptedException {
        Map<String, String> params = new HashMap<>();
        params.put(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY.name(), "8");
        BenchmarkConfig config = BenchmarkConfig.builder()
                                                .title("PerformanceBoundBusy")
                                                .runner(DecatonRunner.class.getName())
                                                .tasks(10_000)
                                                .warmupTasks(10_000)
                                                .simulateLatencyMs(0)
                                                .params(params)
                                                .build();
        Benchmark benchmark = new Benchmark(config);
        List<BenchmarkResult> results = new ArrayList<>(ITERATIONS);
        for (int i = 0; i < ITERATIONS; i++) {
            BenchmarkResult result = benchmark.run();
            results.add(result);
        }
        BenchmarkResult result = BenchmarkResult.aggregateAverage(results);

        assertThat((int) result.performance().throughput(), greaterThanOrEqualTo(10000));
    }

    @Test
    public void testPerformanceBoundWithLatency() throws InterruptedException {
        Map<String, String> params = new HashMap<>();
        params.put(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY.name(), "10");
        BenchmarkConfig config = BenchmarkConfig.builder()
                                                .title("PerformanceBoundWithLatency")
                                                .runner(DecatonRunner.class.getName())
                                                .tasks(10_000)
                                                .warmupTasks(10_000)
                                                .simulateLatencyMs(10)
                                                .params(params)
                                                .build();
        Benchmark benchmark = new Benchmark(config);
        List<BenchmarkResult> results = new ArrayList<>(ITERATIONS);
        for (int i = 0; i < ITERATIONS; i++) {
            BenchmarkResult result = benchmark.run();
            results.add(result);
        }
        BenchmarkResult result = BenchmarkResult.aggregateAverage(results);

        assertThat((int) result.performance().throughput(), greaterThanOrEqualTo(2500));
    }
}
