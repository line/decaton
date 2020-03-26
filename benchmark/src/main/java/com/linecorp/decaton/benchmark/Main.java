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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "DecatonBm", mixinStandardHelpOptions = true)
public final class Main implements Callable<Integer> {
    @Option(names = "--title", description = "Title of the case to test", required = true)
    private String title;

    @Option(names = "--runner", description = "Fully-qualified runner class name that implements Runner", required = true)
    private String runner;

    @Option(names = "--tasks", description = "Number of tasks to generate for testing", required = true)
    private int tasks;

    @Option(names = "--warmup", description = "Number of tasks to apply for warm up execution (class loading, JIT compile...) before start measurement", defaultValue = "10")
    private int warmupTasks;

    @Option(names = "--max-latency", description = "Upper bound latency to inject for simulating processing time for each tasks. Randomly generated latencies between 0 to this value is used for each task", defaultValue = "10")
    private int maxLatencyMs;

    @Option(names = "--bootstrap-servers", description = "Optional bootstrap.servers property. if supplied, the specified kafka cluster is used for benchmarking instead of local embedded clusters")
    private String bootstrapServers;

    @Option(names = "--param", description = "Key-value parameters to supply for runner")
    private Map<String, String> params = new HashMap<>();

    @Override
    public Integer call() throws Exception {
        BenchmarkConfig config = new BenchmarkConfig(
                title, runner, tasks, warmupTasks, maxLatencyMs, bootstrapServers, params);
        Benchmark benchmark = new Benchmark(config);
        BenchmarkResult result = benchmark.run();
        result.print(config, System.out);
        return 0;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}
