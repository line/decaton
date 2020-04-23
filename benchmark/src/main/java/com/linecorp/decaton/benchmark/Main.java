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

import static java.util.Collections.emptyList;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;

import com.linecorp.decaton.benchmark.BenchmarkConfig.ProfilingConfig;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "DecatonBm", mixinStandardHelpOptions = true)
public final class Main implements Callable<Integer> {
    @Option(names = "--title", description = "Title of the case to test", required = true)
    private String title;

    @Option(names = "--runner", description = "Fully-qualified runner class name that implements Runner",
            required = true,
            defaultValue = "com.linecorp.decaton.benchmark.DecatonRunner")
    private String runner;

    @Option(names = "--tasks", description = "Number of tasks to generate for testing", required = true)
    private int tasks;

    @Option(names = "--warmup",
            description = "Number of tasks to apply for warm up execution (class loading, JIT compile...) before start measurement",
            defaultValue = "10")
    private int warmupTasks;

    @Option(names = "--simulate-latency",
            description = "Latency in milliseconds to inject for simulating processing time for each tasks",
            defaultValue = "0")
    private int simulateLatencyMs;

    @Option(names = "--bootstrap-servers",
            description = "Optional bootstrap.servers property. if supplied, the specified kafka cluster is used for benchmarking instead of local embedded clusters")
    private String bootstrapServers;

    @Option(names = "--param", description = "Key-value parameters to supply for runner")
    private Map<String, String> params = new HashMap<>();

    @Option(names = "--profile", description = "Enable profiling of execution with async-profiler")
    private boolean enableProfiling;

    @Option(names = "--profiler-bin", description = "Path to async-profiler's profiler.sh",
            defaultValue = "profiler.sh")
    private Path profilerBin;

    @Option(names = "--profiler-opts", description = "Options to pass for async-profiler's profiler.sh")
    private String profilerOpts;

    private static List<String> parseOptions(String opts) {
        if (opts == null) {
            return emptyList();
        }

        StringTokenizer tok = new StringTokenizer(opts);
        List<String> items = new ArrayList<>();
        while (tok.hasMoreElements()) {
            items.add(tok.nextToken());
        }
        return items;
    }

    @Override
    public Integer call() throws Exception {
        BenchmarkConfig.ProfilingConfig profiling = null;
        if (enableProfiling) {
            profiling = new ProfilingConfig(profilerBin, parseOptions(profilerOpts));
        }
        BenchmarkConfig config = new BenchmarkConfig(
                title, runner, tasks, warmupTasks, simulateLatencyMs, bootstrapServers, params, profiling);

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
