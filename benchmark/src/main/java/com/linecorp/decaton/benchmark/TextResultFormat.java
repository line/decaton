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

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Map.Entry;

import com.linecorp.decaton.benchmark.BenchmarkResult.ExtraInfo;
import com.linecorp.decaton.benchmark.BenchmarkResult.JvmStats;
import com.linecorp.decaton.benchmark.BenchmarkResult.Performance;
import com.linecorp.decaton.benchmark.BenchmarkResult.ResourceUsage;

/**
 * Format {@link BenchmarkResult} into human-readable text format.
 */
public class TextResultFormat implements ResultFormat {
    @Override
    public void print(BenchmarkConfig config, OutputStream out, BenchmarkResult result) {
        PrintWriter pw = new PrintWriter(out);

        Performance perf = result.performance();
        pw.printf("=== %s (%d tasks) ===\n", config.title(), perf.totalTasks());
        pw.printf("# Runner: %s\n", config.runner());
        pw.printf("# Tasks: %d (warmup: %d)\n", config.tasks(), config.warmupTasks());
        pw.printf("# Simulated Latency(ms): %d\n", config.simulateLatencyMs());
        for (Entry<String, String> e : config.params().entrySet()) {
            pw.printf("# Param: %s=%s\n", e.getKey(), e.getValue());
        }
        ExtraInfo extraInfo = result.extraInfo();
        if (extraInfo != null) {
            if (extraInfo.profilerOutput() != null) {
                pw.printf("# Profiler Output: %s\n", extraInfo.profilerOutput());
            }
            if (extraInfo.taskstatsOutput() != null) {
                pw.printf("# Taskstats Output: %s\n", extraInfo.taskstatsOutput());
            }
        }

        pw.printf("--- Performance ---\n");
        pw.printf("Execution Time(ms): %.2f\n", perf.executionTime().toNanos() / 1_000_000.0);
        pw.printf("Throughput: %.2f tasks/sec\n", perf.throughput());
        pw.printf("Delivery Latency(ms): mean=%d max=%d\n",
                  perf.deliveryLatency().avg().toMillis(), perf.deliveryLatency().max().toMillis());

        ResourceUsage resource = result.resource();
        pw.printf("--- Resource Usage (%d threads observed) ---\n", resource.threads());
        pw.printf("Cpu Time(ms): %.2f\n", resource.totalCpuTimeNs() / 1_000_000.0);
        pw.printf("Allocated Heap(KiB): %.2f\n", resource.totalAllocatedBytes() / 1024.0);

        JvmStats jvmStats = result.jvmStats();
        pw.printf("--- JVM ---\n");
        jvmStats.gcStats().keySet().stream().sorted().forEach(name -> {
            JvmStats.GcStats values = jvmStats.gcStats().get(name);
            pw.printf("GC (%s) Count: %d\n", name, values.count());
            pw.printf("GC (%s) Time(ms): %d\n", name, values.time());
        });

        pw.flush();
    }
}
