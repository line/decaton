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
import java.time.Duration;
import java.util.Map.Entry;

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
public class BenchmarkResult {
    @Value
    public static class Performance {
        @Value
        public static class Durations {
            Duration avg;
            Duration max;
        }

        int totalTasks;
        Duration executionTime;
        double throughput;
        Durations deliveryLatency;
    }

    @Value
    public static class ResourceUsage {
        int threads;
        long totalCpuTimeNs;
        long totalAllocatedBytes;
    }

    Performance performance;
    ResourceUsage resource;

    public void print(BenchmarkConfig config, OutputStream out) {
        PrintWriter pw = new PrintWriter(out);

        pw.printf("=== %s (%d tasks) ===\n", config.title(), performance.totalTasks);
        pw.printf("# Runner: %s\n", config.runner());
        pw.printf("# Tasks: %d (warmup: %d)\n", config.tasks(), config.warmupTasks());
        pw.printf("# Simulated Latency: 0..%d\n", config.maxLatencyMs());
        for (Entry<String, String> e : config.configs().entrySet()) {
            pw.printf("# Param: %s=%s\n", e.getKey(), e.getValue());
        }

        pw.printf("--- Performance ---\n");
        pw.printf("Execution Time (ms): %.2f\n", performance.executionTime.toNanos() / 1_000_000.0);
        pw.printf("Throughput: %.2f tasks/sec\n", performance.throughput);
        pw.printf("Delivery Latency(ms): mean=%d max=%d\n",
                  performance.deliveryLatency.avg.toMillis(), performance.deliveryLatency.max.toMillis());

        pw.printf("--- Resource Usage (%d threads observed) ---\n", resource.threads);
        pw.printf("Cpu Time(ms): %.2f\n", resource.totalCpuTimeNs / 1_000_000.0);
        pw.printf("Allocated Heap (KiB): %.2f\n", resource.totalAllocatedBytes / 1024.0);

        pw.flush();
    }
}
