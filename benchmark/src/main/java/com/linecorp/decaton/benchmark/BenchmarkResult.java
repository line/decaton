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
import java.util.List;
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

            public Durations plus(Durations other) {
                return new Durations(Duration.ofNanos(avg.toNanos() + other.avg.toNanos()),
                                     Duration.ofNanos(max.toNanos() + other.max.toNanos()));
            }

            public Durations div(int d) {
                return new Durations(Duration.ofNanos(avg.toNanos() / d),
                                     Duration.ofNanos(max.toNanos() / d));
            }
        }

        int totalTasks;
        Duration executionTime;
        double throughput;
        Durations deliveryLatency;

        public Performance plus(Performance other) {
            return new Performance(totalTasks + other.totalTasks,
                                   Duration.ofNanos(executionTime.toNanos() + other.executionTime.toNanos()),
                                   throughput + other.throughput,
                                   deliveryLatency.plus(other.deliveryLatency));
        }

        public Performance div(int d) {
            return new Performance(totalTasks / d,
                                   Duration.ofNanos(executionTime.toNanos() / d),
                                   throughput / d,
                                   deliveryLatency.div(d));
        }
    }

    @Value
    public static class ResourceUsage {
        int threads;
        long totalCpuTimeNs;
        long totalAllocatedBytes;

        public ResourceUsage plus(ResourceUsage other) {
            return new ResourceUsage(threads + other.threads,
                                     totalCpuTimeNs + other.totalCpuTimeNs,
                                     totalAllocatedBytes + other.totalAllocatedBytes);
        }

        public ResourceUsage div(int d) {
            return new ResourceUsage(threads / d,
                                     totalCpuTimeNs / d,
                                     totalAllocatedBytes / d);
        }
    }

    Performance performance;
    ResourceUsage resource;

    public void print(BenchmarkConfig config, OutputStream out) {
        PrintWriter pw = new PrintWriter(out);

        pw.printf("=== %s (%d tasks) ===\n", config.title(), performance.totalTasks);
        pw.printf("# Runner: %s\n", config.runner());
        pw.printf("# Tasks: %d (warmup: %d)\n", config.tasks(), config.warmupTasks());
        pw.printf("# Simulated Latency(ms): %d\n", config.simulateLatencyMs());
        for (Entry<String, String> e : config.params().entrySet()) {
            pw.printf("# Param: %s=%s\n", e.getKey(), e.getValue());
        }

        pw.printf("--- Performance ---\n");
        pw.printf("Execution Time(ms): %.2f\n", performance.executionTime.toNanos() / 1_000_000.0);
        pw.printf("Throughput: %.2f tasks/sec\n", performance.throughput);
        pw.printf("Delivery Latency(ms): mean=%d max=%d\n",
                  performance.deliveryLatency.avg.toMillis(), performance.deliveryLatency.max.toMillis());

        pw.printf("--- Resource Usage (%d threads observed) ---\n", resource.threads);
        pw.printf("Cpu Time(ms): %.2f\n", resource.totalCpuTimeNs / 1_000_000.0);
        pw.printf("Allocated Heap(KiB): %.2f\n", resource.totalAllocatedBytes / 1024.0);

        pw.flush();
    }

    public BenchmarkResult plus(BenchmarkResult other) {
        return new BenchmarkResult(performance.plus(other.performance), resource.plus(other.resource));
    }

    public BenchmarkResult div(int d) {
        return new BenchmarkResult(performance.div(d), resource.div(d));
    }

    public static BenchmarkResult aggregateAverage(List<BenchmarkResult> results) {
        return results.stream()
                      .reduce(BenchmarkResult::plus)
                      .map(r -> r.div(results.size()))
                      .orElse(null);
    }
}
