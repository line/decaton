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

import static java.util.stream.Collectors.toMap;

import java.beans.ConstructorProperties;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
                return new Durations(avg.plus(other.avg), max.plus(other.max));
            }

            public Durations div(int d) {
                return new Durations(avg.dividedBy(d), max.dividedBy(d));
            }
        }

        int totalTasks;
        Duration executionTime;
        double throughput;
        Durations deliveryLatency;

        public Performance plus(Performance other) {
            return new Performance(totalTasks + other.totalTasks,
                                   executionTime.plus(other.executionTime),
                                   throughput + other.throughput,
                                   deliveryLatency.plus(other.deliveryLatency));
        }

        public Performance div(int d) {
            return new Performance(totalTasks / d,
                                   executionTime.dividedBy(d),
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

    // TODO: WTF? using @Value here causes compilation error
    public static class JvmStats {
        @Value
        public static class GcStats {
            long count;
            long time;
        }

        Map<String, GcStats> gcStats;

        @ConstructorProperties("gcStats")
        public JvmStats(Map<String, GcStats> gcStats) {
            this.gcStats = gcStats;
        }

        public Map<String, GcStats> gcStats() {
            return gcStats;
        }

        public JvmStats plus(JvmStats other) {
            Map<String, GcStats> merged = new HashMap<>(gcStats);
            for (Entry<String, GcStats> e : other.gcStats.entrySet()) {
                GcStats ovs = e.getValue();
                merged.compute(e.getKey(),
                               (k, v) -> new GcStats((v == null ? 0 : v.count) + ovs.count,
                                                     (v == null ? 0 : v.time) + ovs.time));
            }
            return new JvmStats(merged);
        }

        public JvmStats div(int d) {
            Map<String, GcStats> newStats = gcStats.entrySet().stream().collect(toMap(
                    Entry::getKey,
                    e -> new GcStats(e.getValue().count / d, e.getValue().time / d)));
            return new JvmStats(newStats);
        }
    }

    @Value
    public static class ExtraInfo {
        public static final ExtraInfo EMPTY = new ExtraInfo(null);
        Path profilerOutput;
    }

    Performance performance;
    ResourceUsage resource;
    JvmStats jvmStats;
    ExtraInfo extraInfo;

    public void print(BenchmarkConfig config, OutputStream out) {
        PrintWriter pw = new PrintWriter(out);

        pw.printf("=== %s (%d tasks) ===\n", config.title(), performance.totalTasks);
        pw.printf("# Runner: %s\n", config.runner());
        pw.printf("# Tasks: %d (warmup: %d)\n", config.tasks(), config.warmupTasks());
        pw.printf("# Simulated Latency(ms): %d\n", config.simulateLatencyMs());
        for (Entry<String, String> e : config.params().entrySet()) {
            pw.printf("# Param: %s=%s\n", e.getKey(), e.getValue());
        }
        if (extraInfo != null) {
            if (extraInfo.profilerOutput != null) {
                pw.printf("# Profiler Output: %s\n", extraInfo.profilerOutput);
            }
        }

        pw.printf("--- Performance ---\n");
        pw.printf("Execution Time(ms): %.2f\n", performance.executionTime.toNanos() / 1_000_000.0);
        pw.printf("Throughput: %.2f tasks/sec\n", performance.throughput);
        pw.printf("Delivery Latency(ms): mean=%d max=%d\n",
                  performance.deliveryLatency.avg.toMillis(), performance.deliveryLatency.max.toMillis());

        pw.printf("--- Resource Usage (%d threads observed) ---\n", resource.threads);
        pw.printf("Cpu Time(ms): %.2f\n", resource.totalCpuTimeNs / 1_000_000.0);
        pw.printf("Allocated Heap(KiB): %.2f\n", resource.totalAllocatedBytes / 1024.0);

        pw.printf("--- JVM ---\n");
        jvmStats.gcStats.keySet().stream().sorted().forEach(name -> {
            JvmStats.GcStats values = jvmStats.gcStats.get(name);
            pw.printf("GC (%s) Count: %d\n", name, values.count);
            pw.printf("GC (%s) Time(ms): %d\n", name, values.time);
        });

        pw.flush();
    }

    public BenchmarkResult plus(BenchmarkResult other) {
        return new BenchmarkResult(performance.plus(other.performance),
                                   resource.plus(other.resource),
                                   jvmStats.plus(other.jvmStats),
                                   ExtraInfo.EMPTY);
    }

    public BenchmarkResult div(int d) {
        return new BenchmarkResult(performance.div(d), resource.div(d), jvmStats.div(d), ExtraInfo.EMPTY);
    }

    public static BenchmarkResult aggregateAverage(List<BenchmarkResult> results) {
        return results.stream()
                      .reduce(BenchmarkResult::plus)
                      .map(r -> r.div(results.size()))
                      .orElse(null);
    }
}
