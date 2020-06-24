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

import java.lang.management.CompilationMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.linecorp.decaton.benchmark.BenchmarkConfig.ProfilingConfig;
import com.linecorp.decaton.benchmark.BenchmarkConfig.TaskStatsConfig;
import com.linecorp.decaton.benchmark.BenchmarkResult.ExtraInfo;
import com.linecorp.decaton.benchmark.BenchmarkResult.JvmStats;
import com.linecorp.decaton.benchmark.BenchmarkResult.Performance;
import com.linecorp.decaton.benchmark.BenchmarkResult.ResourceUsage;
import com.linecorp.decaton.benchmark.JvmTracker.GcStats;
import com.linecorp.decaton.benchmark.ResourceTracker.TrackingValues;
import com.linecorp.decaton.benchmark.Task.KafkaDeserializer;

import lombok.extern.slf4j.Slf4j;

/**
 * An {@link Execution} that executes benchmark within the instantiated JVM.
 */
@Slf4j
public class InProcessExecution implements Execution {
    private static final int COMPILE_TIME_CHECK_INTERVAL_MS = 5000;

    @Override
    public BenchmarkResult execute(Config config, Consumer<Stage> stageCallback) throws InterruptedException {
        BenchmarkConfig bmConfig = config.benchmarkConfig();

        log.info("Loading runner {}", bmConfig.runner());
        Runner runner = Runner.fromClassName(bmConfig.runner());
        Recording recording = new Recording(bmConfig.tasks(), bmConfig.warmupTasks());
        ResourceTracker resourceTracker = new ResourceTracker();
        log.info("Initializing runner {}", bmConfig.runner());

        Runner.Config runnerConfig = new Runner.Config(config.bootstrapServers(),
                                                       config.topic(),
                                                       new KafkaDeserializer(),
                                                       bmConfig.params());
        Profiling profiling = profiling(bmConfig);
        Taskstats taskstats = taskstats(bmConfig);
        runner.init(runnerConfig, recording, resourceTracker);
        final Map<Long, TrackingValues> resourceUsageReport;
        final Map<String, GcStats> jvmReport;
        final Optional<Path> profilerOutput;
        final Optional<Path> taskstatsOutput;
        try {
            stageCallback.accept(Stage.READY_WARMUP);
            if (!recording.awaitWarmupComplete(3, TimeUnit.MINUTES)) {
                throw new RuntimeException("timeout on awaiting benchmark to complete");
            }
            if (!bmConfig.skipWaitingJIT()) {
                awaitJITGetsSettled();
            }

            profiling.start();
            JvmTracker jvmTracker = JvmTracker.create();
            stageCallback.accept(Stage.READY);
            if (!recording.await(3, TimeUnit.MINUTES)) {
                throw new RuntimeException("timeout on awaiting benchmark to complete");
            }
            profilerOutput = profiling.stop();
            taskstatsOutput = taskstats.reportStats();
            jvmReport = jvmTracker.report();
            resourceUsageReport = resourceTracker.report();
        } finally {
            try {
                runner.close();
            } catch (Exception e) {
                log.warn("Failed to close runner - {}", runner.getClass(), e);
            }
        }

        stageCallback.accept(Stage.FINISH);
        return assembleResult(bmConfig, recording, resourceUsageReport, jvmReport, profilerOutput, taskstatsOutput);
    }

    private static Profiling profiling(BenchmarkConfig bmConfig) {
        ProfilingConfig config = bmConfig.profiling();
        if (config == null) {
            return Profiling.NOOP;
        } else {
            return new AsyncProfilerProfiling(config.profilerBin(), config.profilerOpts());
        }
    }

    private static Taskstats taskstats(BenchmarkConfig bmConfig) {
        if (!JvmUtils.isOsLinux()) {
            log.debug("Not enabling taskstats for unsupported operating system: {}",
                      System.getProperty("os.name"));
            return Taskstats.NOOP;
        }
        TaskStatsConfig config = bmConfig.taskstats();
        if (config == null) {
            return Taskstats.NOOP;
        } else {
            return new Taskstats(config.jtaskstatsBin(), config.jtaskstatsOutput());
        }
    }

    private static void awaitJITGetsSettled() throws InterruptedException {
        CompilationMXBean compileMxBean = ManagementFactory.getCompilationMXBean();
        long time = compileMxBean.getTotalCompilationTime();
        log.info("Waiting for JIT compilation to get stable");
        while (true) {
            log.debug("Still waiting JIT compilation to get stable, time={}", time);
            Thread.sleep(COMPILE_TIME_CHECK_INTERVAL_MS);
            long newTime = compileMxBean.getTotalCompilationTime();
            if (time == newTime) {
                break;
            }
            time = newTime;
        }
    }

    private static BenchmarkResult assembleResult(BenchmarkConfig bmConfig,
                                                  Recording recording,
                                                  Map<Long, TrackingValues> resourceUsageReport,
                                                  Map<String, GcStats> jvmReport,
                                                  Optional<Path> profilerOutput,
                                                  Optional<Path> taskstatsOutput) {
        Performance performance = recording.computeResult();
        int threads = resourceUsageReport.size();
        TrackingValues resourceValues =
                resourceUsageReport.values().stream()
                                   .reduce(new TrackingValues(0, 0),
                                           (a, b) -> new TrackingValues(
                                                   a.cpuTime() + b.cpuTime(),
                                                   a.allocatedBytes() + b.allocatedBytes()));
        ResourceUsage resource = new ResourceUsage(threads,
                                                   resourceValues.cpuTime(),
                                                   resourceValues.allocatedBytes());
        Map<String, JvmStats.GcStats> gcStats = jvmReport.entrySet().stream().collect(toMap(
                Entry::getKey,
                e -> new JvmStats.GcStats(e.getValue().count(), e.getValue().time())));
        JvmStats jvmStats = new JvmStats(gcStats);

        if (bmConfig.fileNameOnly()) {
            profilerOutput = profilerOutput.map(Path::getFileName);
            taskstatsOutput = taskstatsOutput.map(Path::getFileName);
        }
        ExtraInfo extraInfo = new ExtraInfo(
                profilerOutput.map(String::valueOf).orElse(null),
                taskstatsOutput.map(String::valueOf).orElse(null));
        return new BenchmarkResult(performance, resource, jvmStats, extraInfo);
    }
}
