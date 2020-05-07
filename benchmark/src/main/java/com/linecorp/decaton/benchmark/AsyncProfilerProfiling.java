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

import java.io.File;
import java.lang.ProcessBuilder.Redirect;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

/**
 * Profiler for execution.
 * Internally uses async-profiler https://github.com/jvm-profiling-tools/async-profiler to get
 * stack samples.
 */
@Slf4j
public class AsyncProfilerProfiling implements Profiling {
    private static final int PROFILER_CMD_TIMEOUT_SECS = 30;

    private final Path asyncProfilerBin;
    private final List<String> asyncProfilerOpts;

    public AsyncProfilerProfiling(Path asyncProfilerBin, Collection<String> asyncProfilerOpts) {
        this.asyncProfilerBin = asyncProfilerBin;
        if (asyncProfilerOpts == null) {
            this.asyncProfilerOpts = new ArrayList<>();
        } else {
            this.asyncProfilerOpts = new ArrayList<>(asyncProfilerOpts);
        }
        if (!this.asyncProfilerOpts.contains("-f")) {
            this.asyncProfilerOpts.add("-f");
            this.asyncProfilerOpts.add(outputFileName());
        }
    }

    private static long currentPid() {
        String[] names = ManagementFactory.getRuntimeMXBean().getName().split("@", 2);
        return Long.parseLong(names[0]);
    }

    private static String outputFileName() {
        return "profile-" + currentPid() + ".svg";
    }

    private void exec(String subCommand) {
        List<String> cmd = new ArrayList<>();
        cmd.add(asyncProfilerBin.toString());
        cmd.addAll(asyncProfilerOpts);
        cmd.add(subCommand);
        cmd.add(String.valueOf(currentPid()));
        try {
            Process process = new ProcessBuilder(cmd)
                    .redirectOutput(new File("/dev/stderr"))
                    .redirectError(Redirect.INHERIT)
                    .start();
            if (!process.waitFor(PROFILER_CMD_TIMEOUT_SECS, TimeUnit.SECONDS)) {
                throw new RuntimeException("timed out waiting async-profiler command");
            }
            if (process.exitValue() != 0) {
                throw new RuntimeException("async-profiler exits with error: " + process.exitValue());
            }
        } catch (Exception e) {
            log.error("Failed to run profiler command: {}", cmd, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() {
        log.info("Start profiling execution");
        exec("start");
    }

    private Optional<Path> findOutputPath() {
        for (int i = 0; i < asyncProfilerOpts.size() - 1; i++) {
            if ("-f".equals(asyncProfilerOpts.get(i))) {
                // Avoid printing absolute path expecting the result to be copy & pasted in public
                // place by security concerns.
                Path path = Paths.get("").toAbsolutePath().relativize(
                        Paths.get("", asyncProfilerOpts.get(i + 1)));
                return Optional.of(path);
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<Path> stop() {
        log.info("Finish profiling execution");
        exec("stop");
        return findOutputPath();
    }
}
