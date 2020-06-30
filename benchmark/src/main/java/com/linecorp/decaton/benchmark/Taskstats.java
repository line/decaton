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

import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Taskstats {
    public static final Taskstats NOOP = new Taskstats(null, null);

    private final Path jtaskstatsBin;
    private final Path outputPath;

    public Taskstats(Path jtaskstatsBin, Path outputPath) {
        this.jtaskstatsBin = jtaskstatsBin;
        if (outputPath == null) {
            this.outputPath = Paths.get(outputFileName());
        } else {
            this.outputPath = outputPath;
        }
    }

    private static String outputFileName() {
        return "taskstats-" + JvmUtils.currentPid();
    }

    /**
     * Report taskstats of all java threads in the target JVM and return the path to the file
     * containing the output.
     *
     * @return path to the file containing output.
     */
    public Optional<Path> reportStats() {
        if (jtaskstatsBin == null) {
            return Optional.empty();
        }

        String[] cmd = {
                jtaskstatsBin.toString(),
                String.valueOf(JvmUtils.currentPid())
        };
        try {
            Process process = new ProcessBuilder(cmd)
                    .redirectOutput(outputPath.toFile())
                    .redirectError(Redirect.INHERIT)
                    .start();
            if (process.waitFor() != 0) {
                throw new RuntimeException("jtaskstats exits with error: " + process.exitValue());
            }
        } catch (Exception e) {
            log.error("Failed to run jtaskstats command: {}", cmd, e);
            throw new RuntimeException(e);
        }
        return Optional.of(outputPath);
    }
}
