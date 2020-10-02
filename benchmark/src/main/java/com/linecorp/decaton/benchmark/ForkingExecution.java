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
import java.lang.ProcessBuilder.Redirect;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import com.linecorp.decaton.benchmark.BenchmarkRmi.RemoteCallback;

import lombok.extern.slf4j.Slf4j;

/**
 * An {@link Execution} that executes the benchmark in a separate JVM process.
 */
@Slf4j
public class ForkingExecution implements Execution {
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
              .registerModule(new JavaTimeModule());
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Config config = mapper.readValue(args[0], Config.class);
        RemoteCallback remoteCallback = BenchmarkRmi.lookup(Integer.parseInt(args[1]));

        InProcessExecution execution = new InProcessExecution();
        BenchmarkResult result = execution.execute(config, stage -> {
            try {
                remoteCallback.onStage(stage);
            } catch (RemoteException e) {
                log.error("Failed to invoke remote callback", e);
                throw new RuntimeException(e);
            }
        });

        remoteCallback.onResult(mapper.writeValueAsString(result));
    }

    @Override
    public BenchmarkResult execute(Config config, Consumer<Stage> stageCallback) {
        List<String> cmd = new ArrayList<>();

        final BenchmarkResult result;
        final Process process;
        try (BenchmarkRmi rmi = new BenchmarkRmi()) {
            CompletableFuture<String> resultFuture = rmi.start();

            cmd.add(javaBin().toString());
            cmd.add("-server");
            cmd.addAll(jvmFlags());
            cmd.add("-cp"); cmd.add(currentClasspath());
            cmd.add(ForkingExecution.class.getName());
            cmd.add(serializeConfig(config));
            cmd.add(String.valueOf(rmi.port()));

            log.debug("Forking child process for run with: {}", cmd);
            process = new ProcessBuilder()
                    .command(cmd)
                    .redirectError(Redirect.INHERIT)
                    .redirectOutput(Redirect.INHERIT)
                    .start();

            Stage stage;
            while ((stage = rmi.pollStage()) != Stage.FINISH) {
                stageCallback.accept(stage);
            }
            result = mapper.readValue(resultFuture.join(), BenchmarkResult.class);
        } catch (IOException e) {
            log.error("Failed to spawn child process: {}", cmd, e);
            throw new RuntimeException("failed to spawn child process", e);
        }

        try {
            process.waitFor();
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting child", e);
        }

        if (process.exitValue() != 0) {
            throw new RuntimeException("child exit with error: " + process.exitValue());
        }
        return result;
    }

    private static Path javaBin() {
        return Paths.get(System.getProperty("java.home"), "bin", "java");
    }

    private static String currentClasspath() {
        return ManagementFactory.getRuntimeMXBean().getClassPath();
    }

    private static List<String> jvmFlags() {
        return ManagementFactory.getRuntimeMXBean().getInputArguments();
    }

    private static String serializeConfig(Config config) {
        try {
            return mapper.writeValueAsString(config);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("failed to serialize config", e);
        }
    }
}
