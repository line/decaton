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

package com.linecorp.decaton.processor.runtime;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
public class VirtualThreadBenchmark {
    private static final int NUM_TASKS = 10_000;
    private static final int SLEEP_DURATION = 10;

    abstract static class BmState {
        ExecutorService executor;

        abstract ExecutorService createExecutor();

        @Setup(Level.Invocation)
        public void setUp() {
            executor = createExecutor();
        }

        @TearDown(Level.Invocation)
        public void tearDown() throws InterruptedException {
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        }
    }

    private static void doWork(ExecutorService executor) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(NUM_TASKS);
        for (int i = 0; i < NUM_TASKS; i++) {
            executor.execute(() -> {
                try {
                    Thread.sleep(SLEEP_DURATION);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                latch.countDown();
            });
        }
        latch.await();
    }

    @State(Scope.Thread)
    public static class BmStatePThreadPool100 extends BmState {
        @Override
        ExecutorService createExecutor() {
            return Executors.newFixedThreadPool(100);
        }
    }

    @Benchmark
    public void physicalPool100(BmStatePThreadPool100 state) throws InterruptedException {
        doWork(state.executor);
    }

    @State(Scope.Thread)
    public static class BmStatePThreadPool1000 extends BmState {
        @Override
        ExecutorService createExecutor() {
            return Executors.newFixedThreadPool(1000);
        }
    }

    @Benchmark
    public void physicalPool1000(BmStatePThreadPool1000 state) throws InterruptedException {
        doWork(state.executor);
    }

    @State(Scope.Thread)
    public static class BmStatePThreadCreate extends BmState {
        @Override
        ExecutorService createExecutor() {
            return Executors.newThreadPerTaskExecutor(Thread.ofPlatform().factory());
        }
    }

    @Benchmark
    public void physical(BmStatePThreadCreate state) throws InterruptedException {
        doWork(state.executor);
    }

    @State(Scope.Thread)
    public static class BmStateVThreadCreate extends BmState {
        @Override
        ExecutorService createExecutor() {
            return Executors.newVirtualThreadPerTaskExecutor();
        }
    }

    @Benchmark
    public void virtual(BmStateVThreadCreate state) throws InterruptedException {
        doWork(state.executor);
    }

    @State(Scope.Thread)
    public static class BmStateVThreadPoolPerTask extends BmState {
        @Override
        ExecutorService createExecutor() {
            return new ThreadPoolExecutor(NUM_TASKS, NUM_TASKS, 0, TimeUnit.SECONDS,
                                          new LinkedBlockingQueue<>(), Thread.ofVirtual().factory());
        }
    }

    @Benchmark
    public void virtualPoolPerTask(BmStateVThreadPoolPerTask state) throws InterruptedException {
        doWork(state.executor);
    }
}
