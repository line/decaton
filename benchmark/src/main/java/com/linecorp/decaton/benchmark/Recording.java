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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.linecorp.decaton.benchmark.BenchmarkResult.Performance.Durations;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Recording {
    static class ExecutionRecord {
        private final AtomicLong maxDeliveryTime = new AtomicLong();
        private final AtomicLong totalDeliveryTime = new AtomicLong();
        private final AtomicLong processCount = new AtomicLong();
        private final int latencyCount;

        public ExecutionRecord(int latencyCount) {
            this.latencyCount = latencyCount;
        }

        void process(Task task, boolean warmup) {
            if (!warmup) {
                long deliveryLatency = System.currentTimeMillis() - task.getProducedTime();
                while (true) {
                    long currentMax = maxDeliveryTime.get();
                    if (deliveryLatency <= currentMax || maxDeliveryTime.compareAndSet(currentMax, deliveryLatency)) {
                        break;
                    }
                }
                totalDeliveryTime.addAndGet(deliveryLatency);
                processCount.incrementAndGet();
            }

            // Simulate processing latency by sleeping a while
            try {
                int latency = task.getProcessLatency();
                if (latency > 0) {
                    for (int i = 0; i < latencyCount; i++) {
                        Thread.sleep(latency);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted", e);
            }
        }
    }

    private final int tasks;
    private final int warmupTasks;
    ExecutionRecord executionRecord;
    private final AtomicInteger processedCount;
    private final CountDownLatch warmupLatch;
    private final CountDownLatch completeLatch;
    private volatile long startTimeMs;
    private volatile long completeTimeMs;

    public Recording(int tasks, int warmupTasks, int latencyCount) {
        this.tasks = tasks;
        this.warmupTasks = warmupTasks;
        executionRecord = new ExecutionRecord(latencyCount);
        processedCount = new AtomicInteger();
        warmupLatch = new CountDownLatch(1);
        if (warmupTasks == 0) {
            warmupLatch.countDown();
        }
        completeLatch = new CountDownLatch(1);
    }

    public void process(Task task) {
        int seq = processedCount.incrementAndGet();
        boolean warmup = seq <= warmupTasks;
        if (seq == warmupTasks + 1) {
            startTimeMs = System.currentTimeMillis();
            log.debug("Start recording time: {}", startTimeMs);
        }
        executionRecord.process(task, warmup);
        if (log.isTraceEnabled()) {
            log.trace("Task {} completed by thread: {}", seq, Thread.currentThread().getName());
        }
        if (seq == warmupTasks) {
            warmupLatch.countDown();
        }
        if (seq == tasks + warmupTasks) {
            completeTimeMs = System.currentTimeMillis();
            log.debug("Finish recording time: {}", completeTimeMs);
            completeLatch.countDown();
        }
    }

    public boolean awaitWarmupComplete(long timeout, TimeUnit unit) throws InterruptedException {
        return warmupLatch.await(timeout, unit);
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return completeLatch.await(timeout, unit);
    }

    public BenchmarkResult.Performance computeResult() {
        Duration execTime = Duration.ofMillis(completeTimeMs - startTimeMs);
        double throughput = (double) tasks / execTime.toMillis() * 1000;
        long avgDeliveryLatency = executionRecord.totalDeliveryTime.get() / executionRecord.processCount.get();

        Durations deliveryLatencies = new Durations(Duration.ofMillis(avgDeliveryLatency),
                                                    Duration.ofMillis(executionRecord.maxDeliveryTime.get()));
        return new BenchmarkResult.Performance(tasks, execTime, throughput, deliveryLatencies);
    }
}
