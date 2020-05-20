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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.linecorp.decaton.benchmark.BenchmarkResult.Performance.Durations;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Recording {
    private static final ThreadLocal<ChildRecording> CHILD = new ThreadLocal<>();

    static class ChildRecording {
        private long maxDeliveryTime;
        private long totalDeliveryTime;
        private long processCount;

        void process(Task task, boolean warmup) {
            if (!warmup) {
                long deliveryLatency = System.currentTimeMillis() - task.getProducedTime();
                if (deliveryLatency > maxDeliveryTime) {
                    maxDeliveryTime = deliveryLatency;
                }
                totalDeliveryTime += deliveryLatency;
                processCount += 1;
            }

            // Simulate processing latency by sleeping a while
            try {
                int latency = task.getProcessLatency();
                if (latency > 0) {
                    Thread.sleep(latency);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted", e);
            }
        }
    }

    private final int tasks;
    private final int warmupTasks;
    private final List<ChildRecording> children;
    private final AtomicInteger processedCount;
    private final CountDownLatch warmupLatch;
    private final CountDownLatch completeLatch;
    private volatile long startTimeMs;
    private volatile long completeTimeMs;

    public Recording(int tasks, int warmupTasks) {
        this.tasks = tasks;
        this.warmupTasks = warmupTasks;
        children = new ArrayList<>();
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
        ChildRecording child = CHILD.get();
        if (child == null) {
            if (!warmup) {
                log.warn("Thread local recording initialization after warmup period."
                         + " Warmup count might not be enough, thread:{}", Thread.currentThread().getName());
            }
            child = new ChildRecording();
            CHILD.set(child);
            synchronized (children) {
                children.add(child);
            }
        }
        if (seq == warmupTasks + 1) {
            startTimeMs = System.currentTimeMillis();
            log.debug("Start recording time: {}", startTimeMs);
        }
        child.process(task, warmup);
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
        long maxDeliveryLatency = children.stream()
                                          .mapToLong(x -> x.maxDeliveryTime)
                                          .reduce(0, Math::max);
        long avgDeliveryLatency = children.stream().mapToLong(x -> x.totalDeliveryTime).sum()
                                  / children.stream().mapToLong(x -> x.processCount).sum();

        Durations deliveryLatencies = new Durations(Duration.ofMillis(avgDeliveryLatency),
                                                    Duration.ofMillis(maxDeliveryLatency));
        return new BenchmarkResult.Performance(tasks, execTime, throughput, deliveryLatencies);
    }
}
