/*
 * Copyright 2021 LINE Corporation
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

package com.linecorp.decaton.processor.processors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessingContext;

import lombok.ToString;

public class BatchingProcessor<T> implements DecatonProcessor<T> {
    private static final Logger logger = LoggerFactory.getLogger(CompactionProcessor.class);

    BiConsumer<String, List<BufferedTask<T>>> processor;
    long lingerMillis;
    int capacity;
    long maxRetryCount;

    private final ConcurrentMap<String, BufferedTaskGroup> windowedTasks = new ConcurrentHashMap<>(64, 0.75f,
                                                                                                   2);
    final ScheduledExecutorService scheduler = initScheduler();

    protected ScheduledExecutorService initScheduler() {
        return new ScheduledThreadPoolExecutor(1, r -> {
            Thread th = new Thread(r);
            th.setName("Decaton" + BatchingProcessor.class.getSimpleName()
                       + "-Flusher/" + System.identityHashCode(this));
            return th;
        });
    }

    @ToString
    static class BufferedTask<T> {
        final T task;
        final ProcessingContext<T> context;

        BufferedTask(T task, ProcessingContext<T> context) {
            this.task = task;
            this.context = context;
            context.deferCompletion();
        }
    }

    class BufferedTaskGroup {
        private final String key;
        private final int capacity;
        private final List<BufferedTask<T>> bufferedTasks = new ArrayList<>();
        private final Future<?> scheduledFlush;
        private final AtomicBoolean flushed = new AtomicBoolean();

        // Run in Processor thread for this partition
        BufferedTaskGroup(String key, int capacity) {
            this.key = key;
            this.capacity = capacity;
            scheduledFlush = scheduleFlush();
        }

        // Run from Processor thread for this partition

        /**
         * Will reject new tasks is flush has already happened or is ongoing.
         * @return whether the task was actually added.
         */
        boolean addBufferedTask(ProcessingContext<T> context, T task) {
            if (!context.key().equals(key)) {
                throw new RuntimeException("");
            }
            synchronized (bufferedTasks) {
                if (isFull() || flushed.get()) {
                    return false;
                }
                bufferedTasks.add(new BufferedTask<>(task, context));
            }
            return true;
        }

        private boolean isFull() {
            return bufferedTasks.size() >= capacity;
        }

        private Future<?> scheduleFlush() {
            Runnable flushTask = () -> {
                try {
                    flush();
                } catch (InterruptedException e) {
                    logger.error("interrupted while flushing task group {}", this, e);
                    Thread.currentThread().interrupt();
                }
            };
            return scheduler.schedule(flushTask, lingerMillis, TimeUnit.MILLISECONDS);
        }

        void flush() throws InterruptedException {
            BufferedTaskGroup expected;
            synchronized (bufferedTasks) {
                if (flushed.getAndSet(true)) {
                    logger.info("Already flushed: {}", this);
                    return;
                }
                logger.debug("Flushing task group: {}", this);
                expected = windowedTasks.remove(key);

                if (!bufferedTasks.isEmpty()) {
                    process();
                }
            }

            // Prevent concurrent update from missing tasks.
            // Since there is no synchronization on `windowedTasks`
            // then during scheduled flush another task can be inserted instead
            if (expected != null && expected != this && !expected.flushed.get()) {
                synchronized (expected.bufferedTasks) {
                    if (!expected.flushed.getAndSet(true) && !expected.bufferedTasks.isEmpty()) {
                        logger.warn("Flushing other task group: {}", expected);
                        expected.process();
                    }
                }
            }
        }

        // Run in Processor thread for this partition
        void flushNow() throws InterruptedException {
            if (!scheduledFlush.isDone()) {
                boolean cancelled = scheduledFlush.cancel(false);
                logger.debug("{} previously scheduled flush for user {}", key,
                             cancelled ? "Cancelled" : "Already cancelled");
                flush();
            }
        }

        private void process() throws InterruptedException {
            try {
                processor.accept(key, bufferedTasks);
            } catch (RuntimeException e) {
                // Only retryable tasks are retried.
                for (BufferedTask<T> t : bufferedTasks) {
                    handleTaskFailure(e, t.context, t.task);
                }
            } finally {
                // complete successful and abandoned tasks
                // for retried tasks, this does nothing (deferredCompletion is already complete)
                bufferedTasks.forEach(t -> t.context.deferCompletion().complete());
            }
        }

        private void handleTaskFailure(RuntimeException e, ProcessingContext<T> context, T task)
            throws InterruptedException {
            long currentRetryCount = context.metadata().retryCount();
            if (maxRetryCount > currentRetryCount) {
                logger.warn("Failed to process task<{}>, retrying ({}/{})",
                            task, currentRetryCount, maxRetryCount, e);
                context.retry();
            } else {
                logger.error("Failed to process task<{}>, retries exhausted ({})",
                             task, currentRetryCount, e);
            }
        }

    }

    BatchingProcessor(
        BiConsumer<String, List<BufferedTask<T>>> processor,
        long lingerMillis,
        int capacity
    ) {
        this.lingerMillis = lingerMillis;
        this.capacity = capacity;
        this.processor = processor;
    }

    @Override
    public void process(ProcessingContext<T> context, T task) throws InterruptedException {
        String key = context.key();
        BufferedTaskGroup taskGroup = windowedTasks.get(key);
        if (taskGroup != null) {
            if (taskGroup.addBufferedTask(context, task)) {
                logger.debug("Joined existing task group {}", taskGroup);
                return;
            }
        }
        if (taskGroup == null) {
            logger.debug("Creating new task group for key {}", key);
        } else {
            logger.debug("Cannot reuse task group {}", taskGroup);
            taskGroup.flushNow(); // removes `taskGroup` from `windowedTasks`
        }
        while (true) {
            BufferedTaskGroup freshTaskGroup = new BufferedTaskGroup(key, capacity);
            if (freshTaskGroup.addBufferedTask(context, task)) {
                BufferedTaskGroup replacedTaskGroup = windowedTasks.put(key, freshTaskGroup);
                if (replacedTaskGroup != null) {
                    logger.warn("Previous taskGroup should have been removed already, was: {}",
                                replacedTaskGroup);
                    replacedTaskGroup.flushNow();
                }
                break;
            }
            logger.warn("Could not add task to a fresh buffer for key {}", key);
        }
    }

    @Override
    public String name() {
        return DecatonProcessor.super.name();
    }

    @Override
    public void close() throws Exception {
        DecatonProcessor.super.close();
    }
}
