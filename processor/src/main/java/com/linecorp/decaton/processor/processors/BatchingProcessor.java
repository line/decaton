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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.Completion;

import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * An Abstract {@link DecatonProcessor} to Batch several tasks of type {@code T} to {@code List<T>}
 * and process them at once. e.g. when downstream-DB supports batching I/O (which often very efficient).
 * Batch-flushing should be done in time-based and size-based.
 * @param <T> type of task to batch
 */
public abstract class BatchingProcessor<T> implements DecatonProcessor<T> {

    private final ScheduledExecutorService executor;
    private final List<BatchingTask<T>> windowedTasks = Collections.synchronizedList(new ArrayList<>());
    private final long lingerMillis;
    private final int capacity;

    @Getter
    @Accessors(fluent = true)
    public static class BatchingTask<T> {
        @Getter
        private final Completion completion;
        @Getter
        private final ProcessingContext<T> context;
        @Getter
        private final T task;

        private BatchingTask(Completion completion, ProcessingContext<T> context, T task) {
            this.completion = completion;
            this.context = context;
            this.task = task;
        }
    }

    /**
     * Instantiate {@link BatchingProcessor}.
     * If you only need one limit, please set large enough value to another.
     * @param lingerMillis time limit for this processor. On every lingerMillis milliseconds,
     * tasks in past lingerMillis milliseconds are pushed to {@link BatchingTask#processBatchingTasks(List)}.
     * @param capacity size limit for this processor. Every time tasks’size reaches capacity,
     * tasks in past before reaching capacity are pushed to {@link BatchingTask#processBatchingTasks(List)}.
     */
    protected BatchingProcessor(long lingerMillis, int capacity) {
        this.lingerMillis = lingerMillis;
        this.capacity = capacity;

        ScheduledThreadPoolExecutor scheduledExecutor = new ScheduledThreadPoolExecutor(1, r -> {
                Thread th = new Thread(r);
                th.setName("Decaton" + BatchingProcessor.class.getSimpleName()
                           + '/' + System.identityHashCode(this));
                return th;
            });

        // For faster shutdown cancel all pending flush on executor shutdown.
        // In fact for this purpose we have two options here:
        // A. Cancel all pending flush
        // B. Run all pending flush immediately, regardless remaining time until flush
        // A has a downside that it forces consumer to re-process some amount of tasks.
        // B has a downside that it may causes flush bursting and then on spike for storage/API access in
        // downstream processor.
        // Given that this feature is expected to be used mainly for reducing workload for storage/APIs,
        // we think taking A is much desirable behavior in most cases.
        scheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor = scheduledExecutor;

        scheduleFlush();
    }

    private void flush() {
        if (windowedTasks.isEmpty()) {
            return;
        }
        synchronized (windowedTasks) {
            processBatchingTasks(new ArrayList<>(windowedTasks));
            windowedTasks.clear();
        }
    }

    private Runnable periodicallyFlushTask() {
        return () -> {
            flush();
            scheduleFlush();
        };
    }

    // visible for testing
    protected void scheduleFlush() {
        executor.schedule(periodicallyFlushTask(), lingerMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void process(ProcessingContext<T> context, T task) throws InterruptedException {
        BatchingTask<T> newTask = new BatchingTask<>(context.deferCompletion(), context, task);
        windowedTasks.add(newTask);
        if (windowedTasks.size() >= this.capacity) {
            flush();
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    /**
     * *MUST* call {@link BatchingTask#completion}'s {@link DeferredCompletion#complete()} or
     * {@link BatchingTask#context}'s {@link ProcessingContext#retry()} method.
     */
    abstract void processBatchingTasks(List<BatchingTask<T>> batchingTasks);
}
