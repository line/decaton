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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.Accessors;

abstract public class BatchingProcessor<T> implements DecatonProcessor<T> {

    private final ScheduledExecutorService executor;
    private final List<BatchingTask<T>> windowedTasks = Collections.synchronizedList(new ArrayList<>());
    private final long lingerMillis;
    private final int capacity;

    @Getter
    @Accessors(fluent = true)
    public static class BatchingTask<T> {
        @Getter(AccessLevel.NONE)
        public final Completion completion;
        public final ProcessingContext<T> context;
        public final T task;

        private BatchingTask(Completion completion, ProcessingContext<T> context, T task) {
            this.completion = completion;
            this.context = context;
            this.task = task;
        }
    }

    // visible for testing
    BatchingProcessor(
        long lingerMillis,
        int capacity,
        ScheduledThreadPoolExecutor scheduledExecutor
    ) {
        this.lingerMillis = lingerMillis;
        this.capacity = capacity;

        if (scheduledExecutor == null) {
            scheduledExecutor = new ScheduledThreadPoolExecutor(1, r -> {
                Thread th = new Thread(r);
                th.setName("Decaton" + BatchingProcessor.class.getSimpleName()
                           + '/' + System.identityHashCode(this));
                return th;
            });
        }
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

    public BatchingProcessor(long lingerMillis) {
        this(lingerMillis, Integer.MAX_VALUE, null);
    }

    public BatchingProcessor(long lingerMillis, int capacity) {
        this(lingerMillis, capacity, null);
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

    // visible for testing
    Runnable periodicallyFlushTask() {
        return () -> {
            flush();
            scheduleFlush();
        };
    }

    private void scheduleFlush() {
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
