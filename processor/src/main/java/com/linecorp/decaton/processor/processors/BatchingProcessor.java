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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.Completion;
import com.linecorp.decaton.processor.runtime.internal.Utils;

import lombok.Value;
import lombok.experimental.Accessors;

/**
 * An Abstract {@link DecatonProcessor} to Batch several tasks of type {@code T} to {@code List<T>}
 * and process them at once. e.g. when downstream-DB supports batching I/O (which often very efficient).
 * Batch-flushing is done in time-based and size-based.
 * @param <T> type of task to batch
 */
public abstract class BatchingProcessor<T> implements DecatonProcessor<T> {

    private final ScheduledExecutorService executor;
    private List<BatchingTask<T>> currentBatch = new ArrayList<>();
    private final long lingerMillis;
    private final int capacity;

    @Value
    @Accessors(fluent = true)
    public static class BatchingTask<T> {
        Completion completion;
        ProcessingContext<T> context;
        T task;
    }

    /**
     * Instantiate {@link BatchingProcessor}.
     * @param lingerMillis time limit for this processor. On every lingerMillis milliseconds,
     * tasks in past lingerMillis milliseconds are pushed to {@link BatchingTask#processBatchingTasks(List)}.
     * @param capacity size limit for this processor. Every time tasks’size reaches capacity,
     * tasks in past before reaching capacity are pushed to {@link BatchingTask#processBatchingTasks(List)}.
     */
    protected BatchingProcessor(long lingerMillis, int capacity) {
        this.lingerMillis = lingerMillis;
        this.capacity = capacity;

        ScheduledThreadPoolExecutor scheduledExecutor = new ScheduledThreadPoolExecutor(
            1,
            Utils.namedThreadFactory(
                "Decaton" + BatchingProcessor.class.getSimpleName() + '/' + System.identityHashCode(this)
            )
        );

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

    private void periodicallyFlushTask() {
        final List<BatchingTask<T>> batch;
        synchronized (this) {
            if (!currentBatch.isEmpty()) {
                batch = currentBatch;
                currentBatch = new ArrayList<>();
            } else {
                batch = null;
            }
        }
        if (batch != null) {
            processBatchingTasks(batch);
        }
        scheduleFlush();
    }

    private void scheduleFlush() {
        executor.schedule(this::periodicallyFlushTask, lingerMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void process(ProcessingContext<T> context, T task) throws InterruptedException {
        synchronized (this) {
            if (currentBatch.size() >= this.capacity) {
                final List<BatchingTask<T>> batch = currentBatch;
                executor.submit(() -> processBatchingTasks(batch));
                currentBatch = new ArrayList<>();
            }
            BatchingTask<T> newTask = new BatchingTask<>(context.deferCompletion(), context, task);
            currentBatch.add(newTask);
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    /**
     * After complete processing batch of tasks,
     * *MUST* call {@link BatchingTask#completion}'s {@link DeferredCompletion#complete()} or
     * {@link BatchingTask#context}'s {@link ProcessingContext#retry()} method for each {@link BatchingTask}.
     * The above methods is not called automatically even when an error occurs in this method,
     * so design it so that they are called finally by yourself. Otherwise, consumption will stick.
     * BatchingProcessor realizes its function by using {@link ProcessingContext#deferCompletion()}.
     * Reading {@link ProcessingContext#deferCompletion()}'s description will help you.
     * This method runs in different thread from the {@link #process} thread.
     */
    protected abstract void processBatchingTasks(List<BatchingTask<T>> batchingTasks);
}
