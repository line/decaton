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
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(BatchingProcessor.class);

    private final ScheduledExecutorService executor;
    private List<BatchingTask<T>> currentBatch = new ArrayList<>();
    private final Supplier<Long> lingerMillisSupplier;
    private final Supplier<Integer> capacitySupplier;
    private final ReentrantLock rollingLock;

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
        this(() -> lingerMillis, () -> capacity);
    }

    /**
     * Instantiate {@link BatchingProcessor} with dynamic lingerMillis and capacity config.
     * <p>
     * The suppliers are verified capable of returning positive numbers during instantiation.
     * If not, this constructor will fail to instantiate the BatchingProcessor instance.
     * <br>
     * If the suppliers return non-positive numbers in processing time, lask-known-good value is used as fallback.
     * </p>
     */
    protected BatchingProcessor(Supplier<Long> lingerMillisSupplier, Supplier<Integer> capacitySupplier) {
        Objects.requireNonNull(lingerMillisSupplier, "lingerMillisSupplier must not be null.");
        Objects.requireNonNull(capacitySupplier, "capacitySupplier must not be null.");

        this.lingerMillisSupplier = validatedAndLKGWrappedSupplier(
                "lingerMillisSupplier",
                lingerMillisSupplier,
                v -> v > 0);

        this.capacitySupplier = validatedAndLKGWrappedSupplier(
                "capacitySupplier",
                capacitySupplier,
                v -> v > 0);

        // initialize last-known-good values or fail fast at constructor time
        this.lingerMillisSupplier.get();
        this.capacitySupplier.get();

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
        rollingLock = new ReentrantLock();

        scheduleFlush();
    }

    private void periodicallyFlushTask() {
        final List<BatchingTask<T>> batch;
        rollingLock.lock();
        try {
            if (!currentBatch.isEmpty()) {
                batch = currentBatch;
                currentBatch = new ArrayList<>();
            } else {
                batch = null;
            }
        } finally {
            rollingLock.unlock();
        }
        if (batch != null) {
            processBatchingTasks(batch);
        }
        scheduleFlush();
    }

    private void scheduleFlush() {
        executor.schedule(this::periodicallyFlushTask, this.lingerMillisSupplier.get(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void process(ProcessingContext<T> context, T task) throws InterruptedException {
        rollingLock.lock();
        try {
            if (currentBatch.size() >= this.capacitySupplier.get()) {
                final List<BatchingTask<T>> batch = currentBatch;
                executor.submit(() -> processBatchingTasks(batch));
                currentBatch = new ArrayList<>();
            }
            BatchingTask<T> newTask = new BatchingTask<>(context.deferCompletion(), context, task);
            currentBatch.add(newTask);
        } finally {
            rollingLock.unlock();
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    private static <V> Supplier<V> validatedAndLKGWrappedSupplier(
            String name,
            Supplier<V> delegate,
            Predicate<V> validator) {
        final AtomicReference<V> lkg = new AtomicReference<>();
        return () -> {
            final V value;
            try {
                value = delegate.get();
            } catch (Exception e) {
                if (lkg.get() != null) {
                    V lkgValue = lkg.get();
                    logger.warn("{} threw exception from get(), using last-known-good value: {}.", name,
                                lkgValue, e);
                    return lkgValue;
                }
                logger.error("{} threw exception from get(). No last-known-good value is available.", name);
                throw new IllegalArgumentException(name + " threw exception from get().", e);
            }

            if (!validator.test(value)) {
                if (lkg.get() != null) {
                    V lkgValue = lkg.get();
                    logger.warn("{} returned invalid value: {}, using last-known-good value: {}.",
                                name,
                                value,
                                lkgValue);
                    return lkgValue;
                }
                logger.error("{} returned invalid value: {}. No last-known-good value is available.",
                             name,
                             value);
                throw new IllegalArgumentException(name + " returned invalid value: " + value);
            }

            lkg.set(value);
            return value;
        };
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
