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

package com.linecorp.decaton.processor.processors;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.Completion;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.internal.HashableByteArray;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.metrics.Metrics;

import io.micrometer.core.instrument.Counter;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * A {@link DecatonProcessor} to apply pre-processing on tasks to compact multiple tasks into one.
 * This processor can be used to compact multiple tasks which are arrived within configured window.
 * Given the window W=5, if tasks having same key {@code [(1, T1), (2, T4), (3, T6)} are fed to this processor,
 * only the successor of 1 or 2 and 3 are processed by the downstream processor.
 *
 * @param <T> type of task to compact
 */
public class CompactionProcessor<T> implements DecatonProcessor<T> {
    private static final Logger logger = LoggerFactory.getLogger(CompactionProcessor.class);

    // These metrics will be instantiated every time by design, but the overhead is acceptable from micro-benchmark.
    //
    // We cannot instantiate counter in advance because micrometer doesn't support adding tag to existing counter dynamically
    // unlike prometheus simple client. (more precisely, to instantiate counter in advance, we have to introduce some mechanism
    // to retrieve context at the processor creation timing and also processor supplier implementation should be revised a bit,
    // which should be done by separated PR)
    // Even though such situation, we decided to migrate to micrometer because we can benefit a lot from it like various monitoring-system support.
    private static Counter compactedTasks(String subscriptionId) {
        return Counter.builder("processor.compaction.compacted")
                      .description("Counter of the number of tasks compacted")
                      .tags("subscription", subscriptionId)
                      .register(Metrics.registry());
    }

    private static Counter compactedKeys(String subscriptionId) {
        return Counter.builder("processor.compaction.compacted.keys")
                      .description("Counter of the number of keys being added to compaction window")
                      .tags("subscription", subscriptionId)
                      .register(Metrics.registry());
    }

    public enum CompactChoice {
        /**
         * Pick the task given as the first argument.
         */
        PICK_LEFT,
        /**
         * Pick the task given as the second argument.
         */
        PICK_RIGHT,
        /**
         * Both tasks are semantically correct, so either one should be preserved.
         */
        PICK_EITHER;
    }

    private final ScheduledExecutorService executor;
    private final ConcurrentMap<HashableByteArray, CompactingTask<T>> windowedTasks = new ConcurrentHashMap<>();
    private final BiFunction<CompactingTask<T>, CompactingTask<T>, CompactChoice> compactor;
    private final long lingerMillis;

    @Getter
    @Accessors(fluent = true)
    public static class CompactingTask<T> {
        @Getter(AccessLevel.NONE)
        private final Completion completion;
        private final ProcessingContext<T> context;
        private final T task;

        private CompactingTask(Completion completion, ProcessingContext<T> context, T task) {
            this.completion = completion;
            this.context = context;
            this.task = task;
        }
    }

    // visible for testing
    CompactionProcessor(
            long lingerMillis,
            BiFunction<CompactingTask<T>, CompactingTask<T>, CompactChoice> compactor,
            ScheduledThreadPoolExecutor scheduledExecutor) {
        this.lingerMillis = lingerMillis;
        this.compactor = compactor;

        if (scheduledExecutor == null) {
            scheduledExecutor = new ScheduledThreadPoolExecutor(1, r -> {
                Thread th = new Thread(r);
                th.setName("Decaton" + CompactionProcessor.class.getSimpleName()
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
    }

    /**
     * Instantiate {@link CompactionProcessor}.
     * @param lingerMillis time to window tasks fed to this processor. On every lingerMillis milliseconds,
     * succeeded tasks of eacy keys fed in past lingerMillis milliseconds are pushed to downstream
     * processor.
     * @param compactor function which takes two tasks of type {@link T} and returns a value tells the decision
     * which one of tasks should be preserved.
     */
    public CompactionProcessor(
            long lingerMillis, BiFunction<CompactingTask<T>, CompactingTask<T>, CompactChoice> compactor) {
        this(lingerMillis, compactor, null);
    }

    private void flush(byte[] key) throws InterruptedException {
        CompactingTask<T> task = windowedTasks.remove(new HashableByteArray(key));
        if (task == null) {
            return;
        }
        task.completion.completeWith(task.context.push(task.task));
    }

    // visible for testing
    Runnable flushTask(ProcessingContext<T> context) {
        byte[] key = context.key();
        return () -> {
            try {
                flush(key);
            } catch (InterruptedException e) {
                logger.error("interrupted while flushing compacted task result", e);
            } catch (RuntimeException e) {
                logger.warn("flushing compacted task threw an exception", e);
            }
        };
    }

    private void logCompactionOccurrence(ProcessingContext<?> context, T compactedTask, T survivedTask) {
        compactedTasks(context.subscriptionId()).increment();

        if (logger.isTraceEnabled()) {
            logger.trace("task ({}) compacted by successor task ({})", compactedTask, survivedTask);
        }
    }

    private void scheduleFlush(ProcessingContext<T> context) {
        executor.schedule(flushTask(context), lingerMillis, TimeUnit.MILLISECONDS);
        compactedKeys(context.subscriptionId()).increment();
    }

    @Override
    public void process(ProcessingContext<T> context, T task) throws InterruptedException {
        CompactingTask<T> newTask = new CompactingTask<>(context.deferCompletion(), context, task);

        // Even though we do this read and following updates in separate operation, race condition can't be
        // happened because tasks are guaranteed to be serialized by it's key, so simultaneous processing
        // of tasks sharing the same key won't be happen.
        final HashableByteArray key = new HashableByteArray(context.key());
        CompactingTask<T> prevTask = windowedTasks.get(key);
        if (prevTask == null) {
            windowedTasks.put(key, newTask);
            scheduleFlush(context);
            return;
        }

        CompactChoice choice = compactor.apply(prevTask, newTask);

        switch (choice) {
            case PICK_LEFT:
                newTask.completion.complete();
                logCompactionOccurrence(context, newTask.task, prevTask.task);
                break;
            case PICK_RIGHT:
            case PICK_EITHER: // Newer task has larger offset. We want to forward consumed offset.
                // Update the last task with new one.
                Object oldEntry = windowedTasks.put(key, newTask);
                if (oldEntry == null) {
                    // By race condition, there is a chance that the scheduled flush for preceding task just
                    // got fired right after this method checked the key's existence at the beginning of this
                    // method.
                    // In such case we have to re-schedule flush for the new entry that we've been added just
                    // now.
                    scheduleFlush(context);
                } else {
                    // Mark previous(looser) task as completed.
                    // If the previous value returned from windowedTasks#put was null, the previous
                    // task must have been retrieved and now being processed by a scheduled flush so we don't
                    // have to complete it right here.
                    prevTask.completion.complete();
                    logCompactionOccurrence(context, prevTask.task, newTask.task);
                }
                break;
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }
}
