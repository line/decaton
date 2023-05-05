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

package com.linecorp.decaton.processor.runtime.internal;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

/**
 * A collection of utilities method which are used just internally.
 */
@Slf4j
public final class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    // NumberFormat isn't thread-safe so we have to create and cache an instance for each thread.
    private static final ThreadLocal<NumberFormat> numberFormat =
            ThreadLocal.withInitial(() -> NumberFormat.getNumberInstance(Locale.US));

    private Utils() {}

    static <T> T loggingExceptions(Supplier<T> block, String message, T fallback) {
        try {
            return block.get();
        } catch (Exception e) {
            log.error(message, e);
            return fallback;
        }
    }

    public static class Timer {
        private final long t0;

        Timer() {
            t0 = System.nanoTime();
        }

        /**
         * Returns elapsed duration as a {@link Duration}.
         * @return an instance of {@link Duration}
         */
        public Duration duration() {
            return Duration.ofNanos(elapsedNanos());
        }

        /**
         * Returns elapsed nanoseconds since this {@link Timer} instantiated.
         * @return duration in nanoseconds.
         */
        public long elapsedNanos() {
            return System.nanoTime() - t0;
        }

        /**
         * Returns elapsed microseconds since this {@link Timer} instantiated.
         * @return duration in microseconds.
         */
        public long elapsedMicros() {
            return TimeUnit.NANOSECONDS.toMicros(elapsedNanos());
        }

        /**
         * Returns elapsed milliseconds since this {@link Timer} instantiated.
         * @return duration in milliseconds.
         */
        public long elapsedMillis() {
            return TimeUnit.NANOSECONDS.toMillis(elapsedNanos());
        }

        @Override
        public String toString() {
            return formatNum(elapsedNanos()) + " ns";
        }
    }

    /**
     * Creates and returns a {@link Timer} which can be used to observe duration of particular code path.
     * @return an instance of {@link Timer}
     */
    public static Timer timer() {
        return new Timer();
    }

    /**
     * Formats given {@code number} in human-readable, comma-separated string format.
     * @param number number to format.
     * @return comma-separated string representation of given number.
     */
    public static String formatNum(long number) {
        return numberFormat.get().format(number);
    }

    /**
     * Formats given {@code duration} in human-readable, comma-separated string format.
     * @param duration duration to format.
     * @return comma-separated string representation of given duration.
     */
    public static String formatNanos(Duration duration) {
        return formatNum(duration.toNanos());
    }

    /**
     * Creates and returns a {@link ThreadFactory} which just creates {@link Thread} and then calls
     * {@link Thread#setName(String)} to set its name.
     * @param name the name of thread to be created
     * @return a {@link ThreadFactory}
     */
    public static ThreadFactory namedThreadFactory(String name) {
        return r -> {
            Thread th = new Thread(r);
            th.setName(name);
            return th;
        };
    }

    /**
     * A slightly different version of {@link #namedThreadFactory(String)} which gives a {@link Function} that
     * takes monotonically increasing unique integer to name a {@link Thread}.
     * @param nameFn a {@link Function} which takes monotonically increasing unique integer and returns name of
     * {@link Thread}
     * @return a {@link ThreadFactory}
     */
    public static ThreadFactory namedThreadFactory(Function<Integer, String> nameFn) {
        AtomicInteger threadId = new AtomicInteger();
        return r -> {
            Thread th = new Thread(r);
            th.setName(nameFn.apply(threadId.getAndIncrement()));
            return th;
        };
    }

    /**
     * A slightly different version of {@link Runnable} that accepts work which might be throw an
     * {@link Exception}.
     */
    @FunctionalInterface
    public interface Task {
        void run() throws Exception;
    }

    /**
     * A helper to run multiple tasks in parallel with creating ad-hoc {@link ExecutorService} each time.
     * By using this method, caller can save code for:
     *
     * - Maintaining its own {@link ExecutorService}
     * - Exception logging
     * - Bubbling up the exception seen first.
     *
     * @param subject the subject explaining work of tasks. Used to name executor {@link Thread}s.
     * @param tasks collection of {@link Task}s to run.
     *
     * @return a {@link CompletableFuture} that observers all tasks' result. Containing the exception seen first
     * if any.
     */
    public static CompletableFuture<Void> runInParallel(String subject, Collection<Task> tasks) {
        if (tasks.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        ExecutorService executor = Executors.newFixedThreadPool(
                tasks.size(), namedThreadFactory(i -> subject + '/' + i));
        CompletableFuture<?>[] results =
                tasks.stream()
                     .map(t -> CompletableFuture.runAsync(() -> {
                         try {
                             t.run();
                         } catch (Exception e) {
                             logger.error("{} - execution failed", subject, e);
                             throw new RuntimeException(e);
                         }
                     }, executor))
                     .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(results)
                                .whenComplete((v, e) -> executor.shutdown());
    }
}
