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

import static java.util.concurrent.TimeUnit.SECONDS;

@FunctionalInterface
public interface RateLimiter extends AutoCloseable {
    long UNLIMITED = -1L;
    long PAUSED = 0L;
    long MAX_RATE = SECONDS.toMicros(1L);

    /**
     * Acquire single execution from this limiter.
     * Blocks until next execution is permitted and returns duration in microseconds that it has blocked.
     * For any access that is simultaneous or later of {@link #close()} call, this method returns immediately.
     *
     * @return duration in microseconds that it has blocked.
     * @throws InterruptedException when interrupted.
     */
    default long acquire() throws InterruptedException {
        return acquire(1);
    }

    /**
     * Acquire given number of executions from this limiter for execution.
     * Blocks until next execution is permitted and returns duration in microseconds that it has blocked.
     * For any access that is simultaneous or later of {@link #close()} call, this method returns immediately.
     *
     * @param permits number of executions.
     * @return duration in microseconds that it has blocked.
     * @throws InterruptedException when interrupted.
     */
    long acquire(int permits) throws InterruptedException;

    @Override
    default void close() throws Exception {}

    /**
     * Create a new {@link RateLimiter} with appropriate implementation based on given parameter.
     * @param permitsPerSecond the number of executions to permit for every second.
     * @return a {@link RateLimiter}.
     */
    static RateLimiter create(long permitsPerSecond) {
        if (permitsPerSecond == PAUSED) {
            return new InfiniteBlocker();
        }

        if (permitsPerSecond < 0L || permitsPerSecond > MAX_RATE) {
            return permits -> 0L;
        }

        return new AveragingRateLimiter(permitsPerSecond, 1.0d, System::nanoTime);
    }
}
