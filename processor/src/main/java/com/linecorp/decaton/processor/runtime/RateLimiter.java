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

package com.linecorp.decaton.processor.runtime;

import static java.util.concurrent.TimeUnit.SECONDS;

@FunctionalInterface
public interface RateLimiter extends AutoCloseable {
    long UNLIMITED = -1L;
    long PAUSED = 0L;
    long MAX_RATE = SECONDS.toMicros(1L);

    default long acquire() throws InterruptedException {
        return acquire(1);
    }

    long acquire(int permits) throws InterruptedException;

    @Override
    default void close() throws Exception {}

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
