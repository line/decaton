/*
 * Modified work Copyright 2020 LINE Corp.
 * Original work Copyright (C) 2012 The Guava Authors
 *
 * Licensed to you under the Apache License,
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
 *
 * This class was written by reference to Google's Guava SmoothRateLimiter.
 * https://github.com/google/guava/blob/master/guava/src/com/google/common/util/concurrent/SmoothRateLimiter.java
 */

package com.linecorp.decaton.processor.runtime.internal;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.function.LongSupplier;

import com.linecorp.decaton.processor.runtime.internal.Utils.Timer;

// based on SmoothBursty limiter of guava library
public class AveragingRateLimiter implements RateLimiter {
    private final long startNanos;
    private final double stableIntervalMicros;
    private final double maxPermits;
    private final CountDownLatch latch;
    private final LongSupplier currentTimeNanos;

    private double storedPermits;
    private long nextFreeTicketMicros;

    AveragingRateLimiter(long permitsPerSecond, double maxBurstSeconds, LongSupplier currentTimeNanos) {
        if (permitsPerSecond == 0L) {
            throw new IllegalArgumentException("Rate must not be zero");
        }

        this.currentTimeNanos = currentTimeNanos;
        startNanos = currentTimeNanos.getAsLong();
        stableIntervalMicros = SECONDS.toMicros(1L) / (double) permitsPerSecond;
        maxPermits = maxBurstSeconds * permitsPerSecond;
        latch = new CountDownLatch(1);
    }

    @Override
    public long acquire(int permits) throws InterruptedException {
        if (terminated()) {
            return 0;
        }
        long microsToWait = reserve(permits);
        if (microsToWait <= 0L) {
            return 0L;
        }
        Timer timer = Utils.timer();
        latch.await(microsToWait, MICROSECONDS);
        return timer.elapsedMicros();
    }

    // visible for testing
    synchronized long reserve(int permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("Requested permits (%s) must be positive");
        }

        long nowMicros = nowMicros();
        long momentAvailable = reserveEarliestAvailable(permits, nowMicros);
        return max(momentAvailable - nowMicros, 0L);
    }

    private long reserveEarliestAvailable(int requiredPermits, long nowMicros) {
        resync(nowMicros);

        long returnValue = nextFreeTicketMicros;
        double storedPermitsToSpend = min(requiredPermits, storedPermits);
        double freshPermits = requiredPermits - storedPermitsToSpend;
        long waitMicros = (long) (freshPermits * stableIntervalMicros);

        nextFreeTicketMicros = Math.addExact(nextFreeTicketMicros, waitMicros);

        storedPermits -= storedPermitsToSpend;
        return returnValue;
    }

    private void resync(long nowMicros) {
        if (nowMicros > nextFreeTicketMicros) {
            double newPermits = (nowMicros - nextFreeTicketMicros) / stableIntervalMicros;
            storedPermits = min(maxPermits, storedPermits + newPermits);
            nextFreeTicketMicros = nowMicros;
        }
    }

    private long nowMicros() {
        return NANOSECONDS.toMicros(currentTimeNanos.getAsLong() - startNanos);
    }

    private boolean terminated() {
        return latch.getCount() == 0;
    }

    @Override
    public void close() throws Exception {
        latch.countDown();
    }

    @Override
    public String toString() {
        final double rate = SECONDS.toMicros(1L) / stableIntervalMicros;
        return String.format(Locale.ROOT,
                             "AveragingRateLimiter[stableRate=%3.1fqps]", rate);
    }
}
