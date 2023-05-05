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

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class RateLimiterTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private LongSupplier timeSupplier;

    @Test(timeout = 2000)
    public void testBasic() throws InterruptedException {
        RateLimiter limiter = RateLimiter.create(1L);
        long mustBeZero = limiter.acquire();
        assertEquals(0L, mustBeZero);

        long mustBeThrottled = limiter.acquire();
        assertNotEquals(0L, mustBeThrottled);
    }

    @Test(timeout = 2000)
    public void testTooLargeRateMeansUnlimited() throws InterruptedException {
        TimeUnit currentResolution = TimeUnit.MICROSECONDS;
        long maxRatePerSecond = currentResolution.convert(1L, SECONDS);
        RateLimiter limiter = RateLimiter.create(maxRatePerSecond + 1L);

        assertEquals(0L, limiter.acquire());
    }

    @Test(timeout = 2000)
    public void testMinusRateMeansUnlimited() throws InterruptedException {
        new Random().longs(10L, Long.MIN_VALUE, 0L)
                    .forEach(rate -> {
                                 RateLimiter limiter = RateLimiter.create(rate);
                                 try {
                                     assertEquals(0L, limiter.acquire());
                                 } catch (InterruptedException e) {
                                     fail(e.toString());
                                 }
                             }
                    );
    }

    @Test(timeout = 1000)
    public void testUnlimited() throws InterruptedException {
        RateLimiter limiter = RateLimiter.create(-1L);
        for (int i = 0; i < 50; ++i) {
            long throttledMicros = limiter.acquire();
            assertEquals(0L, throttledMicros);
        }
    }

    @Test(timeout = 2000)
    public void testAveragingThrottling() throws InterruptedException {
        final long rate = 10L;
        final long permitIntervalNanos = SECONDS.toNanos(1L) / rate;

        AveragingRateLimiter limiter = new AveragingRateLimiter(rate, 1.0d, timeSupplier);

        AtomicInteger callCount = new AtomicInteger();
        LongUnaryOperator expectedThrottlingMicros = currentNanos -> NANOSECONDS.toMicros(
                Math.max(permitIntervalNanos * callCount.getAndIncrement() - currentNanos, 0L));

        // 1st call
        doReturn(0L).when(timeSupplier).getAsLong();
        assertEquals(expectedThrottlingMicros.applyAsLong(0L), limiter.reserve(1));

        // 2nd call, 9ms after started
        long currentNanos = MILLISECONDS.toNanos(9L);
        doReturn(currentNanos).when(timeSupplier).getAsLong();
        assertEquals(expectedThrottlingMicros.applyAsLong(currentNanos), limiter.reserve(1));

        // 3rd call, 140ms after started
        currentNanos = MILLISECONDS.toNanos(140L);
        doReturn(currentNanos).when(timeSupplier).getAsLong();
        assertEquals(expectedThrottlingMicros.applyAsLong(currentNanos), limiter.reserve(1));

        // 4th call, 150ms after started
        currentNanos = MILLISECONDS.toNanos(150L);
        doReturn(currentNanos).when(timeSupplier).getAsLong();
        assertEquals(expectedThrottlingMicros.applyAsLong(currentNanos), limiter.reserve(1));

        // 5th call, 500ms after started
        currentNanos = MILLISECONDS.toNanos(500L);
        doReturn(currentNanos).when(timeSupplier).getAsLong();
        assertEquals(expectedThrottlingMicros.applyAsLong(currentNanos), limiter.reserve(1));
    }

    @Test(timeout = 2000)
    public void testInfinitelyBlockedUntilClosed() throws Exception {
        RateLimiter limiter = RateLimiter.create(0L);

        final int threadsCount = 10;
        CountDownLatch startAwait = new CountDownLatch(threadsCount);
        ExecutorService es = Executors.newFixedThreadPool(threadsCount);

        AtomicInteger closedWell = new AtomicInteger();
        for (int i = 0; i < threadsCount; ++i) {
            es.submit(() -> {
                startAwait.countDown();
                try {
                    limiter.acquire();
                    closedWell.getAndIncrement();
                } catch (InterruptedException ignored) {
                }
            });
        }

        startAwait.await();
        limiter.close();

        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, SECONDS);

        assertEquals(threadsCount, closedWell.get());
    }

    @Test(timeout = 2000)
    public void testThrottlingIncludingBlocking() throws InterruptedException {
        RateLimiter limiter = spy(new AveragingRateLimiter(10L, 1.0d, timeSupplier));

        doReturn(0L).when(timeSupplier).getAsLong();
        assertEquals(0L, limiter.acquire());

        // simulating blocking like GC
        doReturn(MILLISECONDS.toNanos(200L)).when(timeSupplier).getAsLong();

        // after 200ms blocking, the next 2 calls shouldn`t be blocked
        assertEquals(0L, limiter.acquire());

        doReturn(MILLISECONDS.toNanos(250L)).when(timeSupplier).getAsLong();
        assertEquals(0L, limiter.acquire());

        // the throttle starts again
        assertNotEquals(0L, limiter.acquire());
    }

    @Test(timeout = 3000)
    public void testTooSlowTask() throws Exception {
        final double maxBurstSeconds = 0.0d;

        RateLimiter limiter = spy(new AveragingRateLimiter(10L, maxBurstSeconds, timeSupplier));
        assertEquals(0L, limiter.acquire());

        // simulating super long processing
        doReturn(HOURS.toNanos(1L)).when(timeSupplier).getAsLong();

        assertEquals(0L, limiter.acquire());

        // the throttle starts again
        assertNotEquals(0L, limiter.acquire());
    }

    @Test(timeout = 3000)
    public void testTooSlowTaskWithBursty() throws Exception {
        final double maxBurstSeconds = 1.0d;

        RateLimiter limiter = spy(new AveragingRateLimiter(10L, maxBurstSeconds, timeSupplier));
        assertEquals(0L, limiter.acquire());

        // simulating super long processing
        long baseMillis = HOURS.toMillis(1L);

        // we got 10 free tickets
        // because limiter keeps 10(rate) * 1.0(maxBurstSeconds) = 10 tickets internally
        for (int i = 0; i < 10; ++i) {
            doReturn(MILLISECONDS.toNanos(baseMillis += 10L)).when(timeSupplier).getAsLong();
            assertEquals(0L, limiter.acquire());
        }

        // one additional pass because we got A permit during the loop above
        assertEquals(0L, limiter.acquire());

        // the throttle starts again
        assertNotEquals(0L, limiter.acquire());
    }
}
