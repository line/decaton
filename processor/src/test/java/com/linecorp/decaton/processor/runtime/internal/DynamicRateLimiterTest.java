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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.runtime.Property;

public class DynamicRateLimiterTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    BiConsumer<Long, Long> listener;

    @Mock
    Property<Long> prop;

    DynamicRateLimiter limiter;

    @Before
    public void setUp() {
        doAnswer(invocation -> {
            listener = invocation.getArgument(0);
            return null;
        }).when(prop).listen(any());
        limiter = new DynamicRateLimiter(prop);
    }

    @Test(timeout = 10000)
    public void testAcquireOnSwitch() throws InterruptedException {
        // First setup limiter to pause all execution
        listener.accept(null, RateLimiter.PAUSED);

        final int THREADS = 10;
        final long LIMIT = 4;

        // All threads enters limiter for acquisition
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        CountDownLatch atAcquire = new CountDownLatch(THREADS);
        long[] execTimes = new long[THREADS];
        for (int i = 0; i < THREADS; i++) {
            int id = i;
            executor.execute(() -> {
                try {
                    atAcquire.countDown();
                    limiter.acquire();
                    execTimes[id] = System.nanoTime();
                } catch (InterruptedException e) {
                    fail("got exception");
                }
            });
        }
        atAcquire.await();

        // Dynamically update limit and unblock from pause
        listener.accept(RateLimiter.PAUSED, LIMIT);
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        // Sum up all intervals between executions to get approximate time needed in total to finish all N
        // executions. It should be fine to take diff of all executions even for those in the same group (e.g,
        // the first LIMIT threads) because their diff must be nearly equal to zero which is negligible.
        Arrays.sort(execTimes);
        long sum = 0;
        for (int i = 1; i < execTimes.length; i++) {
            sum += execTimes[i] - execTimes[i - 1];
        }
        // Say we have N threads, L limit of executions per second, only L threads are permitted to execute
        // so in total at least N / L seconds will be needed to complete executing all threads.
        long expectedTime = THREADS / LIMIT;
        long sumSeconds = TimeUnit.NANOSECONDS.toSeconds(sum);
        assertTrue(String.format("%d >= %d", sumSeconds, expectedTime), sumSeconds >= expectedTime);
    }
}
