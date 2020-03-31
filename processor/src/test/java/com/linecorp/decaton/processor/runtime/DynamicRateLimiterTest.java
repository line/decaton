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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.Property;

public class DynamicRateLimiterTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private Property<Long> prop;

    private BiConsumer<Long, Long> listener;

    @Before
    public void setUp() {
        doAnswer(invocation -> listener = invocation.getArgument(0)).when(prop).listen(any());
    }

    @Test(timeout = 5000)
    public void testDynamicSwitchLimiter() throws InterruptedException {
        RateLimiter oldLimiter = spy(RateLimiter.create(10));

        AtomicBoolean firstCall = new AtomicBoolean(true);
        CountDownLatch readOld = new CountDownLatch(1);
        CountDownLatch recreate = new CountDownLatch(1);
        DynamicRateLimiter dyn = new DynamicRateLimiter(prop) {
            @Override
            RateLimiter createLimiter(long permitsPerSecond) {
                if (firstCall.getAndSet(false)) {
                    return oldLimiter;
                } else {
                    recreate.countDown();
                    try {
                        readOld.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return super.createLimiter(permitsPerSecond);
                }
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(1);

        doAnswer(invocation -> {
            readOld.countDown();
            // The prop listener thread will entire wait on write lock and we have no way to synchronize on it
            // just linger a bit here assuming if anything went wrong prop listener closes limiter.
            Thread.sleep(100);
            return invocation.callRealMethod();
        }).when(oldLimiter).acquire(anyInt()); // Don't switch to acquire() which is defined as default interface method

        // 1. Call prop listener so it reads the current limiter and stops there
        executor.execute(() -> listener.accept(null, 100L));
        recreate.await();

        // 2. Attempt to acquire so it also reads the current limiter and stops at entry
        // of acquire().
        // 3. By below completing readOld latch, the listener becomes runnable again, close old limiter
        // 4. At the end of listener it completes limiterClosed latch so the below finally reaches the actual
        // limiter's acquire() and returns.
        long got = dyn.acquire();

        // We're actually checking that it returns w/o an exception
        assertEquals(0, got);
    }
}
