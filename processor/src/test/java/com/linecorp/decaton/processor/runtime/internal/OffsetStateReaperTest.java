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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.Completion.TimeoutChoice;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;

public class OffsetStateReaperTest {
    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    private static final long COMPLETION_TIMEOUT_MS = 1000;

    private final AtomicLong timeMillis = new AtomicLong();
    @Mock
    private Clock clock;
    private OffsetStateReaper reaper;

    @Before
    public void setUp() {
        doAnswer(invocation -> timeMillis.get()).when(clock).millis();
        reaper = new OffsetStateReaper(
                Property.ofStatic(ProcessorProperties.CONFIG_DEFERRED_COMPLETE_TIMEOUT_MS,
                                  COMPLETION_TIMEOUT_MS),
                Metrics.withTags("subscription", "subsc",
                                 "topic", "topic",
                                 "partition", "1")
                        .new CommitControlMetrics(),
                clock);
    }

    @Test(timeout = 5000)
    public void maybeReapOffset() throws InterruptedException {
        OffsetState state = spy(new OffsetState(100));
        AtomicInteger cbCount = new AtomicInteger();
        state.completion().expireCallback(comp -> {
            if (cbCount.getAndIncrement() == 0) {
                return TimeoutChoice.EXTEND;
            } else {
                return TimeoutChoice.GIVE_UP;
            }
        });

        state.setTimeout(10);
        timeMillis.set(9);
        reaper.maybeReapOffset(state);

        assertFalse(state.completion().isComplete());

        CountDownLatch setToLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            Object ret = invocation.callRealMethod();
            setToLatch.countDown();
            return ret;
        }).when(state).setTimeout(anyLong());
        timeMillis.set(10);
        reaper.maybeReapOffset(state); // First callback call
        setToLatch.await();
        assertFalse(state.completion().isComplete());
        assertEquals(10 + COMPLETION_TIMEOUT_MS, state.timeoutAt());

        timeMillis.set(10 + COMPLETION_TIMEOUT_MS);
        reaper.maybeReapOffset(state); // Second callback call
        state.completion().asFuture().toCompletableFuture().join();
        assertTrue(state.completion().isComplete());

        reaper.maybeReapOffset(state); // Should be no-op
        assertEquals(2, cbCount.get());
    }
}
