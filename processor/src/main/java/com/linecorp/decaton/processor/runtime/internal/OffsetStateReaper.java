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

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.linecorp.decaton.processor.runtime.Property;

public class OffsetStateReaper implements AutoCloseable {
    private final ExecutorService executor;
    private final Property<Long> completionTimeoutMs;
    private final Clock clock;

    OffsetStateReaper(Property<Long> completionTimeoutMs, Clock clock) {
        this.completionTimeoutMs = completionTimeoutMs;
        executor = Executors.newSingleThreadExecutor(Utils.namedThreadFactory("OffsetStateReaper"));
        this.clock = clock;
    }

    public OffsetStateReaper(Property<Long> completionTimeoutMs) {
        this(completionTimeoutMs, Clock.systemDefaultZone());
    }

    public void maybeReapOffset(OffsetState state) {
        if (state.completion().isComplete()) {
            return;
        }
        long timeoutAt = state.timeoutAt();
        if (timeoutAt < 0) {
            return;
        }
        long now = clock.millis();
        if (timeoutAt <= now) {
            long nextExpireAt = now + completionTimeoutMs.value();
            executor.execute(() -> {
                if (!state.completion().tryExpire()) {
                    state.setTimeout(nextExpireAt);
                }
            });
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
}
