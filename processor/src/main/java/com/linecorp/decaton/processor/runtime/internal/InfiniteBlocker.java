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

import java.util.concurrent.CountDownLatch;

import com.linecorp.decaton.processor.runtime.internal.Utils.Timer;

public class InfiniteBlocker implements RateLimiter {
    private final CountDownLatch latch;

    InfiniteBlocker() {
        latch = new CountDownLatch(1);
    }

    @Override
    public long acquire(int permits) throws InterruptedException {
        if (terminated()) {
            return 0;
        }
        Timer timer = Utils.timer();
        latch.await();
        return timer.elapsedMicros();
    }

    private boolean terminated() {
        return latch.getCount() == 0;
    }

    @Override
    public void close() {
        latch.countDown();
    }
}
