/*
 * Copyright 2023 LINE Corporation
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

import java.time.Duration;
import java.util.function.Supplier;

import com.linecorp.decaton.processor.runtime.internal.KeyCounter.Key;

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;

/**
 * Providing per-key statistics over recent time window.
 */
class WindowedKeyStat {
    private static final int NUM_WINDOWS = 2;

    @Value
    @Accessors(fluent = true)
    static class Stat {
        long fromMs;
        long toMs;
        long count;
    }

    @AllArgsConstructor
    private static class Window {
        private long startMs;
        private final KeyCounter counter;

        void expire() {
            startMs = -1;
        }

        boolean isValid() {
            return startMs >= 0;
        }
    }

    private final Window[] ring;
    private final long windowDurationMs;
    private int currentWindowIndex;

    WindowedKeyStat(Duration window, Supplier<KeyCounter> counterSupplier) {
        windowDurationMs = window.toMillis();
        currentWindowIndex = 0;
        ring = new Window[NUM_WINDOWS];
        for (int i = 0; i < ring.length; i++) {
            ring[i] = new Window(-1, counterSupplier.get());
        }
    }

    /**
     * Record key occurrence at the given time and return the statistic of the key.
     * @param nowMs current time in epoch millis
     * @param key the key to record
     */
    public Stat recordAndGet(long nowMs, byte[] key) {
        Window currentWindow = ring[currentWindowIndex];
        // window is not initialized yet
        if (!currentWindow.isValid()) {
            currentWindow.startMs = nowMs;
        }

        // rotate if necessary
        if (nowMs - currentWindow.startMs >= windowDurationMs) {
            currentWindowIndex = (currentWindowIndex + 1) % NUM_WINDOWS;
            currentWindow = ring[currentWindowIndex];
            currentWindow.startMs = nowMs;
            currentWindow.counter.reset();
        }

        Key calculatedKey = currentWindow.counter.createKey(key);

        long keyCount = currentWindow.counter.incrementAndGet(calculatedKey, 1);
        long fromMs = currentWindow.startMs;

        // query other windows which are not expired
        for (int i = 1; i < NUM_WINDOWS; i++) {
            int j = (currentWindowIndex + i) % NUM_WINDOWS;
            Window window = ring[j];
            if (window.isValid()) {
                // expire if the window is too old
                if (window.startMs < nowMs - windowDurationMs * NUM_WINDOWS) {
                    window.expire();
                } else {
                    keyCount += window.counter.get(calculatedKey);
                    fromMs = Math.min(fromMs, window.startMs);
                }
            }
        }

        return new Stat(fromMs, nowMs, keyCount);
    }
}
