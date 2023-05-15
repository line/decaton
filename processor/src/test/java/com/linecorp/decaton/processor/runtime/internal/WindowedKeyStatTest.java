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

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.linecorp.decaton.processor.internal.HashableByteArray;
import com.linecorp.decaton.processor.runtime.internal.WindowedKeyStat.Stat;

public class WindowedKeyStatTest {
    // HashMap-based exact key counter for ease of testing
    static class MockKeyCounter extends KeyCounter {
        private final HashMap<HashableByteArray, Long> map = new HashMap<>();

        MockKeyCounter() {
            super(new Random(), 1, 1);
        }

        @Override
        public Key createKey(byte[] rawKey) {
            return new Key(rawKey, new int[0]);
        }

        @Override
        public long incrementAndGet(Key key, long amount) {
            HashableByteArray a = new HashableByteArray(key.rawKey());
            long newValue = map.getOrDefault(a, 0L) + amount;
            map.put(a, newValue);
            return newValue;
        }

        @Override
        public long get(Key key) {
            return map.getOrDefault(new HashableByteArray(key.rawKey()), 0L);
        }

        @Override
        public void reset() {
            map.clear();
        }
    }

    private WindowedKeyStat windowedKeyStat;

    @Before
    public void setUp() {
        long windowMs = 100L;
        windowedKeyStat = new WindowedKeyStat(Duration.ofMillis(windowMs), MockKeyCounter::new);
    }

    @Test
    public void testRecordAndGet() {
        long t0 = System.currentTimeMillis();

        assertEquals(new Stat(t0, t0, 1), windowedKeyStat.recordAndGet(t0, key("a")));
        assertEquals(new Stat(t0, t0 + 99, 2), windowedKeyStat.recordAndGet(t0 + 99, key("a")));
        assertEquals(new Stat(t0, t0 + 100, 3), windowedKeyStat.recordAndGet(t0 + 100, key("a")));
        // different key
        assertEquals(new Stat(t0, t0 + 101, 1), windowedKeyStat.recordAndGet(t0 + 101, key("b")));
        // first window (< t0 + 100) is rotated
        assertEquals(new Stat(t0 + 100, t0 + 200, 2), windowedKeyStat.recordAndGet(t0 + 200, key("b")));
    }

    @Test
    public void testExpire() {
        long t0 = System.currentTimeMillis();

        windowedKeyStat.recordAndGet(t0, key("a"));
        windowedKeyStat.recordAndGet(t0 + 99, key("a"));
        windowedKeyStat.recordAndGet(t0 + 100, key("a"));

        // all previous windows are expired so new window is rolled
        assertEquals(new Stat(t0 + 301, t0 + 301, 1),
                     windowedKeyStat.recordAndGet(t0 + 301, key("a")));

        windowedKeyStat.recordAndGet(t0 + 400, key("a"));

        assertEquals(new Stat(t0 + 301, t0 + 402, 1),
                     windowedKeyStat.recordAndGet(t0 + 402, key("b")));
    }

    private static byte[] key(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }
}
