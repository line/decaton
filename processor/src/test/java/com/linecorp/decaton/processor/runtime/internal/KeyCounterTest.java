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
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.junit.Test;

import com.linecorp.decaton.processor.runtime.internal.KeyCounter.Key;

public class KeyCounterTest {
    @Test
    public void testIncrementAndGet() {
        // we use fixed seed to get consistent test result
        KeyCounter counter = new KeyCounter(new Random(0L), 0.001, 0.001);

        int numKeys = 100;
        Key[] keys = new Key[numKeys];
        for (int i = 0; i < numKeys; i++) {
            keys[i] = counter.createKey(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
            long cnt = counter.incrementAndGet(keys[i], i + 42);
            assertEquals("count for key: " + i, i + 42, cnt);
        }

        for (int i = 0; i < numKeys; i++) {
            assertEquals("count for key: " + i, i + 42, counter.get(keys[i]));
        }
    }

    @Test
    public void testReset() {
        KeyCounter counter = new KeyCounter(new Random(0L), 0.001, 0.001);

        int numKeys = 100;
        Key[] keys = new Key[numKeys];
        for (int i = 0; i < numKeys; i++) {
            keys[i] = counter.createKey(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
            counter.incrementAndGet(keys[i], i + 42);
        }

        counter.reset();
        for (int i = 0; i < numKeys; i++) {
            assertEquals("count for key: " + i, 0, counter.get(keys[i]));
        }
    }

    /*
     * Sanity check of the estimation accuracy with fixed data set.
     * Note that, since Count-Min is a probabilistic data algorithm, the error may
     * be larger than εN (asserted in this test) depending on a data set in real-world.
     */
    @Test
    public void testAccuracy() {
        int numKeys = 1000000;
        long totalCount = 0;
        double errorFactor = 0.00001;
        double errorProbability = 0.001;

        KeyCounter counter = new KeyCounter(new Random(0L), errorFactor, errorProbability);

        Random rnd = new Random(42L);
        long[] actual = new long[numKeys];
        for (int i = 0; i < numKeys; i++) {
            long amount;
            if (i < 100) {
                // simulate 100 keys are "burst"
                amount = rnd.nextInt(100) + 10000;
            } else {
                amount = rnd.nextInt(100);
            }

            actual[i] = amount;
            totalCount += amount;
            counter.incrementAndGet(
                    counter.createKey(String.valueOf(i).getBytes(StandardCharsets.UTF_8)),
                    amount);
        }

        String maxErrorKey = null;
        long maxError = 0;
        for (int i = 0; i < numKeys; i++) {
            long estimation = counter.get(counter.createKey(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
            long a = actual[i];

            long error = Math.abs(estimation - a);
            if (error > maxError) {
                maxError = error;
                maxErrorKey = String.valueOf(i);
            }
        }

        double epsilonN = totalCount * errorFactor;
        assertTrue("estimation error for key: " + maxErrorKey, maxError <= epsilonN);
    }
}
