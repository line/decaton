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

import java.util.Arrays;
import java.util.Random;

import net.openhft.hashing.LongHashFunction;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

/**
 * Counter to record task counts per-key.
 * <p>
 * Since Decaton can make no assumption in key cardinality nor key length, we cannot simply use HashMap to record
 * counts, which potentially consumes unbounded amount of memory.
 * <p>
 * To address this problem, we use probabilistic algorithm called Count-Min sketch [1],
 * which can count arbitrary keys in constant memory space, by allowing some errors.
 * <p>
 * [1] Graham Cormode and S. Muthukrishnan (2005). "An Improved Data Stream Summary: The Count-Min Sketch and its Applications"
 */
class KeyCounter {
    private static final int MAX_WIDTH = 1 << 20;
    private final long[][] sketch;
    private final LongHashFunction[] hashFunctions;
    private final int bitMask;

    /**
     * Holds hash values (i.e. count-min column indices) for a key
     * to reduce expensive hash calculations when you call
     * counter methods multiple times for a key.
     */
    @RequiredArgsConstructor
    static class Key {
        @Getter
        @Accessors(fluent = true)
        private final byte[] rawKey;

        private final int[] hashes;
    }

    /**
     * Instantiate the counter with the specified parameters to control the estimation quality.
     * Count-Min sketch estimates key count in following error characteristic:
     * <pre>
     *     estimation(x) ≤ actual(x) + εN with probability 1 - δ
     * </pre>
     * Where x is any key, N is the total key occurrences ever recorded, ε is the errorFactor and δ is the error probability.
     * Let's see this by example.
     * Say we set ε = 0.0001, δ = 0.0001.
     * After we record 10000000 item occurrences, for any key, the estimated count is in the range
     * [actual(x), actual(x) + 1000] with 99.99% probability.
     * <p>
     * Please refer original paper mentioned in {@link KeyCounter} class javadoc for the mathematical analysis.
     * @param random random to generate hash function seed to get independent hash family
     * @param errorFactor `ε` parameter of the Count-Min sketch
     * @param errorProbability `δ` parameter of the Count-Min sketch
     */
    KeyCounter(Random random, double errorFactor, double errorProbability) {
        int d = Math.max(1, (int) Math.ceil(Math.log(1 / errorProbability)));
        int w0 = (int) Math.ceil(Math.E / errorFactor);

        /*
         * We need `d` hash functions well distributed over columns, i.e.
         * each hash function's range is [0, width) (= Math.E / errorFactor).
         *
         * To get [0, width) hash value from long hash function, we use bit-masking by
         * aligning the width to power of 2 (instead of simply taking modulo (abs(hash_value) % width)) to avoid modulo-bias.
         */
        int w = 1;
        while (w < w0) {
            w <<= 1;
            if (w >= MAX_WIDTH) {
                break;
            }
        }
        bitMask = w - 1;

        sketch = new long[d][w];
        hashFunctions = new LongHashFunction[d];
        for (int i = 0; i < hashFunctions.length; i++) {
            hashFunctions[i] = LongHashFunction.xx3(random.nextLong());
        }
    }

    /**
     * Create calculated {@link Key} corresponding to the raw byte-array key.
     */
    public Key createKey(byte[] rawKey) {
        int[] hashes = new int[hashFunctions.length];
        for (int i = 0; i < hashFunctions.length; i++) {
            hashes[i] = (int) (hashFunctions[i].hashBytes(rawKey) & bitMask);
        }
        return new Key(rawKey, hashes);
    }

    /**
     * Increment the count for the key with specified amount.
     * The algorithm is based on minimal increment (aka conservative update) [2], which is a heuristic known to
     * improve the estimation accuracy.
     * <p>
     * [2] Younes Ben Mazziane; Sara Alouf; and Giovanni Neglia (2022). "A Formal Analysis of the Count-Min Sketch with Conservative Updates"
     */
    public long incrementAndGet(Key key, long amount) {
        long currentMin = get(key);
        long newMin = Long.MAX_VALUE;
        for (int i = 0; i < sketch.length; i++) {
            int col = key.hashes[i];
            sketch[i][col] = Math.max(sketch[i][col], currentMin + amount);
            newMin = Math.min(newMin, sketch[i][col]);
        }
        return newMin;
    }

    /**
     * Get the estimation for the key
     */
    public long get(Key key) {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < sketch.length; i++) {
            int col = key.hashes[i];
            min = Math.min(min, sketch[i][col]);
        }
        return min;
    }

    /**
     * Reset this counter to the initial state with forgetting all previously recorded values.
     */
    public void reset() {
        for (long[] cols : sketch) {
            Arrays.fill(cols, 0);
        }
    }
}
