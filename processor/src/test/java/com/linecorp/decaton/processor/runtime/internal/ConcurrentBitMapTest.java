/*
 * Copyright 2024 LY Corporation
 *
 * LY Corporation licenses this file to you under the Apache License,
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.mockito.internal.util.collections.Sets;

class ConcurrentBitMapTest {
    private static void setGetCheck(ConcurrentBitMap map, Set<Integer> trueIndicies) {
        for (int index : trueIndicies) {
            map.set(index, true);
        }
        for (int i = 0; i < map.size(); i++) {
            assertEquals(trueIndicies.contains(i), map.get(i));
        }
        for (int index : trueIndicies) {
            map.set(index, false);
        }
        for (int i = 0; i < map.size(); i++) {
            assertFalse(map.get(i));
        }
    }

    @Test
    void testSmall() {
        ConcurrentBitMap map = new ConcurrentBitMap(10);
        assertThrows(IndexOutOfBoundsException.class, () -> map.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> map.get(10));
        setGetCheck(map, Sets.newSet(0, 3, 8));
    }

    @Test
    void test64() {
        ConcurrentBitMap map = new ConcurrentBitMap(Long.SIZE);
        assertThrows(IndexOutOfBoundsException.class, () -> map.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> map.get(Long.SIZE));
        setGetCheck(map, Sets.newSet(0, 10, Long.SIZE - 1));
    }

    @Test
    void testLarge() {
        ConcurrentBitMap map = new ConcurrentBitMap(Long.SIZE + 10);
        assertThrows(IndexOutOfBoundsException.class, () -> map.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> map.get(Long.SIZE + 10));
        setGetCheck(map, Sets.newSet(0, 10, Long.SIZE, Long.SIZE + 3, Long.SIZE + 8));
    }
}
