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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.linecorp.decaton.processor.runtime.internal.OffsetStorageComplex.OffsetIndex;

class OffsetStorageComplexTest {

    @Test
    void testOffsetIndex() {
        OffsetIndex index = new OffsetIndex();

        index.addOffset(10); // index 0
        index.addOffset(11); // index 1
        index.addOffset(12); // index 2
        index.addOffset(15); // index 3
        index.addOffset(16); // index 4
        index.addOffset(17); // index 5

        assertEquals(0, index.firstIndex());
        assertEquals(0, index.indexOf(10));
        assertEquals(2, index.indexOf(12));
        assertEquals(3, index.indexOf(15));
        assertEquals(5, index.indexOf(17));
        assertEquals(-1, index.indexOf(9));
        assertEquals(-1, index.indexOf(18));

        assertEquals(0, index.pollFirst());
        assertEquals(1, index.firstIndex());
        assertEquals(1, index.indexOf(11));
        assertEquals(2, index.indexOf(12));
        assertEquals(3, index.indexOf(15));
        assertEquals(5, index.indexOf(17));
        assertEquals(-1, index.indexOf(10));
        assertEquals(-1, index.indexOf(18));

        index.pollFirst(); // 11 out
        index.pollFirst(); // 12 out
        assertEquals(3, index.pollFirst());
        assertEquals(4, index.firstIndex());
        assertEquals(4, index.indexOf(16));
        assertEquals(5, index.indexOf(17));
        assertEquals(-1, index.indexOf(15));
        assertEquals(-1, index.indexOf(18));

        index.pollFirst(); // 14 out
        index.pollFirst(); // 15 out

        index.addOffset(18);
        assertEquals(6, index.firstIndex());
        assertEquals(6, index.indexOf(18));
        assertEquals(-1, index.indexOf(17));
        assertEquals(-1, index.indexOf(19));
    }

    private static int addOffset(OffsetStorageComplex complex, long offset) {
        return complex.addOffset(offset, false, new OffsetState(offset));
    }

    @Test
    void test() {
        OffsetStorageComplex complex = new OffsetStorageComplex(10);

        addOffset(complex, 10);
        addOffset(complex, 11);
        int ri12 = addOffset(complex, 12);
        int ri15 = addOffset(complex, 15);
        addOffset(complex, 16);
        addOffset(complex, 17);

        assertEquals(6, complex.size());
        assertFalse(complex.isComplete(10));
        assertFalse(complex.isComplete(17));

        complex.complete(ri12);
        complex.complete(ri15);
        assertTrue(complex.isComplete(12));
        assertTrue(complex.isComplete(15));
        assertFalse(complex.isComplete(10));
        assertFalse(complex.isComplete(17));
//
//        addOffset(complex, 10);
    }
}
