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
import com.linecorp.decaton.protocol.Decaton.OffsetStorageComplexProto;

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
        int index = complex.allocNextIndex(offset);
        complex.setIndex(index, false, new OffsetState(offset));
        return index;
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

    @Test
    void testRecoveryState() {
        OffsetStorageComplex origComplex = new OffsetStorageComplex(10);

        addOffset(origComplex, 10);
        addOffset(origComplex, 11);
        int ri12 = addOffset(origComplex, 12);
        int ri15 = addOffset(origComplex, 15);
        addOffset(origComplex, 16);
        addOffset(origComplex, 17);
        origComplex.complete(ri12);
        origComplex.complete(ri15);

        OffsetStorageComplexProto proto = origComplex.toProto();
        OffsetStorageComplex complex = OffsetStorageComplex.fromProto(proto);

        assertEquals(0, complex.size());
        assertFalse(complex.isComplete(10));
        assertFalse(complex.isComplete(11));
        assertTrue(complex.isComplete(12));
        assertTrue(complex.isComplete(15));
        assertFalse(complex.isComplete(16));
        assertFalse(complex.isComplete(17));

        assertThrows(OffsetRegressionException.class, () -> complex.allocNextIndex(9));
        complex.complete(addOffset(complex, 10));
        complex.complete(addOffset(complex, 11));
        complex.allocNextIndex(12);
        complex.allocNextIndex(15);
        complex.complete(addOffset(complex, 16));
        complex.complete(addOffset(complex, 17));

        assertEquals(10, complex.firstOffset());
        assertTrue(complex.isComplete(10));
        assertTrue(complex.isComplete(11));
        assertTrue(complex.isComplete(12));
        assertTrue(complex.isComplete(15));
        assertTrue(complex.isComplete(16));
        assertTrue(complex.isComplete(17));
    }
}
