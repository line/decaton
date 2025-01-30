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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.NoSuchElementException;

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

    @Test
    void size() {
        OffsetStorageComplex complex = new OffsetStorageComplex(3);
        assertEquals(0, complex.size());
        // By allocating one offset, it increments size.
        int index = complex.allocNextIndex(10);
        assertEquals(1, complex.size());
        // Setting the allocated index doesn't impact size
        complex.setIndex(index, false, null);
        // Another addition
        complex.allocNextIndex(11);
        assertEquals(2, complex.size());
        // Polling the first offset decrements size.
        complex.pollFirst();
        assertEquals(1, complex.size());
        // Alloc a few more offsets, so the ring-buffer go over one cycle at least.
        complex.allocNextIndex(20);
        complex.allocNextIndex(21);
        assertEquals(3, complex.size());
    }

    @Test
    void firstOffset() {
        OffsetStorageComplex complex = new OffsetStorageComplex(3);
        // Without any entry it is -1
        assertEquals(-1, complex.firstOffset());
        // Add first offset, it should return as-is
        complex.allocNextIndex(10);
        assertEquals(10, complex.firstOffset());
        // Adding the second offset, should still return the first.
        complex.allocNextIndex(11);
        assertEquals(10, complex.firstOffset());
        // Popping the first offset out now the first offset should become the second added.
        complex.pollFirst();
        assertEquals(11, complex.firstOffset());
        // Add a few more offsets with gaps, also making the ring-buffer to go over one cycle.
        complex.allocNextIndex(20);
        complex.allocNextIndex(30);
        complex.pollFirst();
        assertEquals(20, complex.firstOffset());
        complex.pollFirst();
        assertEquals(30, complex.firstOffset());
        // Finally there are no more offsets in storage, should come back to return -1 again.
        complex.pollFirst();
        assertEquals(-1, complex.firstOffset());
    }

    @Test
    void pollFirst() {
        OffsetStorageComplex complex = new OffsetStorageComplex(1);
        assertThrows(NoSuchElementException.class, complex::pollFirst);
        // Add an entry, and remove it then it should become back to empty.
        complex.allocNextIndex(10);
        complex.pollFirst();
        assertThrows(NoSuchElementException.class, complex::pollFirst);
    }

    @Test
    void allocNextIndex() {
        OffsetStorageComplex complex = new OffsetStorageComplex(2);
        complex.allocNextIndex(10); // OK
        complex.allocNextIndex(11); // OK
        assertThrows(IllegalStateException.class, () -> complex.allocNextIndex(20));
    }

    @Test
    void firstState() {
        OffsetStorageComplex complex = new OffsetStorageComplex(3);
        // Without any entry it is null
        assertNull(complex.firstState());
        // Try adding the first offset, now the first is it.
        OffsetState s10 = new OffsetState(10);
        complex.setIndex(complex.allocNextIndex(10), false, s10);
        assertSame(s10, complex.firstState());
        // Adding the second offset, should still return the first.
        OffsetState s11 = new OffsetState(11);
        complex.setIndex(complex.allocNextIndex(11), false, s11);
        assertSame(s10, complex.firstState());
        // Popping the first offset out now the first offset should become the second added.
        complex.pollFirst();
        assertSame(s11, complex.firstState());
        // Add a few more offsets with gaps, also making the ring-buffer to go over one cycle.
        OffsetState s20 = new OffsetState(20);
        complex.setIndex(complex.allocNextIndex(20), false, s20);
        OffsetState s30 = new OffsetState(30);
        complex.setIndex(complex.allocNextIndex(30), false, s30);
        complex.pollFirst();
        assertSame(s20, complex.firstState());
        complex.pollFirst();
        assertSame(s30, complex.firstState());
        // Finally there are no more offsets in storage, should come back to return -1 again.
        complex.pollFirst();
        assertNull(complex.firstState());
    }

    @Test
    void isComplete() {
        OffsetStorageComplex complex = new OffsetStorageComplex(3);
        // Despite no offsets has been added, it returns false for whatever offset that it doesn't
        // recognize.
        assertFalse(complex.isComplete(1));
        assertFalse(complex.isComplete(100));
        // Add first offset
        int i10 = complex.allocNextIndex(10);
        complex.setIndex(i10, false, new OffsetState(10));
        assertFalse(complex.isComplete(10));
        // Now complete the first
        complex.complete(i10);
        assertTrue(complex.isComplete(10));
        // By popping out the offset, it turns back to return false
        complex.pollFirst();
        assertFalse(complex.isComplete(10));
        // Add another offset with completion true
        int i11 = complex.allocNextIndex(11);
        complex.setIndex(i11, true, new OffsetState(11));
        assertTrue(complex.isComplete(11));
        // Add a few more offsets to let ring-buffer go over one cycle.
        int i20 = complex.allocNextIndex(20);
        complex.setIndex(i20, false, new OffsetState(20));
        int i30 = complex.allocNextIndex(30);
        complex.setIndex(i30, false, new OffsetState(30));
        complex.complete(i30);
        // Now the state should be 11=c, 20=n, 30=c
        assertTrue(complex.isComplete(11));
        assertFalse(complex.isComplete(20));
        assertTrue(complex.isComplete(30));
        // Popping them all should make them all false
        complex.pollFirst();
        assertFalse(complex.isComplete(11));
        complex.pollFirst();
        assertFalse(complex.isComplete(20));
        complex.pollFirst();
        assertFalse(complex.isComplete(30));
    }

    @Test
    void fromProto() {
        final OffsetStorageComplex recov;
        {
            OffsetStorageComplex complex = new OffsetStorageComplex(3);
            int i10 = complex.allocNextIndex(10);
            complex.setIndex(i10, false, new OffsetState(10));
            int i11 = complex.allocNextIndex(11);
            complex.setIndex(i11, false, new OffsetState(11));
            int i20 = complex.allocNextIndex(20);
            complex.setIndex(i20, true, new OffsetState(20));

            recov = OffsetStorageComplex.fromProto(complex.toProto());
        }
        // Recovered complex has no offsets, hence no first state as well
        assertEquals(0, recov.size());
        assertEquals(-1, recov.firstOffset());
        assertNull(recov.firstState());

        // The recovered complex expects exactly same sequence of offsets to receive after recovery.
        assertThrows(OffsetRegressionException.class, () -> recov.allocNextIndex(20));
        int ri10 = recov.allocNextIndex(10);
        assertEquals(1, recov.size()); // Size should be incremented only after allocation
        assertFalse(recov.isComplete(10));
        recov.setIndex(ri10, false, new OffsetState(10));
        recov.complete(ri10);
        assertTrue(recov.isComplete(10));
        int ri11 = recov.allocNextIndex(11);
        assertFalse(recov.isComplete(11));
        recov.setIndex(ri11, false, new OffsetState(11));
        recov.complete(ri11);
        assertTrue(recov.isComplete(11));
        recov.allocNextIndex(20);
        // No setIndex for offset=20 because it's complete already.
        assertTrue(recov.isComplete(20));
        // firstOffset always returns the first offset, while firstState might return null
        assertEquals(10, recov.firstOffset());
        assertEquals(10, recov.firstState().offset());
        recov.pollFirst();
        assertEquals(11, recov.firstOffset());
        assertEquals(11, recov.firstState().offset());
        recov.pollFirst();
        assertEquals(20, recov.firstOffset());
        assertNull(recov.firstState());
        recov.pollFirst();

        // After recovery and added all sequence previously seen, it could be used continually
        int ri30 = recov.allocNextIndex(30);
        recov.setIndex(ri30, false, new OffsetState(30));
        recov.complete(ri30);
        assertTrue(recov.isComplete(30));
        assertEquals(30, recov.firstOffset());
        assertEquals(30, recov.firstState().offset());
    }
}
