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

package com.linecorp.decaton.processor.runtime;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.runtime.OutOfOrderCommitControl.OffsetState;

public class OutOfOrderCommitControlTest {
    private static final int STATES_CAPACITY = 1000;

    private final TopicPartition topicPartition = new TopicPartition("topic", 0);
    private final OutOfOrderCommitControl partitionState =
            new OutOfOrderCommitControl(topicPartition, STATES_CAPACITY);

    @Test
    public void testInOrderOffsetCompletion() {
        DeferredCompletion comp1 = partitionState.reportFetchedOffset(1);
        DeferredCompletion comp2 = partitionState.reportFetchedOffset(2);
        DeferredCompletion comp3 = partitionState.reportFetchedOffset(3);

        comp1.complete();
        partitionState.updateHighWatermark();
        assertEquals(2, partitionState.pendingOffsetsCount());
        assertEquals(1, partitionState.commitReadyOffset());

        comp2.complete();
        partitionState.updateHighWatermark();
        assertEquals(1, partitionState.pendingOffsetsCount());
        assertEquals(2, partitionState.commitReadyOffset());

        comp3.complete();
        partitionState.updateHighWatermark();
        assertEquals(0, partitionState.pendingOffsetsCount());
        assertEquals(3, partitionState.commitReadyOffset());
    }

    @Test
    public void testOutOfOrderOffsetCompletion() {
        DeferredCompletion comp1 = partitionState.reportFetchedOffset(1);
        DeferredCompletion comp2 = partitionState.reportFetchedOffset(2);
        DeferredCompletion comp3 = partitionState.reportFetchedOffset(3);
        DeferredCompletion comp4 = partitionState.reportFetchedOffset(4);

        comp3.complete();
        partitionState.updateHighWatermark();
        assertEquals(4, partitionState.pendingOffsetsCount());
        assertEquals(-1, partitionState.commitReadyOffset());

        comp2.complete();
        partitionState.updateHighWatermark();
        assertEquals(4, partitionState.pendingOffsetsCount());
        assertEquals(-1, partitionState.commitReadyOffset());

        comp1.complete();
        partitionState.updateHighWatermark();
        assertEquals(1, partitionState.pendingOffsetsCount());
        assertEquals(3, partitionState.commitReadyOffset());

        comp4.complete();
        partitionState.updateHighWatermark();
        assertEquals(0, partitionState.pendingOffsetsCount());
        assertEquals(4, partitionState.commitReadyOffset());
    }

    @Test
    public void testDoubleCompletingSameOffset() {
        DeferredCompletion comp1 = partitionState.reportFetchedOffset(1);

        comp1.complete();
        assertEquals(-1, partitionState.commitReadyOffset());
        comp1.complete(); // nothing happens
        partitionState.updateHighWatermark();
        assertEquals(1, partitionState.commitReadyOffset());
        comp1.complete(); // nothing happens
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompletingTooHighOffset() {
        partitionState.reportFetchedOffset(1);
        partitionState.complete(new OffsetState(2, false)); // throws
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReportingTooLargeOffset() {
        partitionState.reportFetchedOffset(1); // now earliest=1
        for (int i = 0; i < STATES_CAPACITY; i++) {
            partitionState.reportFetchedOffset(1 + i); // throws
        }
    }

    @Test
    public void testDoubleCompletingSameOffsetCaseDuplicateInCommitted() {
        partitionState.reportFetchedOffset(1);
        DeferredCompletion comp2 = partitionState.reportFetchedOffset(2);

        comp2.complete(); // now committedOffsets contains 2
        partitionState.updateHighWatermark();
        assertEquals(-1, partitionState.commitReadyOffset());
        comp2.complete(); // commit again
        partitionState.updateHighWatermark();
        assertEquals(-1, partitionState.commitReadyOffset());
    }

    @Test
    public void testPendingRecordsCountWithGaps() {
        DeferredCompletion comp1 = partitionState.reportFetchedOffset(1);
        assertEquals(1, partitionState.pendingOffsetsCount());

        DeferredCompletion comp3 = partitionState.reportFetchedOffset(3);
        assertEquals(2, partitionState.pendingOffsetsCount());

        comp1.complete();
        comp3.complete();
        partitionState.updateHighWatermark();
        assertEquals(3, partitionState.commitReadyOffset());
        assertEquals(0, partitionState.pendingOffsetsCount());
    }

    @Test
    public void testPendingRecordsCountWithLargeGap() {
        DeferredCompletion comp1 = partitionState.reportFetchedOffset(1);
        assertEquals(1, partitionState.pendingOffsetsCount());

        long largeGapOffset = 1 + STATES_CAPACITY;
        DeferredCompletion compLarge = partitionState.reportFetchedOffset(largeGapOffset);
        assertEquals(2, partitionState.pendingOffsetsCount());

        comp1.complete();
        compLarge.complete();
        partitionState.updateHighWatermark();
        assertEquals(largeGapOffset, partitionState.commitReadyOffset());
        assertEquals(0, partitionState.pendingOffsetsCount());
    }
}
