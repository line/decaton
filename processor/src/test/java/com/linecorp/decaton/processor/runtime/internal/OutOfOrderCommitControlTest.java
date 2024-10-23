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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.time.Clock;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;

public class OutOfOrderCommitControlTest {
    private static final int STATES_CAPACITY = 1000;

    private final TopicPartition topicPartition = new TopicPartition("topic", 0);
    private final OutOfOrderCommitControl commitControl =
            new OutOfOrderCommitControl(topicPartition, STATES_CAPACITY, mock(OffsetStateReaper.class));

    @Test
    public void testInOrderOffsetCompletion() {
        OffsetState state1 = commitControl.reportFetchedOffset(1);
        OffsetState state2 = commitControl.reportFetchedOffset(2);
        OffsetState state3 = commitControl.reportFetchedOffset(3);

        state1.completion().complete();
        commitControl.updateHighWatermark();
        assertEquals(2, commitControl.pendingOffsetsCount());
        assertEquals(2, commitControl.commitReadyOffset().offset());

        state2.completion().complete();
        commitControl.updateHighWatermark();
        assertEquals(1, commitControl.pendingOffsetsCount());
        assertEquals(3, commitControl.commitReadyOffset().offset());

        state3.completion().complete();
        commitControl.updateHighWatermark();
        assertEquals(0, commitControl.pendingOffsetsCount());
        assertEquals(4, commitControl.commitReadyOffset().offset());
    }

    @Test
    public void testOutOfOrderOffsetCompletion() {
        OffsetState state1 = commitControl.reportFetchedOffset(1);
        OffsetState state2 = commitControl.reportFetchedOffset(2);
        OffsetState state3 = commitControl.reportFetchedOffset(3);
        OffsetState state4 = commitControl.reportFetchedOffset(4);

        state3.completion().complete();
        commitControl.updateHighWatermark();
        assertEquals(4, commitControl.pendingOffsetsCount());
        assertNull(commitControl.commitReadyOffset());

        state2.completion().complete();
        commitControl.updateHighWatermark();
        assertEquals(4, commitControl.pendingOffsetsCount());
        assertNull(commitControl.commitReadyOffset());

        state1.completion().complete();
        commitControl.updateHighWatermark();
        assertEquals(1, commitControl.pendingOffsetsCount());
        assertEquals(4, commitControl.commitReadyOffset().offset());

        state4.completion().complete();
        commitControl.updateHighWatermark();
        assertEquals(0, commitControl.pendingOffsetsCount());
        assertEquals(5, commitControl.commitReadyOffset().offset());
    }

    @Test
    public void testDoubleCompletingSameOffset() {
        OffsetState state1 = commitControl.reportFetchedOffset(1);

        state1.completion().complete();
        assertNull(commitControl.commitReadyOffset());
        state1.completion().complete(); // nothing happens
        commitControl.updateHighWatermark();
        assertEquals(2, commitControl.commitReadyOffset().offset());
        state1.completion().complete(); // nothing happens
    }

    @Test
    public void testReportingTooLargeOffset() {
        commitControl.reportFetchedOffset(1); // now earliest=1
        for (int i = 0; i < STATES_CAPACITY - 1; i++) {
            commitControl.reportFetchedOffset(1 + i);
        }
        assertThrows(IllegalArgumentException.class, () -> commitControl.reportFetchedOffset(STATES_CAPACITY));
    }

    @Test
    public void testDoubleCompletingSameOffsetCaseDuplicateInCommitted() {
        commitControl.reportFetchedOffset(1);
        OffsetState state2 = commitControl.reportFetchedOffset(2);

        state2.completion().complete(); // now committedOffsets contains 2
        commitControl.updateHighWatermark();
        assertNull(commitControl.commitReadyOffset());
        state2.completion().complete(); // commit again
        commitControl.updateHighWatermark();
        assertNull(commitControl.commitReadyOffset());
    }

    @Test
    public void testPendingRecordsCountWithGaps() {
        OffsetState state1 = commitControl.reportFetchedOffset(1);
        assertEquals(1, commitControl.pendingOffsetsCount());

        OffsetState state3 = commitControl.reportFetchedOffset(3);
        assertEquals(2, commitControl.pendingOffsetsCount());

        state1.completion().complete();
        state3.completion().complete();
        commitControl.updateHighWatermark();
        assertEquals(4, commitControl.commitReadyOffset().offset());
        assertEquals(0, commitControl.pendingOffsetsCount());
    }

    @Test
    public void testPendingRecordsCountWithLargeGap() {
        OffsetState state1 = commitControl.reportFetchedOffset(1);
        assertEquals(1, commitControl.pendingOffsetsCount());

        long largeGapOffset = 1 + STATES_CAPACITY;
        OffsetState stateLarge = commitControl.reportFetchedOffset(largeGapOffset);
        assertEquals(2, commitControl.pendingOffsetsCount());

        state1.completion().complete();
        stateLarge.completion().complete();
        commitControl.updateHighWatermark();
        assertEquals(largeGapOffset + 1, commitControl.commitReadyOffset().offset());
        assertEquals(0, commitControl.pendingOffsetsCount());
    }

    @Test
    @Timeout(5)
    public void testTimeoutOffsetReaping() {
        Clock clock = mock(Clock.class);
        doReturn(10L).when(clock).millis();
        OffsetStateReaper reaper = new OffsetStateReaper(
                Property.ofStatic(ProcessorProperties.CONFIG_DEFERRED_COMPLETE_TIMEOUT_MS, 10L),
                Metrics.withTags("subscription", "subsc",
                                 "topic", "topic",
                                 "partition", "1")
                        .new CommitControlMetrics(),
                clock);
        OutOfOrderCommitControl ooocc = new OutOfOrderCommitControl(topicPartition, 10, reaper);

        OffsetState state1 = ooocc.reportFetchedOffset(1);
        state1.setTimeout(20);
        OffsetState state2 = ooocc.reportFetchedOffset(2);

        // 1 is blocking watermark to progress
        state2.completion().complete();
        ooocc.updateHighWatermark();
        assertNull(ooocc.commitReadyOffset());

        doReturn(20L).when(clock).millis();
        // offset reaping performed but does not proceed watermark yet
        ooocc.updateHighWatermark();
        state1.completion().asFuture().toCompletableFuture().join();
        assertNull(ooocc.commitReadyOffset());
        // offset should progress as offset 1 has reaped in previous call
        ooocc.updateHighWatermark();
        assertEquals(3, ooocc.commitReadyOffset().offset());
    }

    @Test
    void perOffsetCompleteTest() {
        OffsetState state1 = commitControl.reportFetchedOffset(101);
        OffsetState state2 = commitControl.reportFetchedOffset(102);
        OffsetState state3 = commitControl.reportFetchedOffset(103);

        state1.completion().complete();
        state3.completion().complete();

        commitControl.updateHighWatermark();
        OffsetAndMetadata om = commitControl.commitReadyOffset();
        System.err.println("ooocc.complex = " + commitControl.complex);

        assertEquals(om.offset(), 102L);
        OffsetStorageComplex complex = OutOfOrderCommitControl.complexFromMeta(om.metadata());
        System.err.println("meta = " + om.metadata());
        assertEquals(2, complex.size());
        assertFalse(complex.isComplete(102));
        assertTrue(complex.isComplete(103));

        state2.completion().complete();
        commitControl.updateHighWatermark();
        om = commitControl.commitReadyOffset();

        assertEquals(om.offset(), 104L);
        complex = OutOfOrderCommitControl.complexFromMeta(om.metadata());
        assertEquals(0, complex.size());
    }
}
