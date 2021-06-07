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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.time.Clock;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;

public class OutOfOrderCommitControlTest {
    private static final int STATES_CAPACITY = 1000;

    private final TopicPartition topicPartition = new TopicPartition("topic", 0);
    private final OutOfOrderCommitControl partitionState =
            new OutOfOrderCommitControl(topicPartition, STATES_CAPACITY, mock(OffsetStateReaper.class));

    @Test
    public void testInOrderOffsetCompletion() {
        OffsetState state1 = partitionState.reportFetchedOffset(1);
        OffsetState state2 = partitionState.reportFetchedOffset(2);
        OffsetState state3 = partitionState.reportFetchedOffset(3);

        state1.completion().complete();
        partitionState.updateHighWatermark();
        assertEquals(2, partitionState.pendingOffsetsCount());
        assertEquals(1, partitionState.commitReadyOffset());

        state2.completion().complete();
        partitionState.updateHighWatermark();
        assertEquals(1, partitionState.pendingOffsetsCount());
        assertEquals(2, partitionState.commitReadyOffset());

        state3.completion().complete();
        partitionState.updateHighWatermark();
        assertEquals(0, partitionState.pendingOffsetsCount());
        assertEquals(3, partitionState.commitReadyOffset());
    }

    @Test
    public void testOutOfOrderOffsetCompletion() {
        OffsetState state1 = partitionState.reportFetchedOffset(1);
        OffsetState state2 = partitionState.reportFetchedOffset(2);
        OffsetState state3 = partitionState.reportFetchedOffset(3);
        OffsetState state4 = partitionState.reportFetchedOffset(4);

        state3.completion().complete();
        partitionState.updateHighWatermark();
        assertEquals(4, partitionState.pendingOffsetsCount());
        assertEquals(-1, partitionState.commitReadyOffset());

        state2.completion().complete();
        partitionState.updateHighWatermark();
        assertEquals(4, partitionState.pendingOffsetsCount());
        assertEquals(-1, partitionState.commitReadyOffset());

        state1.completion().complete();
        partitionState.updateHighWatermark();
        assertEquals(1, partitionState.pendingOffsetsCount());
        assertEquals(3, partitionState.commitReadyOffset());

        state4.completion().complete();
        partitionState.updateHighWatermark();
        assertEquals(0, partitionState.pendingOffsetsCount());
        assertEquals(4, partitionState.commitReadyOffset());
    }

    @Test
    public void testDoubleCompletingSameOffset() {
        OffsetState state1 = partitionState.reportFetchedOffset(1);

        state1.completion().complete();
        assertEquals(-1, partitionState.commitReadyOffset());
        state1.completion().complete(); // nothing happens
        partitionState.updateHighWatermark();
        assertEquals(1, partitionState.commitReadyOffset());
        state1.completion().complete(); // nothing happens
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
        OffsetState state2 = partitionState.reportFetchedOffset(2);

        state2.completion().complete(); // now committedOffsets contains 2
        partitionState.updateHighWatermark();
        assertEquals(-1, partitionState.commitReadyOffset());
        state2.completion().complete(); // commit again
        partitionState.updateHighWatermark();
        assertEquals(-1, partitionState.commitReadyOffset());
    }

    @Test
    public void testPendingRecordsCountWithGaps() {
        OffsetState state1 = partitionState.reportFetchedOffset(1);
        assertEquals(1, partitionState.pendingOffsetsCount());

        OffsetState state3 = partitionState.reportFetchedOffset(3);
        assertEquals(2, partitionState.pendingOffsetsCount());

        state1.completion().complete();
        state3.completion().complete();
        partitionState.updateHighWatermark();
        assertEquals(3, partitionState.commitReadyOffset());
        assertEquals(0, partitionState.pendingOffsetsCount());
    }

    @Test
    public void testPendingRecordsCountWithLargeGap() {
        OffsetState state1 = partitionState.reportFetchedOffset(1);
        assertEquals(1, partitionState.pendingOffsetsCount());

        long largeGapOffset = 1 + STATES_CAPACITY;
        OffsetState stateLarge = partitionState.reportFetchedOffset(largeGapOffset);
        assertEquals(2, partitionState.pendingOffsetsCount());

        state1.completion().complete();
        stateLarge.completion().complete();
        partitionState.updateHighWatermark();
        assertEquals(largeGapOffset, partitionState.commitReadyOffset());
        assertEquals(0, partitionState.pendingOffsetsCount());
    }

    @Test(timeout = 5000)
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
        assertEquals(-1, ooocc.commitReadyOffset());

        doReturn(20L).when(clock).millis();
        // offset reaping performed but does not proceed watermark yet
        ooocc.updateHighWatermark();
        state1.completion().asFuture().toCompletableFuture().join();
        assertEquals(-1, ooocc.commitReadyOffset());
        // offset should progress as offset 1 has reaped in previous call
        ooocc.updateHighWatermark();
        assertEquals(2, ooocc.commitReadyOffset());
    }
}
