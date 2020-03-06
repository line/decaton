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

import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents consumption processing progress of records consumed from a single partition.
 * This class manages below states:
 * - The list of record offsets which are currently being processed(either sequentially or concurrently).
 * - The list of record offsets which are completed processing and marked ready to be committed.
 * - The maximum offset of records which tells high watermark of records which has been processed and completed.
 */
public class OutOfOrderCommitControlV1 {
    private static final Logger logger = LoggerFactory.getLogger(OutOfOrderCommitControlV1.class);

    private final Object offsetsHeadEntryLock = new Object();

    private final TopicPartition topicPartition;
    private final Deque<Long> onBoardOffsets;
    private final PriorityBlockingQueue<Long> committedOffsets;
    private final AtomicLong committedHighWatermark;

    public OutOfOrderCommitControlV1(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
        onBoardOffsets = new LinkedBlockingDeque<>();
        committedOffsets = new PriorityBlockingQueue<>();
        committedHighWatermark = new AtomicLong();
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public void reportFetchedOffset(long offset) {
        final Long lastOffset = onBoardOffsets.peekLast();
        if (lastOffset != null) {
            if (offset < lastOffset) {
                throw new IllegalArgumentException(
                        "offset regression: offset=" + offset + ", lastOffset=" + lastOffset);
            }
        }
        onBoardOffsets.addLast(offset);
    }

    // visible for testing
    boolean isValidOffsetToComplete(long offset, Long firstOnBoardOffset) {
        return firstOnBoardOffset != null && firstOnBoardOffset <= offset;
    }

    // visible for testing
    // this method is made for hooking in unit test to validate timing issue which can happen under concurrent
    // access.
    Long peekFirstOnBoardOffset() {
        return onBoardOffsets.peekFirst();
    }

    public void complete(long offset) {
        synchronized (offsetsHeadEntryLock) {
            if (!isValidOffsetToComplete(offset, onBoardOffsets.peekFirst()) ||
                committedOffsets.contains(offset)) {
                // This should happens only when processor misused DeferredCompletion
                throw new IllegalArgumentException(
                        "offset " + offset + " of partition " + topicPartition + " completed already");
            }
            committedOffsets.add(offset);
        }

        if (logger.isTraceEnabled()) {
            logger.trace("offset complete({}), onBoard = {}, committed = {}",
                         offset, onBoardOffsets, committedOffsets);
        } else if (logger.isDebugEnabled()) {
            final Long peekFirst = onBoardOffsets.peekFirst();
            final Long peekLast = onBoardOffsets.peekLast();
            final long onBoardApproximateSize = peekFirst == null || peekLast == null
                                                ? onBoardOffsets.size() : peekLast - peekFirst + 1;
            logger.debug("offset complete({}) onBoard = [{}:{},{}], committed = [{},{}]", offset,
                         peekFirst, peekLast, onBoardApproximateSize,
                         committedOffsets.peek(), committedOffsets.size());
        }

        Long firstOnBoardOffset = peekFirstOnBoardOffset();
        if (firstOnBoardOffset != null && offset == firstOnBoardOffset) {
            shiftHighWatermark();
        }
    }

    private synchronized void shiftHighWatermark() {
        // whenever this is true, !onBoardOffsets.isEmpty() should also be true
        while (!committedOffsets.isEmpty()) {
            long firstOnBoardOffset = onBoardOffsets.getFirst();
            long firstCommittedOffset = committedOffsets.peek();
            if (firstOnBoardOffset != firstCommittedOffset) {
                break;
            }
            synchronized (offsetsHeadEntryLock) {
                onBoardOffsets.removeFirst();
                committedHighWatermark.set(committedOffsets.remove());
            }
        }
        logger.debug("new high watermark for partition {} = {}", topicPartition, committedHighWatermark.get());
    }

    public long commitReadyOffset() {
        return committedHighWatermark.get();
    }

    public int pendingRecordsCount() {
        return onBoardOffsets.size();
    }
}
