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

import org.apache.kafka.common.TopicPartition;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents consumption processing progress of records consumed from a single partition.
 * This class manages sequence of offsets and a flag which represents if each of them was completed or not.
 */
@Slf4j
@Accessors(fluent = true)
public class OutOfOrderCommitControl implements AutoCloseable {
    @Getter
    private final TopicPartition topicPartition;
    private final int capacity;
    private final OffsetStorageComplex complex;
    private final OffsetStateReaper offsetStateReaper;

    /**
     * The current maximum offset which it and all it's previous offsets were committed.
     */
    private volatile long highWatermark;

    public OutOfOrderCommitControl(TopicPartition topicPartition, int capacity,
                                   OffsetStateReaper offsetStateReaper) {
        this.topicPartition = topicPartition;
        complex = new OffsetStorageComplex(capacity);
        this.capacity = capacity;
        this.offsetStateReaper = offsetStateReaper;
        highWatermark = -1;
    }

    public synchronized OffsetState reportFetchedOffset(long offset) {
        if (isRegressing(offset)) {
            throw new OffsetRegressionException(String.format(
                    "offset regression %s: %d < %d", topicPartition, offset, highWatermark));
        }

        if (complex.size() == capacity) {
            throw new IllegalArgumentException(
                    String.format("offsets count overflow: cap=%d, offset=%d", capacity, offset));
        }

        if (complex.isComplete(offset)) {
            // There are two cases for this.
            // 1. Offset bigger than that complex's managing bounds. Reasonable to consider as not completed
            // because it is the offset to coming in future (and will be added to complex in line below).
            // 2. Offset has been processed in the past, marked as completed and now the consumer's consuming
            // it again from the point of watermark.
            return null;
        }

        OffsetState state = new OffsetState(offset);
        int ringIndex = complex.addOffset(offset, false, state);

        state.completion().asFuture().whenComplete((unused, throwable) -> onComplete(offset, ringIndex)); // TODO okay in this order?
        return state;
    }

    void onComplete(long offset, int ringIndex) {
        if (log.isDebugEnabled()) {
            log.debug("Offset complete: {}", offset);
        }
        complex.complete(ringIndex);
    }

    public synchronized void updateHighWatermark() {
        if (log.isTraceEnabled()) {
//            StringBuilder sb = new StringBuilder("[");
//
//            boolean first = true;
//            for (OffsetState st : states) {
//                if (first) {
//                    first = false;
//                } else {
//                    sb.append(", ");
//                }
//                sb.append(String.valueOf(st.offset()) + ':' + (st.completion().isComplete() ? 'c' : 'n'));
//            }
//            sb.append(']');
//            log.trace("Begin updateHighWatermark earliest={} latest={} hw={} states={}",
//                      earliest, latest, highWatermark, sb);
        }

        long lastHighWatermark = highWatermark;

        while (complex.size() > 0) {
            long offset = complex.firstOffset();
            if (complex.isComplete(offset)) {
                highWatermark = offset;
                complex.pollFirst();
            } else {
                OffsetState state = complex.firstState();
                offsetStateReaper.maybeReapOffset(state);
                break;
            }
        }

        if (highWatermark != lastHighWatermark) {
            log.debug("High watermark updated for {}: {} => {}",
                      topicPartition, lastHighWatermark, highWatermark);
        }
    }

    public synchronized int pendingOffsetsCount() {
        return complex.size();
    }

    public long commitReadyOffset() {
        return highWatermark;
    }

    public boolean isRegressing(long offset) {
        long firstOffset = complex.firstOffset();
        if (firstOffset < 0) {
            return offset <= highWatermark;
        } else {
            return offset < firstOffset;
        }
    }

    @Override
    public String toString() {
        return "OutOfOrderCommitControl{" +
               "topicPartition=" + topicPartition +
               ", highWatermark=" + highWatermark +
               '}';
    }

    @Override
    public void close() throws Exception {
        offsetStateReaper.close();
    }
}
