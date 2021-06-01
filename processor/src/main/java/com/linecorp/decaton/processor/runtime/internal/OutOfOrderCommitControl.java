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

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents consumption processing progress of records consumed from a single partition.
 * This class manages sequence of offsets and a flag which represents if each of them was completed or not.
 */
public class OutOfOrderCommitControl implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(OutOfOrderCommitControl.class);

    private final TopicPartition topicPartition;
    private final int capacity;
    private final Deque<OffsetState> states;
    private final OffsetStateReaper offsetStateReaper;

    /**
     * The current smallest offset that has been reported but not completed.
     */
    private volatile long earliest;
    /**
     * The current largest offset that has been reported.
     */
    private volatile long latest;
    /**
     * The current maximum offset which it and all it's previous offsets were committed.
     */
    private volatile long highWatermark;

    public OutOfOrderCommitControl(TopicPartition topicPartition, int capacity,
                                   OffsetStateReaper offsetStateReaper) {
        this.topicPartition = topicPartition;
        states = new ArrayDeque<>(capacity);
        this.capacity = capacity;
        this.offsetStateReaper = offsetStateReaper;
        earliest = latest = 0;
        highWatermark = -1;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public synchronized OffsetState reportFetchedOffset(long offset) {
        if (isRegressing(offset)) {
            throw new OffsetRegressionException(String.format(
                    "offset regression %s: %d > %d", topicPartition, offset, latest));
        }

        if (states.size() == capacity) {
            throw new IllegalArgumentException(
                    String.format("offsets count overflow: cap=%d, offset=%d", capacity, offset));
        }

        OffsetState state = new OffsetState(offset);
        states.addLast(state);
        latest = state.offset();

        state.completion().asFuture().whenComplete((unused, throwable) -> onComplete(offset)); // TODO okay in this order?
        return state;
    }

    static void onComplete(long offset) {
        if (logger.isDebugEnabled()) {
            logger.debug("Offset complete: {}", offset);
        }
    }

    public synchronized void updateHighWatermark() {
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder("[");

            boolean first = true;
            for (OffsetState st : states) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append(String.valueOf(st.offset()) + ':' + (st.completion().isComplete() ? 'c' : 'n'));
            }
            sb.append(']');
            logger.trace("Begin updateHighWatermark earliest={} latest={} hw={} states={}",
                         earliest, latest, highWatermark, sb);
        }

        long lastHighWatermark = highWatermark;

        OffsetState state;
        while ((state = states.peekFirst()) != null) {
            earliest = state.offset();
            if (state.completion().isComplete()) {
                highWatermark = state.offset();
                states.pollFirst();
            } else {
                offsetStateReaper.maybeReapOffset(state);
                break;
            }
        }

        if (highWatermark != lastHighWatermark) {
            logger.debug("High watermark updated for {}: {} => {}",
                         topicPartition, lastHighWatermark, highWatermark);
        }
    }

    public synchronized int pendingOffsetsCount() {
        return states.size();
    }

    public long commitReadyOffset() {
        return highWatermark;
    }

    public boolean isRegressing(long offset) {
        return offset < latest;
    }

    @Override
    public String toString() {
        return "OutOfOrderCommitControl{" +
               "topicPartition=" + topicPartition +
               ", earliest=" + earliest +
               ", latest=" + latest +
               ", highWatermark=" + highWatermark +
               '}';
    }

    @Override
    public void close() throws Exception {
        offsetStateReaper.close();
    }
}
