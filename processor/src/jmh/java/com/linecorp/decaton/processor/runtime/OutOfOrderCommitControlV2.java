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

import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents consumption processing progress of records consumed from a single partition.
 * This class manages sequence of offsets and a flag which represents if each of them was completed or not.
 */
public class OutOfOrderCommitControlV2 {
    private static final Logger logger = LoggerFactory.getLogger(OutOfOrderCommitControlV2.class);

    private static class OffsetState {
        private final long offset;
        private volatile boolean committed;

        private OffsetState(long offset, boolean committed) {
            this.offset = offset;
            this.committed = committed;
        }

        @Override
        public String toString() {
            return "OffsetState{" +
                   "offset=" + offset +
                   ", committed=" + committed +
                   '}';
        }
    }

    private final TopicPartition topicPartition;
    private final AtomicReferenceArray<OffsetState> states;

    /**
     * Points an minimum offset of offsets currently stored in {@link #states}.
     */
    private volatile long earliest;
    /**
     * Points a next offset which is expected to be received by next call of {@link #reportFetchedOffset(long)}.
     */
    private volatile long latest;
    /**
     * The current maximum offset which it and all it's previous offsets were committed.
     */
    private volatile long highWatermark;

    public OutOfOrderCommitControlV2(TopicPartition topicPartition, int capacity) {
        this.topicPartition = topicPartition;
        states = new AtomicReferenceArray<>(capacity);
        earliest = latest = highWatermark = 0;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    private int pos(long offset) {
        return (int) (offset % states.length());
    }

    public synchronized void reportFetchedOffset(long offset) {
        if (earliest == 0) {
            earliest = latest = offset;
        }

        if (isRegressing(offset)) {
            throw new IllegalArgumentException("offset regression [" + offset + "] on " + topicPartition);
        }

        if (offset - earliest >= states.length()) {
            throw new IllegalArgumentException(String.format(
                    "too large gap to store offset: offset=%d earliest=%d states.length=%d",
                    offset, earliest, states.length()));
        }

        while (latest <= offset) {
            long fill = latest++;
            boolean phantom = fill != offset;
            OffsetState state = new OffsetState(fill, phantom);
            if (phantom) {
                logger.debug("filling phantom state for gaping offset: {}", fill);
            }
            states.set(pos(fill), state);
        }
    }

    /**
     * Commit given offset by marking corresponding {@link OffsetState} committed.
     * Thread safety rationale:
     * Thread safety among threads which may call {@link #complete(long)} concurrently:
     * - If two threads attempt to complete different offsets:
     *   => Safe, as {@link #states} can provide atomic reference for each element of an array, and they are
     *      referenced and modified independently.
     * - If two threads attempt to complete the same offset:
     *   => Safe, as one of two threads may overwrite {@link OffsetState#committed} but the field is modified
     *      by {@code volatile} so it provides idempotent read result once it gets updated to true.
     *
     * Thread safety among a thread completing offset and the thread updating {@link #states}:
     * In the first place, pointer fields like {@link #earliest} and {@link #latest} are not needed to be
     * considered, as they are read/written only by a consumer thread, which is also guaranteed by methods
     * marked {@code synchronized}.
     * - If {@link #reportFetchedOffset(long)} overwrites an element which is not yet committed.
     *   => This shouldn't happen, as {@link #reportFetchedOffset(long)} prohibits accepting offset which is larger
     *      than {@code earliest + states.length()}. Therefore this case is not necessary to consider.
     * - If {@link #reportFetchedOffset(long)} overwrites an element which is already completed.
     *   => This can happen when a asynchronous processor attempts to complete an offset twice. However, even in
     *      case, if a read {@link OffsetState} points an offset which is same to the given offset, it's just
     *      writes true to {@link OffsetState#committed} again, which leads idempotent result, and if a read
     *      {@link OffsetState} points an offset which is larger than the given offset, it's just returns doing
     *      nothing.
     *
     * @param offset the offset to complete.
     */
    public void complete(long offset) {
        int pos = pos(offset);
        OffsetState state = states.get(pos);

        if (state == null || state.offset < offset) {
            // Shouldn't happen. Likely a bug.
            throw new IllegalArgumentException(String.format(
                    "attempted to complete an offset which isn't expected: offset=%d earliest=%d latest=%d" +
                    " found-state=%s",
                    offset, earliest, latest, state));
        }
        if (state.offset > offset) {
            // Suppose this offset has already been completed once, so no need to do anything.
            return;
        }

        state.committed = true;

        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder("[");
            // Save these volatile variables current view.
            long start = earliest;
            long end = latest;
            for (long i = start; i < end; i++) {
                if (i != start) {
                    sb.append(", ");
                }
                OffsetState st = states.get(pos(i));
                if (st == null) {
                    sb.append('N');
                } else {
                    sb.append(String.valueOf(st.offset) + ':' + (st.committed ? 'c' : 'n'));
                }
            }
            sb.append(']');
            logger.trace("offset complete({}) earliest={} latest={} hw={} states={}",
                         offset, start, end, highWatermark, sb);
        } else if (logger.isDebugEnabled()) {
            logger.debug("offset complete({}) earliest={} latest={} hw={}",
                         offset, earliest, latest, highWatermark);
        }
    }

    public synchronized void updateHighWatermark() {
        long lastHighWatermark = highWatermark;
        while (earliest < latest) {
            OffsetState state = states.get(pos(earliest));
            if (state.committed) {
                highWatermark = earliest++;
            } else {
                break;
            }
        }
        if (highWatermark != lastHighWatermark) {
            logger.debug("new high watermark for partition {} = {}", topicPartition, highWatermark);
        }
    }

    public synchronized int pendingOffsetsCount() {
        return (int) (latest - earliest);
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
               ", states=" + states +
               ", earliest=" + earliest +
               ", latest=" + latest +
               ", highWatermark=" + highWatermark +
               '}';
    }
}
