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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import com.linecorp.decaton.protocol.Decaton.OffsetStorageComplexProto;

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
    private final OffsetStateReaper offsetStateReaper;
    final OffsetStorageComplex complex;

    /**
     * The current maximum offset which it and all it's previous offsets were committed.
     */
    private volatile long highWatermark;
    private volatile boolean anyOffsetsUpdated;

    public OutOfOrderCommitControl(TopicPartition topicPartition, int capacity,
                                   OffsetStateReaper offsetStateReaper,
                                   OffsetStorageComplex complex, long highWatermark) {
        this.topicPartition = topicPartition;
        this.capacity = capacity;
        this.offsetStateReaper = offsetStateReaper;
        this.complex = complex;
        this.highWatermark = highWatermark;
    }

    public OutOfOrderCommitControl(TopicPartition topicPartition, int capacity,
                                   OffsetStateReaper offsetStateReaper) {
        this(topicPartition, capacity, offsetStateReaper, new OffsetStorageComplex(capacity), -1);
    }

    public static OutOfOrderCommitControl fromOffsetMeta(TopicPartition tp,
                                                         int capacity,
                                                         OffsetStateReaper offsetStateReaper,
                                                         OffsetAndMetadata offsetMeta) {
        OffsetStorageComplex complex = complexFromMeta(offsetMeta.metadata());
        OutOfOrderCommitControl cc = new OutOfOrderCommitControl(
                tp, capacity, offsetStateReaper, complex, offsetMeta.offset() - 1);
        log.debug("Restoring OOOCC from offsetMeta for {}: pending={} hw={} states={}",
                  cc.topicPartition, cc.pendingOffsetsCount(), cc.highWatermark, cc.complex.compDebugDump());
        return cc;
    }

    public synchronized OffsetState reportFetchedOffset(long offset) {
        if (isRegressing(offset)) {
            log.debug("Not newly adding offset state on {} for offset={} because its lower than HW",
                      topicPartition, offset);
            return null;
        }

        if (complex.size() == capacity) {
            throw new IllegalArgumentException(
                    String.format("offsets count overflow: size=%d, cap=%d", complex.size(), capacity));
        }

        int ringIndex = complex.allocNextIndex(offset);
        if (complex.isComplete(offset)) {
            // There are two cases for this.
            // 1. Offset is bigger than the complex's managing bounds. Reasonable to consider as not completed
            // because it is the offset to coming in future (and will be added to complex in line below).
            // 2. Offset has been processed in the past, marked as completed and now the consumer's consuming
            // it again from the point of watermark.
            log.debug("Not newly adding offset state on {} for offset={}, ringIndex={} because it's complete",
                      topicPartition, offset, ringIndex);
            return null;
        }

        OffsetState state = new OffsetState(offset, () -> onComplete(offset, ringIndex));
        complex.setIndex(ringIndex, false, state);
        return state;
    }

    void onComplete(long offset, int ringIndex) {
        if (log.isDebugEnabled()) {
            log.debug("Offset complete on {}: {}", topicPartition, offset);
        }
        complex.complete(ringIndex);
        anyOffsetsUpdated = true;
    }

    public synchronized void updateHighWatermark() {
        if (log.isTraceEnabled()) {
            log.trace("Begin updateHighWatermark tp={} pending={} hw={} states={}",
                      topicPartition, pendingOffsetsCount(), highWatermark, complex.compDebugDump());
        }

        long lastHighWatermark = highWatermark;

        while (complex.size() > 0) {
            long offset = complex.firstOffset();
            boolean complete = complex.isComplete(offset);
            if (complete) {
                highWatermark = offset;
                complex.pollFirst();
            } else {
                OffsetState state = complex.firstState();
                if (state != null) {
                    offsetStateReaper.maybeReapOffset(state);
                }
                break;
            }
        }

        if (highWatermark != lastHighWatermark && log.isDebugEnabled()) {
            int pending = pendingOffsetsCount();
            log.debug("High watermark updated {}: {} => {}, pending={}, first={}",
                      topicPartition, lastHighWatermark, highWatermark, pending, pending > 0 ? complex.firstOffset() : -1);
        }
    }

    public synchronized int pendingOffsetsCount() {
        return complex.size();
    }

    public OffsetAndMetadata commitReadyOffset(long lastCommittedOffset) {
        if (highWatermark < lastCommittedOffset && !anyOffsetsUpdated) {
            return null;
        }
        OffsetStorageComplexProto complexProto = complex.toProto();
        final OffsetAndMetadata offsetMeta;
        try {
            String meta = JsonFormat.printer().omittingInsignificantWhitespace().print(complexProto);
            long commitOffset = highWatermark + 1;
            offsetMeta = new OffsetAndMetadata(commitOffset, meta);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("failed to serialize offset metadata into proto", e);
        }
        anyOffsetsUpdated = false;
        return offsetMeta;
    }

    static OffsetStorageComplex complexFromMeta(String metadata) {
        OffsetStorageComplexProto proto = parseOffsetMeta(metadata);
        return OffsetStorageComplex.fromProto(proto);
    }

    static OffsetStorageComplexProto parseOffsetMeta(String metadata) {
        OffsetStorageComplexProto.Builder builder = OffsetStorageComplexProto.newBuilder();
        try {
            JsonFormat.parser().merge(metadata, builder);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(e);
        }
        return builder.build();
    }

    public boolean isRegressing(long offset) {
        // There's a case that regressing offset gets reported into.
        // A most typical case may happen in the following process sequence.
        // 1. Consumer commits offset X
        // 2. Consumer starts rebalancing
        // 3. The remaining tasks in queue, having offset X+1 and later kept processing
        // 4. updateHighWatermark() => HW becomes X+1 or later.
        // 5. Consumer rebalance complements, the consuming offset resets to X.
        // 6. reportFetchedOffset(X) => regression.
        // As long as the reported offset is lower than the highWatermark, we should be able to
        // respond as the offset has already been processed.
        return offset <= highWatermark;
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
