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

import java.util.Map.Entry;
import java.util.TreeMap;

import com.linecorp.decaton.protocol.Decaton.BitMapProto;
import com.linecorp.decaton.protocol.Decaton.OffsetIndexEntryProto;
import com.linecorp.decaton.protocol.Decaton.OffsetIndexProto;
import com.linecorp.decaton.protocol.Decaton.OffsetStorageComplexProto;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
public class OffsetStorageComplex {
    @AllArgsConstructor(access = AccessLevel.PACKAGE)
    @ToString
    static class OffsetIndex {
        @AllArgsConstructor
        @ToString
        static class BlockInfo {
            long index;
            int length;
        }

        // Map of block's base offset -> block's starting index
        private final TreeMap<Long, BlockInfo> blockIndex;
        @Getter
        private long firstIndex;
        private long nextIndex;

        OffsetIndex() {
            this(new TreeMap<>(), 0, 0);
        }

        public int indexSize() {
            return (int) (nextIndex - firstIndex);
        }

        public long firstOffset() {
            return blockIndex.isEmpty() ? -1 : blockIndex.firstKey();
        }

        public long pollFirst() {
            Entry<Long, BlockInfo> first = blockIndex.pollFirstEntry();
            final BlockInfo firstBlock = first.getValue();
            long removedIndex = firstBlock.index++;
            firstIndex = firstBlock.index;
            firstBlock.length--;
            if (firstBlock.length > 0) {
                long nextOffset = first.getKey() + 1;
                blockIndex.put(nextOffset, firstBlock); // TODO: inefficient, needs to handle by batch
            }
            return removedIndex;
        }

        public long indexOf(long offset) {
            Entry<Long, BlockInfo> e = blockIndex.floorEntry(offset);
            if (e == null) {
                return -1;
            }
            BlockInfo blockInfo = e.getValue();
            if (offset - e.getKey() >= blockInfo.length) {
                return -1; // This offset is out of managed bounds
            }
            return blockInfo.index + offset - e.getKey();
        }

        public long addOffset(long offset) {
            Entry<Long, BlockInfo> e = blockIndex.lastEntry();
            long offsetIndex = nextIndex++;
            if (e != null) {
                BlockInfo blockInfo = e.getValue();
                long nextOffset = e.getKey() + blockInfo.length;
                if (offset < nextOffset) {
                    // throw new IllegalArgumentException("can't regress");
                    throw new OffsetRegressionException("offset regression at " + offset);
                }
                if (offset == nextOffset) {
                    // No offset gap, can extend the last block
                    blockInfo.length++;
                    return offsetIndex;
                }
            }
            // Offset gap or first entry after cleanup, needs to create a new block
            blockIndex.put(offset, new BlockInfo(offsetIndex, 1));
            return offsetIndex;
        }

        public OffsetIndexProto toProto() {
            OffsetIndexProto.Builder builder = OffsetIndexProto.newBuilder()
                    .setFirstIndex(firstIndex)
                    .setNextIndex(nextIndex);
            for (Entry<Long, BlockInfo> entry : blockIndex.entrySet()) {
                long offset = entry.getKey();
                BlockInfo blockInfo = entry.getValue();
                OffsetIndexEntryProto entryProto = OffsetIndexEntryProto.newBuilder()
                                                                        .setStartOffset(offset)
                                                                        .setStartIndex(blockInfo.index)
                                                                        .setLength(blockInfo.length).build();
                builder.addEntries(entryProto);
            }
            return builder.build();
        }

        public static OffsetIndex fromProto(OffsetIndexProto proto) {
            TreeMap<Long, BlockInfo> blockIndex = new TreeMap<>();
            for (int i = 0; i < proto.getEntriesCount(); i++) {
                OffsetIndexEntryProto entry = proto.getEntries(i);
                blockIndex.put(entry.getStartOffset(), new BlockInfo(entry.getStartIndex(), entry.getLength()));
            }
            return new OffsetIndex(blockIndex, proto.getFirstIndex(), proto.getNextIndex());
        }
    }

    private final OffsetIndex index;
    private final ConcurrentBitMap compFlags;
    private final OffsetState[] states;

    public OffsetStorageComplex(int capacity) {
        this(new OffsetIndex(), new ConcurrentBitMap(capacity), new OffsetState[capacity]);
    }

    public int size() {
        return index.indexSize();
    }

    public long firstOffset() {
        return index.firstOffset();
    }

    public void pollFirst() {
        int firstIndex = (int) (index.pollFirst() % states.length);
        compFlags.set(firstIndex, false);
        states[firstIndex] = null;
    }

    public int addOffset(long offset, boolean complete, OffsetState state) {
        int nextIndex = (int) (index.addOffset(offset) % states.length);
        compFlags.set(nextIndex, complete);
        states[nextIndex] = state;
        return nextIndex;
    }

    public void complete(int ringIndex) {
        // Q. Don't we need to guard against multiple-complete of the same offset, which, if it happens
        // after the watermark progress, it can set complete flag on 1 or more round forward offset resulting
        // a bug?
        // A. This method itself contains that weakness, so this method assume the caller to take care of
        // external control and prevent a same offset being completed more than once.
        // In current OOOCC implementation, it is implemented by using CompletableFuture and call this method
        // from the callback of CF on completion, which is guaranteed to be triggered just once even though
        // the same CF completed more than once.
        compFlags.set(ringIndex, true);
    }

    public OffsetState firstState() {
        return states[(int) (index.firstIndex() % states.length)];
    }

    public boolean isComplete(long offset) {
        int ringIndex = (int) (index.indexOf(offset) % states.length);
        if (ringIndex == -1) {
            // By contract we expect the offset-out-of-range case to be just the offset being too large against
            // managed range, not lower.
            return false;
        }
        return compFlags.get(ringIndex);
    }

    public OffsetStorageComplexProto toProto() {
        OffsetIndexProto indexProto = index.toProto();
        BitMapProto compFlagsProto = compFlags.toProto();

        return OffsetStorageComplexProto.newBuilder()
                                        .setIndex(indexProto)
                                        .setCompFlags(compFlagsProto)
                                        .build();
    }

    public static OffsetStorageComplex fromProto(OffsetStorageComplexProto proto) {
        OffsetIndex index = OffsetIndex.fromProto(proto.getIndex());
        ConcurrentBitMap compFlags = ConcurrentBitMap.fromProto(proto.getCompFlags());
        return new OffsetStorageComplex(index, compFlags, new OffsetState[compFlags.size()]);
    }
}
