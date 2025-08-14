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

import java.util.concurrent.atomic.AtomicLongArray;

import com.linecorp.decaton.protocol.Decaton.BitMapProto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
@AllArgsConstructor
@ToString
class ConcurrentBitMap {
    @Getter
    private final int size;
    private final AtomicLongArray buckets;

    ConcurrentBitMap(int size) {
        this(size, new AtomicLongArray(bucketSize(size)));
    }

    static int bucketSize(int size) {
        int nbuckets = size / Long.SIZE;
        if (size % Long.SIZE != 0) {
            nbuckets++;
        }
        return nbuckets;
    }

    private static int bucketOf(int index) {
        return index / Long.SIZE;
    }

    private static int localIndex(int index) {
        return index % Long.SIZE;
    }

    private void ensureBound(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index out of range: " + index);
        }
    }

    public void set(int index, boolean on) {
        ensureBound(index);
        int bucket = bucketOf(index);
        int li = localIndex(index);
        while (true) {
            long bits = buckets.get(bucket);
            long newBits = bits & ~(1L << li) | (on ? 1L : 0L) << li;
            if (buckets.compareAndSet(bucket, bits, newBits)) {
                break;
            }
        }
    }

    public boolean get(int index) {
        ensureBound(index);
        int bucket = bucketOf(index);
        long bits = buckets.get(bucket);
        boolean x = (bits >> localIndex(index) & 1) == 1;
        return x;
    }

    public BitMapProto toProto() {
        BitMapProto.Builder builder = BitMapProto.newBuilder()
                                                 .setSize(size);
        for (int i = 0; i < buckets.length(); i++) {
            builder.addBuckets(buckets.get(i));
        }
        return builder.build();
    }

    public static ConcurrentBitMap fromProto(BitMapProto proto) {
        AtomicLongArray buckets = new AtomicLongArray(proto.getBucketsCount());
        for (int i = 0; i < proto.getBucketsCount(); i++) {
            buckets.set(i, proto.getBuckets(i));
        }
        return new ConcurrentBitMap(proto.getSize(), buckets);
    }
}
