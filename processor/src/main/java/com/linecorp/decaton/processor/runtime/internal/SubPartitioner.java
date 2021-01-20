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

import java.util.concurrent.atomic.AtomicLong;

class SubPartitioner {
    private final int bound;
    private final AtomicLong monotonicValueSupplier;

    SubPartitioner(int bound) {
        this.bound = bound;
        monotonicValueSupplier = new AtomicLong();
    }

    private static int toPositive(int number) {
        return number & 2147483647;
    }

    public int partitionFor(String key) {
        if (key == null) {
            return toPositive((int) monotonicValueSupplier.getAndIncrement()) % bound;
        } else {
            // Kafka client uses murmur2 for hashing keys to decide partition to route the record,
            // so all keys we receive in a partition processor has highly biased distribution.
            // Here just by adding few bytes to the key we can "shift" hashing of the key and
            // can get back better distribution again in murmur2 result to evenly distribute keys
            // for subpartitions.
            String shiftedKey = "s:" + key;
            int hash = org.apache.kafka.common.utils.Utils.murmur2(shiftedKey.getBytes());
            return toPositive(hash) % bound;
        }
    }
}
