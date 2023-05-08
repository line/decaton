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

import java.nio.ByteBuffer;

/**
 * Default implementation for {@link SubPartitioner}.
 * This implementation guarantees that tasks with same key will always be assigned to the same subpartition.
 * If the key is null, the subpartition will be chosen in round-robin manner.
 */
public class DefaultSubPartitioner implements SubPartitioner {
    private final int bound;
    private final RoundRobinSubPartitioner roundRobinStrategy;

    public DefaultSubPartitioner(int bound) {
        this.bound = bound;
        roundRobinStrategy = new RoundRobinSubPartitioner(bound);
    }

    private static int toPositive(int number) {
        return number & 2147483647;
    }

    @Override
    public int subPartitionFor(byte[] key) {
        if (key == null) {
            return roundRobinStrategy.subPartitionFor(null);
        } else {
            // Kafka client uses murmur2 for hashing keys to decide partition to route the record,
            // so all keys we receive in a partition processor has highly biased distribution.
            // Here just by adding few bytes to the key we can "shift" hashing of the key and
            // can get back better distribution again in murmur2 result to evenly distribute keys
            // for subpartitions.
            final ByteBuffer bb = ByteBuffer.allocate(key.length + 2);
            bb.put((byte) 's');
            bb.put((byte) ':');
            bb.put(key);

            int hash = org.apache.kafka.common.utils.Utils.murmur2(bb.array());
            return toPositive(hash) % bound;
        }
    }
}
