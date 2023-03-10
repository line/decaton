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

import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link SubPartitioner} implementation that assigns tasks to sub-partitions in round-robin manner.
 * This implementation is useful when you want to evenly distribute tasks among sub-partitions
 * regardless of the key of the tasks.
 */
public class RoundRobinSubPartitioner implements SubPartitioner {
    private final int bound;
    private final AtomicLong monotonicValueSupplier;


    public RoundRobinSubPartitioner(int bound) {
        this.bound = bound;
        monotonicValueSupplier = new AtomicLong();
    }
    private static int toPositive(int number) {
        return number & 2147483647;
    }


    @Override
    public int subPartitionFor(byte[] key) {
        return toPositive((int) monotonicValueSupplier.getAndIncrement()) % bound;
    }
}
