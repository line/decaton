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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.common.utils.Utils;
import org.junit.Before;
import org.junit.Test;

public class SubPartitionerTest {
    static final int DIST_KEYS_COUNT = 10000;
    static final int[] PARTITION_COUNTS = { 1, 8, 16, 32, 64, 96 };
    static final int[] SUBPARTITION_COUNTS = { 10, 17, 32, 64 };
    static final double TARGET_STDDEV_RATIO = 0.05;

    static final byte[][] keys = new byte[DIST_KEYS_COUNT][];

    @Before
    public void setUp() {
        for (int i = 0; i < keys.length; i++) {
            keys[i] = String.valueOf(i).getBytes(UTF_8);
        }
    }

    private static double stddev(int[] counts) {
        double avg = (double) Arrays.stream(counts).sum() / counts.length;
        double variance = Arrays.stream(counts).asDoubleStream()
                                .map(v -> Math.pow(avg - v, 2)).sum() / counts.length;
        return Math.sqrt(variance);
    }

    @Test
    public void testEvenlyDistributedSelection() {
        for (int partitionCount : PARTITION_COUNTS) {
            List<List<byte[]>> partitions = new ArrayList<>(partitionCount);
            for (int i = 0; i < partitionCount; i++) {
                partitions.add(new ArrayList<>());
            }

            for (byte[] key : keys) {
                // This is the way used to determine partition in Kafka's DefaultPartitioner
                int partition = (Utils.murmur2(key) & 2147483647) % partitionCount;
                partitions.get(partition).add(key);
            }

            int[] partCounts = partitions.stream().mapToInt(List::size).toArray();
            double partStddev = stddev(partCounts);
            System.err.printf("[%d] partition stddev = %f, counts = %s\n",
                              partitionCount, partStddev, Arrays.toString(partCounts));

            for (int subpartitionCount : SUBPARTITION_COUNTS) {
                for (List<byte[]> partition : partitions) {
                    int[] counts = new int[subpartitionCount];
                    SubPartitioner subPartitioner = new DefaultSubPartitioner(counts.length);
                    for (byte[] key : partition) {
                        int subPartition = subPartitioner.subPartitionFor(key);
                        counts[subPartition]++;
                    }

                    double stddev = stddev(counts);
                    System.err.printf("[%d/%d] stddev = %f, counts = %s\n",
                                      partitionCount, subpartitionCount, stddev, Arrays.toString(counts));

                    assertTrue(String.format("[%d/%d] %f (stddev) < %d * %f",
                                             partitionCount, subpartitionCount,
                                             stddev, partitionCount, TARGET_STDDEV_RATIO),
                               stddev < partition.size() * TARGET_STDDEV_RATIO);
                }
            }
        }
    }

    @Test
    public void testConsistentSelectionForSameKeys() {
        for (int subpartitionCount : SUBPARTITION_COUNTS) {
            SubPartitioner subPartitioner = new DefaultSubPartitioner(subpartitionCount);
            for (byte[] key : keys) {
                int assign1 = subPartitioner.subPartitionFor(key);
                int assign2 = subPartitioner.subPartitionFor(key);
                assertEquals(String.format("[%d] assign of %s", subpartitionCount, new String(key, UTF_8)),
                             assign2, assign1);
            }
        }
    }

    @Test
    public void testRoundRobin() {
        for (int subpartitionCount : SUBPARTITION_COUNTS) {
            SubPartitioner subPartitioner = new RoundRobinSubPartitioner(subpartitionCount);
            for (byte[] key : keys) {
                int assign1 = subPartitioner.subPartitionFor(key);
                int assign2 = subPartitioner.subPartitionFor(key);
                assertEquals(String.format("[%d] first assign: %d; second assign: %d", subpartitionCount, assign1, assign2),
                             assign2, (assign1 + 1) % subpartitionCount);
            }
        }
    }
}
