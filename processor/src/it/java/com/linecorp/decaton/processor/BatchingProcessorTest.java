/*
 * Copyright 2022 LINE Corporation
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

package com.linecorp.decaton.processor;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.linecorp.decaton.processor.processors.BatchingProcessor;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.testing.KafkaClusterExtension;
import com.linecorp.decaton.testing.RandomExtension;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;
import com.linecorp.decaton.testing.processor.TestTask;

public class BatchingProcessorTest {
    @RegisterExtension
    public static KafkaClusterExtension rule = new KafkaClusterExtension();
    @RegisterExtension
    public RandomExtension randomExtension = new RandomExtension();

    @Test
    @Timeout(30)
    public void testBatchingProcessor() throws Exception {
        Random rand = randomExtension.random();
        ProcessorTestSuite
            .builder(rule)
            .configureProcessorsBuilder(builder -> builder.thenProcess(
                new BatchingProcessor<TestTask>(() -> 1000L, () -> 100) {
                    @Override
                    protected void processBatchingTasks(List<BatchingTask<TestTask>> batchingTasks) {
                        // adding some random delay to simulate realistic usage
                        try {
                            Thread.sleep(rand.nextInt(10));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        batchingTasks.forEach(batchingTask -> batchingTask.completion().complete());
                    }
                }
            ))
            .propertySupplier(StaticPropertySupplier.of(
                Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 16)
            ))
            .build()
            .run();
    }

    @Test
    @Timeout(30)
    public void testDynamicConfiguration() throws Exception {
        // Test with dynamic linger milliseconds but static capacity
        Random rand = randomExtension.random();
        
        // Track the value of lingerMs for testing purposes
        final long[] lingerMsValues = {1000L, 2000L, 3000L};
        AtomicInteger lingerMsCallTimes = new AtomicInteger(0);
        final int[] capacityValues = {100, 200, 300};
        AtomicInteger capacityCallTimes = new AtomicInteger(0);
        
        ProcessorTestSuite
                .builder(rule)
                .configureProcessorsBuilder(builder -> builder.thenProcess(
                        new BatchingProcessor<TestTask>(
                                () -> lingerMsValues[lingerMsCallTimes.getAndIncrement() % lingerMsValues.length],
                                () -> capacityValues[capacityCallTimes.getAndIncrement() % capacityValues.length]
                        ) {
                            @Override
                            protected void processBatchingTasks(List<BatchingTask<TestTask>> batchingTasks) {
                                // adding some random delay to simulate realistic usage
                                try {
                                    Thread.sleep(rand.nextInt(10));
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException(e);
                                }
                                batchingTasks.forEach(batchingTask -> batchingTask.completion().complete());
                            }
                        }
                ))
                .propertySupplier(StaticPropertySupplier.of(
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 16)
                ))
                .build()
                .run();

        assertTrue(lingerMsCallTimes.get() > 1);
        assertTrue(capacityCallTimes.get() > 1);
    }
}
