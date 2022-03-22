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

import java.util.List;
import java.util.Random;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.linecorp.decaton.processor.processors.BatchingProcessor;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.RandomRule;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;
import com.linecorp.decaton.testing.processor.TestTask;

public class BatchingProcessorTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();
    @Rule
    public RandomRule randomRule = new RandomRule();

    @Test(timeout = 30000)
    public void testBatchingProcessor() throws Exception {
        Random rand = randomRule.random();
        ProcessorTestSuite
            .builder(rule)
            .configureProcessorsBuilder(builder -> builder.thenProcess(
                new BatchingProcessor<TestTask>(1000, 100) {
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
}
