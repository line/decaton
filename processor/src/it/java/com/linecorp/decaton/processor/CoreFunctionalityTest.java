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

package com.linecorp.decaton.processor;

import java.util.stream.IntStream;

import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.protocol.Sample.HelloTask;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite.DecatonProducerRecord;

public class CoreFunctionalityTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    @Test(timeout = 30000)
    public void testProcessConcurrently() {
        int numTasks = 10000;
        ProcessorTestSuite<HelloTask> suite =
                ProcessorTestSuite.builder(rule, HelloTask.parser())
                                  .produce(numTasks, IntStream.range(0, numTasks).mapToObj(i -> {
                                      String key = String.valueOf(i % 100);
                                      return new DecatonProducerRecord<>(key, HelloTask.newBuilder().setAge(i).build());
                                  }).iterator())
                                  .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                                      // adding some delay to generate out-of-order completion
                                      Thread.sleep(task.getAge() % 5);
                                  }))
                                  .propertySupplier(StaticPropertySupplier.of(
                                          Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 16),
                                          Property.ofStatic(ProcessorProperties.CONFIG_GROUP_REBALANCE_TIMEOUT_MS, Long.MAX_VALUE)
                                  ))
                                  .build();
        suite.run();
    }
}
