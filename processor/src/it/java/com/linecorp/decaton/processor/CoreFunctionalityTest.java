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

import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.protocol.Sample.HelloTask;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite.DecatonProducerRecord;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite.ProcessOrdering;

public class CoreFunctionalityTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    @Test(timeout = 30000)
    public void testAsyncTaskCompletion() {
        int numTasks = 10000;
        ProcessorTestSuite
                .builder(rule, HelloTask.parser())
                .produce(numTasks, IntStream.range(0, numTasks).mapToObj(i -> {
                    String key = String.valueOf(i % 100);
                    return new DecatonProducerRecord<>(key, HelloTask.newBuilder().setAge(i).build());
                }).iterator())
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    DeferredCompletion completion = ctx.deferCompletion();
                    CompletableFuture.runAsync(() -> {
                        try {
                            Thread.sleep(task.getAge() % 5);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } finally {
                            completion.complete();
                        }
                    });
                }))
                .build()
                .run();
    }

    @Test(timeout = 30000)
    public void testProcessConcurrent() {
        int numTasks = 10000;
        ProcessorTestSuite
                .builder(rule, HelloTask.parser())
                .produce(numTasks, IntStream.range(0, numTasks).mapToObj(i -> {
                    String key = String.valueOf(i % 100);
                    return new DecatonProducerRecord<>(key, HelloTask.newBuilder().setAge(i).build());
                }).iterator())
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    // adding some delay to generate out-of-order completion
                    Thread.sleep(task.getAge() % 5);
                }))
                .propertySupplier(StaticPropertySupplier.of(
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 16)
                ))
                .build()
                .run();
    }

    /**
     * Check all produced tasks will be processed in order only once
     * if we set partition concurrency to 1 as vanilla KafkaConsumer.
     *
     * We can also make sure that offset commit is working properly during rebalance by this test
     * since the test involves rolling restart
     */
    @Test(timeout = 30000)
    public void testProcessSerial() {
        int numTasks = 10000;
        ProcessorTestSuite
                .builder(rule, HelloTask.parser())
                .produce(numTasks, IntStream.range(0, numTasks).mapToObj(i -> {
                    String key = String.valueOf(i % 100);
                    return new DecatonProducerRecord<>(key, HelloTask.newBuilder().setAge(i).build());
                }).iterator())
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    // adding some latency not to finish all tasks too early
                    Thread.sleep(task.getAge() % 3);
                }))
                .propertySupplier(StaticPropertySupplier.of(
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 1)
                ))
                .expectOrdering(ProcessOrdering.STRICT)
                .build()
                .run();
    }
}